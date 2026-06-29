"""
run_mp_reorganize_daily.py — BC job：MP 每日重組 v1.0

職責：
  讀 ./staging/ artifact（由 fetch_mp job 傳入）
  → 並發下載所有 ticker 現有 d.csv 到記憶體
  → 逐日 merge（缺口優先，今日最後）
  → 寫本地 ./output/mp_data/ticker/*/d.csv
  → upload_folder 一次寫回 HF Dataset（SDK 自動處理 429）
  → 全量核查 verify_log
  → 更新 MP_Full_Progress + Task_Log

模式（從 summary.json 讀取，對應 fetch_mp 的 mode）：
  daily   — 正常模式，處理 summary.json 中所有 dates_fetched
  rebuild — 單日強制重覆寫，同樣走此腳本，無特殊分支

核查邏輯：
  每個 ticker d.csv 最新日期應 == trading_date（或至少包含 dates_fetched 中最大日期）
  核查失敗 → status=partial, resolved=False → 下次 fetch_mp daily 自動補跑

環境變量（GitHub Actions Secrets）：
  HF_TOKEN     → HF Dataset 讀寫
  HF_REPO_ID   → HF Dataset repo
  MONGO_URI    → MongoDB 進度存取
  TRADING_DATE → 目標交易日（workflow input）
  COMM_HUB_URL → GAS Telegram proxy（可選）
  WEBHOOK_SECRET → GAS 鑒權（可選）

依賴：
  huggingface_hub>=0.20（upload_folder SDK）
  pandas, pytz, pymongo, requests

Python 3.9 兼容。
"""

import io
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import pymongo
import pytz
import requests
from huggingface_hub import HfApi

# ─────────────────────────────────────────────
# 常數
# ─────────────────────────────────────────────

HF_TICKER_DIR  = "mp_data/ticker"
STAGING_DIR    = "./staging"
OUTPUT_DIR     = "./output"
DOWNLOAD_WORKERS = 10   # 並發下載線程數（不超過 20，HF CDN 隱性限速）

EST_TZ = pytz.timezone("US/Eastern")


# ─────────────────────────────────────────────
# 環境變量
# ─────────────────────────────────────────────

def _hf_token() -> str:
    return os.environ.get("HF_TOKEN", "")

def _hf_repo() -> str:
    return os.environ.get("HF_REPO_ID", "zhujun0511-AI/ai-telegram-bot-dataset")

def _mongo_uri() -> str:
    return os.environ.get("MONGO_URI", "")

def _trading_date() -> str:
    return os.environ.get("TRADING_DATE", "")


# ─────────────────────────────────────────────
# 時間工具
# ─────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(EST_TZ).isoformat()


# ─────────────────────────────────────────────
# MongoDB
# ─────────────────────────────────────────────

class ReorganizeDB:
    def __init__(self):
        uri = _mongo_uri()
        if not uri:
            raise RuntimeError("MONGO_URI 未設定")
        self.client   = pymongo.MongoClient(uri)
        self.stock_db = self.client["StockData"]
        self.progress = self.stock_db["MP_Full_Progress"]
        self.task_log = self.stock_db["Task_Log"]

    def close(self):
        self.client.close()

    def get_last_processed_date(self) -> str:
        doc = self.progress.find_one({"date": "_reorganize_daily_checkpoint"})
        return (doc or {}).get("last_processed_date", "")

    def save_checkpoint(self, last_processed_date: str):
        self.progress.update_one(
            {"date": "_reorganize_daily_checkpoint"},
            {"$set": {
                "date":                 "_reorganize_daily_checkpoint",
                "last_processed_date":  last_processed_date,
                "updated_at":           _now_iso(),
            }},
            upsert=True,
        )

    def mark_done(self, date_str: str, ticker_count: int):
        self.progress.update_one(
            {"date": date_str},
            {"$set": {
                "status":     "done",
                "resolved":   True,
                "row_count":  ticker_count,
                "updated_at": _now_iso(),
            }},
            upsert=True,
        )

    def mark_partial(self, date_str: str, failed_tickers: list):
        groups = {}
        for t in failed_tickers:
            key = t[0].upper() if t else "?"
            groups.setdefault(key, []).append(t)

        self.progress.update_one(
            {"date": date_str},
            {"$set": {
                "status":         "partial",
                "resolved":       False,
                "failed_tickers": failed_tickers,
                "failed_groups":  groups,
                "reason":         "verify_failed",
                "updated_at":     _now_iso(),
            }},
            upsert=True,
        )

    def write_task_log(self, task: str, status: str, detail: dict):
        self.task_log.insert_one({
            "task":       task,
            "status":     status,
            "detail":     detail,
            "created_at": _now_iso(),
        })


# ─────────────────────────────────────────────
# Telegram
# ─────────────────────────────────────────────

def _send_telegram(msg: str):
    url    = os.environ.get("COMM_HUB_URL", "")
    secret = os.environ.get("WEBHOOK_SECRET", "")
    if not url:
        print(f"  ⚠️ COMM_HUB_URL 未設定，跳過 Telegram")
        return
    try:
        resp = requests.post(
            url,
            json={"message": msg, "secret": secret},
            timeout=15,
        )
        print(f"  📨 Telegram: {resp.status_code}")
    except Exception as e:
        print(f"  ⚠️ Telegram 發送失敗: {e}")


# ─────────────────────────────────────────────
# 讀取 staging artifact
# ─────────────────────────────────────────────

def _load_summary() -> dict:
    path = os.path.join(STAGING_DIR, "summary.json")
    if not os.path.exists(path):
        raise FileNotFoundError(f"summary.json 不存在: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_staging_data(dates_fetched: list) -> dict:
    """
    讀取 ./staging/*.parquet，按日期 → {ticker: DataFrame} 結構組裝。
    返回 { date_str: { symbol: df_single_row } }
    """
    staging_by_date = {}

    for date_str in dates_fetched:
        path = os.path.join(STAGING_DIR, f"{date_str}.parquet")
        if not os.path.exists(path):
            print(f"  ⚠️ staging 文件不存在: {path}")
            continue
        try:
            df = pd.read_parquet(path)
            df.columns = [c.lower() for c in df.columns]
            if "symbol" not in df.columns:
                print(f"  ⚠️ {date_str}.parquet 缺少 symbol 欄位")
                continue
            df["symbol"] = df["symbol"].str.upper().str.strip()
            # 按 ticker 分組成 {symbol: single-row DataFrame}
            by_ticker = {}
            for _, row in df.iterrows():
                sym = row.get("symbol", "")
                if not sym:
                    continue
                by_ticker[sym] = pd.DataFrame([{
                    "date":   row.get("date", date_str),
                    "open":   row.get("open"),
                    "high":   row.get("high"),
                    "low":    row.get("low"),
                    "close":  row.get("close"),
                    "volume": row.get("volume"),
                }])
            staging_by_date[date_str] = by_ticker
            print(f"  📂 staging {date_str}: {len(by_ticker)} tickers")
        except Exception as e:
            print(f"  ❌ 讀取 staging {date_str}: {e}")

    return staging_by_date


# ─────────────────────────────────────────────
# HF Dataset 下載（並發）
# ─────────────────────────────────────────────

def _download_one_ticker_d_csv(ticker: str) -> tuple:
    """
    下載單個 ticker d.csv，返回 (ticker, df or None)。
    供 ThreadPoolExecutor 調用。
    """
    url = (
        f"https://huggingface.co/datasets/{_hf_repo()}"
        f"/resolve/main/{HF_TICKER_DIR}/{ticker}/d.csv"
    )
    for attempt in range(2):
        try:
            resp = requests.get(
                url,
                headers={"Authorization": f"Bearer {_hf_token()}"},
                timeout=30,
            )
            if resp.status_code == 200:
                df = pd.read_csv(io.BytesIO(resp.content), dtype=str)
                df.columns = [c.lower() for c in df.columns]
                return ticker, df
            if resp.status_code == 404:
                return ticker, None
            if resp.status_code == 429:
                print(f"  ⚠️ 下載 {ticker} d.csv 429，等待 70s 重試")
                time.sleep(70)
                continue
            return ticker, None
        except Exception as e:
            if attempt == 0:
                time.sleep(2)
            else:
                print(f"  ❌ 下載 {ticker} d.csv 異常: {e}")
    return ticker, None


def _download_all_tickers(all_tickers: list) -> dict:
    """
    並發下載所有 ticker 的 d.csv 到記憶體。
    返回 { ticker: df or None }（None = 404 / 失敗）
    """
    print(f"📥 並發下載 d.csv（{DOWNLOAD_WORKERS} threads）| ticker: {len(all_tickers)}")
    t0 = time.monotonic()
    cache = {}

    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as exe:
        futures = {exe.submit(_download_one_ticker_d_csv, t): t for t in all_tickers}
        done_count = 0
        for future in as_completed(futures):
            ticker, df = future.result()
            cache[ticker] = df
            done_count += 1
            if done_count % 500 == 0:
                print(f"  ⏳ 下載進度: {done_count}/{len(all_tickers)}")

    elapsed = time.monotonic() - t0
    found   = sum(1 for v in cache.values() if v is not None)
    print(f"✅ 下載完成 | 已有: {found} | 新建: {len(all_tickers) - found} | 耗時: {elapsed:.0f}s")
    return cache


# ─────────────────────────────────────────────
# Merge 邏輯
# ─────────────────────────────────────────────

def _merge_ticker(
    ticker: str,
    existing_df,  # pd.DataFrame or None
    new_rows_by_date: dict,  # { date_str: single-row DataFrame }
) -> pd.DataFrame:
    """
    將現有 d.csv 與新數據合併：
    - 現有為 None → 只用新數據
    - 現有有數據 → concat + drop_duplicates(date) + sort
    - 結果：date 升序，drop_duplicates
    """
    frames = []
    if existing_df is not None and not existing_df.empty:
        frames.append(existing_df)

    for date_str, new_df in new_rows_by_date.items():
        if new_df is not None and not new_df.empty:
            frames.append(new_df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined.columns = [c.lower() for c in combined.columns]

    if "date" not in combined.columns:
        return pd.DataFrame()

    combined = (
        combined
        .sort_values("date")
        .drop_duplicates("date")
        .reset_index(drop=True)
    )
    return combined


# ─────────────────────────────────────────────
# 本地輸出目錄寫入
# ─────────────────────────────────────────────

def _write_output_d_csv(ticker: str, df: pd.DataFrame):
    """寫到 ./output/mp_data/ticker/{ticker}/d.csv。"""
    dir_path = os.path.join(OUTPUT_DIR, HF_TICKER_DIR, ticker)
    os.makedirs(dir_path, exist_ok=True)
    path = os.path.join(dir_path, "d.csv")
    df.to_csv(path, index=False)


# ─────────────────────────────────────────────
# HF upload_folder
# ─────────────────────────────────────────────

def _upload_to_hf(output_path: str):
    """
    用 huggingface_hub upload_folder 一次性上傳 output/ 到 HF Dataset。
    SDK 自動分批、自動重試 429。
    """
    print(f"⬆️ upload_folder 開始 | 目錄: {output_path}")
    t0  = time.monotonic()
    api = HfApi(token=_hf_token())
    api.upload_folder(
        folder_path=output_path,
        repo_id=_hf_repo(),
        repo_type="dataset",
        commit_message=f"mp_reorganize_daily: batch upload",
        # path_in_repo 不指定 → 保持目錄結構對齊 output/ 的子路徑
    )
    elapsed = time.monotonic() - t0
    print(f"✅ upload_folder 完成 | 耗時: {elapsed:.0f}s")


# ─────────────────────────────────────────────
# 主邏輯
# ─────────────────────────────────────────────

def main() -> int:
    trading_date = _trading_date()
    if not trading_date:
        print("❌ TRADING_DATE 未設定")
        return 1

    print(f"🚀 run_mp_reorganize_daily.py 啟動 | trading_date={trading_date}")
    t_start = time.monotonic()

    # ── 1. 讀 summary.json ──
    try:
        summary = _load_summary()
    except FileNotFoundError as e:
        print(f"❌ {e}")
        return 1

    dates_fetched = summary.get("dates_fetched", [])
    if not dates_fetched:
        skipped = summary.get("skipped", False)
        if skipped:
            print(f"✅ summary.json 標記 skipped=True（fetch 階段已 done），reorganize 無需執行")
            return 0
        print(f"⚠️ dates_fetched 為空，無數據可 reorganize")
        return 0

    print(f"📋 待 reorganize 日期: {dates_fetched}")

    # ── 2. 讀 staging 數據 ──
    staging_by_date = _load_staging_data(dates_fetched)
    if not staging_by_date:
        print("❌ staging 無有效數據，退出")
        return 1

    # 所有出現過的 ticker
    all_tickers = set()
    for date_str, by_ticker in staging_by_date.items():
        all_tickers.update(by_ticker.keys())
    all_tickers = sorted(all_tickers)
    print(f"📊 涉及 ticker: {len(all_tickers)}")

    # ── 3. 並發下載所有 ticker 現有 d.csv ──
    ticker_cache = _download_all_tickers(all_tickers)

    # ── 4. 逐日 merge ──
    print(f"\n🔀 開始 merge（{len(dates_fetched)} 個日期 × {len(all_tickers)} 個 ticker）")
    t_merge = time.monotonic()

    # verify_log: { ticker: 最新 date }
    verify_log = {}

    for ticker in all_tickers:
        existing_df = ticker_cache.get(ticker)  # None or DataFrame
        new_rows_by_date = {}
        for date_str in dates_fetched:
            by_ticker = staging_by_date.get(date_str, {})
            if ticker in by_ticker:
                new_rows_by_date[date_str] = by_ticker[ticker]

        if not new_rows_by_date:
            # 此 ticker 在所有 dates_fetched 中均無數據（已被過濾掉）
            continue

        merged = _merge_ticker(ticker, existing_df, new_rows_by_date)
        if merged.empty:
            continue

        _write_output_d_csv(ticker, merged)

        # 記錄最新日期用於核查
        if "date" in merged.columns:
            verify_log[ticker] = str(merged["date"].max())

    elapsed_merge = time.monotonic() - t_merge
    print(f"✅ merge 完成 | 涉及 ticker: {len(verify_log)} | 耗時: {elapsed_merge:.0f}s")

    # ── 5. upload_folder → HF ──
    output_path = os.path.join(OUTPUT_DIR, HF_TICKER_DIR)
    if not os.path.exists(output_path):
        print("⚠️ output 目錄為空，無文件可上傳")
        return 1

    try:
        _upload_to_hf(OUTPUT_DIR)
    except Exception as e:
        print(f"❌ upload_folder 失敗: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # ── 6. 全量核查 ──
    # 核查：所有已 merge 的 ticker，最新日期應包含 trading_date
    # （如果 trading_date 不在 dates_fetched 則核查對象是 dates_fetched 最大日期）
    expected_latest = max(dates_fetched) if dates_fetched else trading_date

    failed_tickers = [
        t for t, latest in verify_log.items()
        if latest < expected_latest
    ]

    total_tickers  = len(verify_log)
    passed_tickers = total_tickers - len(failed_tickers)
    print(f"\n🔍 全量核查: {passed_tickers}/{total_tickers} 通過")

    # ── 7. 更新 MongoDB ──
    db_obj = None
    try:
        db_obj = ReorganizeDB()

        if failed_tickers:
            print(f"⚠️ 核查失敗 ticker: {len(failed_tickers)} 個")
            print(f"   前 10 個: {failed_tickers[:10]}")
            db_obj.mark_partial(trading_date, failed_tickers)
            _send_telegram(
                f"⚠️ MP reorganize_daily 完整性告警\n"
                f"日期: {trading_date}\n"
                f"失敗 ticker: {len(failed_tickers)} 個\n"
                f"已記錄 partial，下次自動補跑"
            )
        else:
            db_obj.mark_done(trading_date, total_tickers)
            # 同時更新 checkpoint
            db_obj.save_checkpoint(trading_date)

        elapsed_total = time.monotonic() - t_start
        status_str = "PARTIAL" if failed_tickers else "SUCCESS"

        db_obj.write_task_log("run_mp_reorganize_daily", status_str, {
            "trading_date":    trading_date,
            "dates_fetched":   dates_fetched,
            "total_tickers":   total_tickers,
            "passed_tickers":  passed_tickers,
            "failed_tickers":  len(failed_tickers),
            "elapsed_s":       round(elapsed_total, 1),
        })

        if not failed_tickers:
            _send_telegram(
                f"✅ MP reorganize_daily 完成\n"
                f"日期: {trading_date}\n"
                f"核查: {passed_tickers}/{total_tickers} 通過\n"
                f"耗時: {elapsed_total:.0f}s"
            )

        print(f"\n✅ reorganize_daily ALL_DONE | "
              f"total: {total_tickers} | "
              f"failed: {len(failed_tickers)} | "
              f"耗時: {elapsed_total:.0f}s")

    finally:
        if db_obj:
            db_obj.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
