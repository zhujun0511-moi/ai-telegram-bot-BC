"""
run_mp_reorganize_wm.py — BC job：MP 週/月線重組 v1.0

職責：
  讀 ./staging/summary.json 取得本次涉及 ticker 列表
  → 並發下載這些 ticker 的最新 d.csv
  → 從 d.csv resample → w.csv / m.csv（嚴格跳過未收盤最後一根）
  → 寫本地 ./output_wm/mp_data/ticker/*/w.csv + m.csv
  → upload_folder 一次寫回 HF Dataset
  → dispatch mp_nightly.yml
  → 寫 Task_Log

規則（DANGER_ZONES 全局約定）：
  W1 必須從 d.csv resample，禁止讀現有 w.csv（partial bar 污染風險）
  週線時間戳標準：週一（W-MON anchor，closed="left", label="left"）
  未完整最後一根嚴格跳過（is_week_complete=False → iloc[:-1]）
  月線同樣跳過未完整最後一根（is_month_complete=False → iloc[:-1]）
  is_week_complete / is_month_complete 由 trading_date 推算（週五=true，月末=true）

環境變量（GitHub Actions Secrets）：
  HF_TOKEN      → HF Dataset 讀寫
  HF_REPO_ID    → HF Dataset repo
  MONGO_URI     → MongoDB 寫 Task_Log
  GH_TOKEN      → GitHub API（dispatch mp_nightly.yml）
  GH_REPO       → BC repo 全名（如 zhujun0511-moi/ai-telegram-bot-BC）
  TRADING_DATE  → 目標交易日（workflow input）
  COMM_HUB_URL  → GAS Telegram proxy（可選）
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
from datetime import datetime, timedelta

import pandas as pd
import pymongo
import pytz
import requests
from huggingface_hub import HfApi

# ─────────────────────────────────────────────
# 常數
# ─────────────────────────────────────────────

HF_TICKER_DIR    = "mp_data/ticker"
STAGING_DIR      = "./staging"
OUTPUT_WM_DIR    = "./output_wm"
DOWNLOAD_WORKERS = 10

BC_BRANCH  = "main"
BC_WORKFLOW = "mp_nightly.yml"

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

def _gh_token() -> str:
    return os.environ.get("GH_TOKEN", "")

def _gh_repo() -> str:
    return os.environ.get("GH_REPO", "zhujun0511-moi/ai-telegram-bot-BC")


# ─────────────────────────────────────────────
# 時間工具
# ─────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(EST_TZ).isoformat()


# ─────────────────────────────────────────────
# 週/月完整性推算
# ─────────────────────────────────────────────

def _calc_week_month_complete(trading_date: str) -> tuple:
    """
    根據 trading_date 推算 is_week_complete / is_month_complete。

    is_week_complete:
      trading_date 是週五（weekday=4）→ True
      其他交易日 → False

    is_month_complete:
      trading_date 是當月最後一個交易日（下一個交易日跨月）→ True
      簡化判斷：trading_date 是當月最後一天，或 trading_date 是週五且
      date + 3 天已是下個月 → True
      此處用保守判斷：date 是當月最後一個週五或月末日
    """
    try:
        dt = datetime.strptime(trading_date, "%Y-%m-%d")
    except ValueError:
        return False, False

    weekday = dt.weekday()  # 0=Mon, 4=Fri

    is_week_complete = (weekday == 4)  # 週五

    # 月完整性：下一個自然日（跳過週末）是否跨月
    next_day = dt + timedelta(days=1)
    while next_day.weekday() >= 5:  # 跳過週末
        next_day += timedelta(days=1)
    is_month_complete = (next_day.month != dt.month)

    return is_week_complete, is_month_complete


# ─────────────────────────────────────────────
# MongoDB
# ─────────────────────────────────────────────

class WmDB:
    def __init__(self):
        uri = _mongo_uri()
        if not uri:
            raise RuntimeError("MONGO_URI 未設定")
        self.client   = pymongo.MongoClient(uri)
        self.stock_db = self.client["StockData"]
        self.task_log = self.stock_db["Task_Log"]

    def close(self):
        self.client.close()

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
        return
    try:
        resp = requests.post(
            url,
            json={"message": msg, "secret": secret},
            timeout=15,
        )
        print(f"  📨 Telegram: {resp.status_code}")
    except Exception as e:
        print(f"  ⚠️ Telegram 失敗: {e}")


# ─────────────────────────────────────────────
# 讀 summary.json
# ─────────────────────────────────────────────

def _load_summary() -> dict:
    path = os.path.join(STAGING_DIR, "summary.json")
    if not os.path.exists(path):
        raise FileNotFoundError(f"summary.json 不存在: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ─────────────────────────────────────────────
# HF Dataset 下載（並發）
# ─────────────────────────────────────────────

def _download_one_d_csv(ticker: str) -> tuple:
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
                print(f"  ⚠️ 下載 {ticker} d.csv 429，等待 70s")
                time.sleep(70)
                continue
            return ticker, None
        except Exception as e:
            if attempt == 0:
                time.sleep(2)
            else:
                print(f"  ❌ 下載 {ticker} d.csv: {e}")
    return ticker, None


def _download_tickers(tickers: list) -> dict:
    print(f"📥 並發下載 d.csv（{DOWNLOAD_WORKERS} threads）| ticker: {len(tickers)}")
    t0 = time.monotonic()
    cache = {}

    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as exe:
        futures = {exe.submit(_download_one_d_csv, t): t for t in tickers}
        done = 0
        for future in as_completed(futures):
            ticker, df = future.result()
            cache[ticker] = df
            done += 1
            if done % 500 == 0:
                print(f"  ⏳ {done}/{len(tickers)}")

    elapsed = time.monotonic() - t0
    found = sum(1 for v in cache.values() if v is not None)
    print(f"✅ 下載完成 | 有數據: {found} | 耗時: {elapsed:.0f}s")
    return cache


# ─────────────────────────────────────────────
# Resample 計算（與 DC mp_reorganize.py 完全一致）
# ─────────────────────────────────────────────

def _build_ohlcv_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates("date")
    df = df.set_index("date")
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close", "volume"])
    return df


def _resample_weekly(df_d: pd.DataFrame) -> pd.DataFrame:
    """日線 → 週線（W-MON 錨點，DANGER_ZONES 規定）"""
    df = _build_ohlcv_df(df_d)
    resampled = df.resample("W-MON", label="left", closed="left").agg({
        "open":   "first",
        "high":   "max",
        "low":    "min",
        "close":  "last",
        "volume": "sum",
    }).dropna(subset=["open", "high", "low", "close"])
    resampled = resampled.reset_index()
    resampled["date"] = resampled["date"].dt.strftime("%Y-%m-%d")
    return resampled[["date", "open", "high", "low", "close", "volume"]]


def _resample_monthly(df_d: pd.DataFrame) -> pd.DataFrame:
    """日線 → 月線（MS 錨點，DANGER_ZONES 規定）"""
    df = _build_ohlcv_df(df_d)
    resampled = df.resample("MS", label="left", closed="left").agg({
        "open":   "first",
        "high":   "max",
        "low":    "min",
        "close":  "last",
        "volume": "sum",
    }).dropna(subset=["open", "high", "low", "close"])
    resampled = resampled.reset_index()
    resampled["date"] = resampled["date"].dt.strftime("%Y-%m-%d")
    return resampled[["date", "open", "high", "low", "close", "volume"]]


# ─────────────────────────────────────────────
# 本地輸出寫入
# ─────────────────────────────────────────────

def _write_output_csv(ticker: str, filename: str, df: pd.DataFrame):
    dir_path = os.path.join(OUTPUT_WM_DIR, HF_TICKER_DIR, ticker)
    os.makedirs(dir_path, exist_ok=True)
    df.to_csv(os.path.join(dir_path, filename), index=False)


# ─────────────────────────────────────────────
# HF upload_folder
# ─────────────────────────────────────────────

def _upload_to_hf():
    print(f"⬆️ upload_folder 開始 | 目錄: {OUTPUT_WM_DIR}")
    t0  = time.monotonic()
    api = HfApi(token=_hf_token())
    api.upload_folder(
        folder_path=OUTPUT_WM_DIR,
        repo_id=_hf_repo(),
        repo_type="dataset",
        commit_message="mp_reorganize_wm: weekly/monthly update",
    )
    elapsed = time.monotonic() - t0
    print(f"✅ upload_folder 完成 | 耗時: {elapsed:.0f}s")


# ─────────────────────────────────────────────
# Dispatch mp_nightly.yml
# ─────────────────────────────────────────────

def _dispatch_mp_nightly(trading_date: str) -> bool:
    token = _gh_token()
    repo  = _gh_repo()
    if not token:
        print("❌ GH_TOKEN 未設定，無法 dispatch mp_nightly.yml")
        return False

    url = f"https://api.github.com/repos/{repo}/actions/workflows/{BC_WORKFLOW}/dispatches"
    payload = {
        "ref":    BC_BRANCH,
        "inputs": {"trigger_date": trading_date},
    }
    headers = {
        "Authorization":        f"Bearer {token}",
        "Accept":               "application/vnd.github+json",
        "Content-Type":         "application/json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        if resp.status_code == 204:
            print(f"✅ dispatch mp_nightly.yml 成功 | trading_date={trading_date}")
            return True
        print(f"❌ dispatch 失敗: {resp.status_code} {resp.text[:200]}")
        return False
    except Exception as e:
        print(f"❌ dispatch 異常: {e}")
        return False


# ─────────────────────────────────────────────
# 主邏輯
# ─────────────────────────────────────────────

def main() -> int:
    trading_date = _trading_date()
    if not trading_date:
        print("❌ TRADING_DATE 未設定")
        return 1

    print(f"🚀 run_mp_reorganize_wm.py 啟動 | trading_date={trading_date}")
    t_start = time.monotonic()

    # ── 1. 讀 summary.json 取得本次 ticker 列表 ──
    try:
        summary = _load_summary()
    except FileNotFoundError as e:
        print(f"❌ {e}")
        return 1

    dates_fetched = summary.get("dates_fetched", [])
    if not dates_fetched:
        if summary.get("skipped"):
            print("✅ summary skipped=True，wm 無需執行，直接 dispatch mp_nightly")
            _dispatch_mp_nightly(trading_date)
            return 0
        print("⚠️ dates_fetched 為空，wm 無數據可處理")
        return 0

    # 從 staging parquet 取得所有涉及 ticker
    all_tickers = set()
    for date_str in dates_fetched:
        path = os.path.join(STAGING_DIR, f"{date_str}.parquet")
        if not os.path.exists(path):
            continue
        try:
            df = pd.read_parquet(path)
            df.columns = [c.lower() for c in df.columns]
            if "symbol" in df.columns:
                all_tickers.update(df["symbol"].str.upper().str.strip().tolist())
        except Exception as e:
            print(f"  ⚠️ 讀取 staging {date_str}: {e}")

    if not all_tickers:
        print("⚠️ 無 ticker 可處理")
        return 0

    all_tickers = sorted(all_tickers)
    print(f"📊 涉及 ticker: {len(all_tickers)}")

    # ── 2. 推算週/月完整性 ──
    is_week_complete, is_month_complete = _calc_week_month_complete(trading_date)
    print(f"📅 is_week_complete={is_week_complete} | is_month_complete={is_month_complete}")

    # ── 3. 並發下載最新 d.csv（reorganize_daily 已更新過） ──
    ticker_cache = _download_tickers(all_tickers)

    # ── 4. 計算 w.csv / m.csv ──
    print(f"\n📊 計算 w/m.csv（ticker: {len(all_tickers)}）")
    t_calc = time.monotonic()

    ok_count = 0
    skip_count = 0
    err_count = 0

    for ticker in all_tickers:
        df_d = ticker_cache.get(ticker)
        if df_d is None or df_d.empty:
            skip_count += 1
            continue

        try:
            # W1（必須從 d.csv resample，禁止讀 w.csv）
            df_w = _resample_weekly(df_d)
            if df_w.empty:
                skip_count += 1
                continue

            # 未完整週嚴格跳過最後一根
            if not is_week_complete and len(df_w) > 0:
                df_w = df_w.iloc[:-1].reset_index(drop=True)

            if df_w.empty:
                skip_count += 1
                continue

            _write_output_csv(ticker, "w.csv", df_w)

            # M1
            df_m = _resample_monthly(df_d)
            if not df_m.empty:
                # 未完整月嚴格跳過最後一根
                if not is_month_complete and len(df_m) > 0:
                    df_m = df_m.iloc[:-1].reset_index(drop=True)
                if not df_m.empty:
                    _write_output_csv(ticker, "m.csv", df_m)

            ok_count += 1

        except Exception as e:
            print(f"  ❌ {ticker} 計算失敗: {e}")
            err_count += 1

    elapsed_calc = time.monotonic() - t_calc
    print(f"✅ 計算完成 | ok: {ok_count} | skip: {skip_count} | err: {err_count} | 耗時: {elapsed_calc:.0f}s")

    # ── 5. upload_folder → HF ──
    output_path = os.path.join(OUTPUT_WM_DIR, HF_TICKER_DIR)
    if not os.path.exists(output_path):
        print("⚠️ output_wm 目錄為空，無文件可上傳")
    else:
        try:
            _upload_to_hf()
        except Exception as e:
            print(f"❌ upload_folder 失敗: {e}")
            import traceback
            traceback.print_exc()
            # 繼續執行 dispatch（不因 upload 失敗阻塞 mp_nightly）

    # ── 6. dispatch mp_nightly.yml ──
    dispatch_ok = _dispatch_mp_nightly(trading_date)
    if not dispatch_ok:
        _send_telegram(
            f"⚠️ MP reorganize_wm 完成但 dispatch mp_nightly 失敗\n"
            f"日期: {trading_date}\n請手動觸發 mp_nightly.yml"
        )

    # ── 7. Task_Log ──
    elapsed_total = time.monotonic() - t_start
    db_obj = None
    try:
        db_obj = WmDB()
        db_obj.write_task_log("run_mp_reorganize_wm", "SUCCESS", {
            "trading_date":      trading_date,
            "dates_fetched":     dates_fetched,
            "total_tickers":     len(all_tickers),
            "ok_count":          ok_count,
            "skip_count":        skip_count,
            "err_count":         err_count,
            "is_week_complete":  is_week_complete,
            "is_month_complete": is_month_complete,
            "dispatch_ok":       dispatch_ok,
            "elapsed_s":         round(elapsed_total, 1),
        })
    except Exception as e:
        print(f"⚠️ Task_Log 寫入失敗: {e}")
    finally:
        if db_obj:
            db_obj.close()

    print(f"\n✅ reorganize_wm ALL_DONE | "
          f"ok: {ok_count} | 耗時: {elapsed_total:.0f}s")

    return 0


if __name__ == "__main__":
    sys.exit(main())
