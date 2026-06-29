"""
run_mp_fetch.py — BC job：MP 數據拉取 v1.0

職責：
  從 MarketParquet API 拉取交易日數據，寫到本地 ./staging/ 供後續 job 使用。

模式：
  daily  — 正常模式。
           1. 查 MP_Full_Progress 找所有未解決缺口（status partial/failed, resolved=False）
           2. 自動補缺口 + 今日數據（今日若已 done 則跳過）
           3. 完整寫 Task_Log + MP_Full_Progress

  rebuild — 備用工具。
            強制重拉 trading_date 指定的單日，不查缺口，直接覆蓋。
            用於特殊情況手動修復。

輸出：
  ./staging/{date}.parquet  每日一個文件
  ./staging/summary.json    { trading_date, dates_fetched, ticker_counts, skipped }

環境變量（GitHub Actions Secrets）：
  MP_API_KEY   → MarketParquet API 鑰匙
  MONGO_URI    → MongoDB 連線
  TRADING_DATE → 目標交易日（YYYY-MM-DD），workflow input 傳入
  MODE         → daily / rebuild（預設 daily）

過濾條件（與 DC fetch_mp_full.py 一致）：
  close > 5.0, volume > 1,000,000

Python 3.9 兼容。
"""

import io
import json
import os
import sys
import time
from datetime import datetime

import pandas as pd
import pymongo
import pytz
import requests

# ─────────────────────────────────────────────
# 常數
# ─────────────────────────────────────────────

MP_BASE_URL  = "https://marketparquet.com/api/v1"
SLEEP_PER_REQ = 1.0
PRICE_MIN     = 5.0
VOLUME_MIN    = 1_000_000
STAGING_DIR   = "./staging"

EST_TZ = pytz.timezone("US/Eastern")


# ─────────────────────────────────────────────
# 環境變量
# ─────────────────────────────────────────────

def _mp_headers() -> dict:
    key = os.environ.get("MP_API_KEY", "")
    return {"Authorization": f"Bearer {key}"}

def _mongo_uri() -> str:
    return os.environ.get("MONGO_URI", "")

def _trading_date() -> str:
    return os.environ.get("TRADING_DATE", "")

def _mode() -> str:
    return os.environ.get("MODE", "daily").lower()


# ─────────────────────────────────────────────
# 時間工具
# ─────────────────────────────────────────────

def _now_est() -> str:
    return datetime.now(EST_TZ).strftime("%Y-%m-%dT%H:%M:%S")

def _now_iso() -> str:
    return datetime.now(EST_TZ).isoformat()


# ─────────────────────────────────────────────
# MongoDB
# ─────────────────────────────────────────────

class FetchDB:
    def __init__(self):
        uri = _mongo_uri()
        if not uri:
            raise RuntimeError("MONGO_URI 未設定")
        self.client    = pymongo.MongoClient(uri)
        self.stock_db  = self.client["StockData"]
        self.comm_db   = self.client["CommData"]
        self.progress  = self.stock_db["MP_Full_Progress"]
        self.task_log  = self.stock_db["Task_Log"]

    def close(self):
        self.client.close()

    def get_progress_doc(self, date_str: str) -> dict:
        doc = self.progress.find_one({"date": date_str})
        return doc or {}

    def get_unresolved_gaps(self) -> list:
        """查詢所有未解決缺口（status partial/failed, resolved=False，排除 _ 開頭 key）。"""
        docs = self.progress.find(
            {
                "status": {"$in": ["failed", "partial"]},
                "resolved": False,
                "date": {"$not": {"$regex": "^_"}},
            },
            {"date": 1, "status": 1},
            sort=[("date", 1)],
        )
        return [d["date"] for d in docs]

    def is_date_done(self, date_str: str) -> bool:
        doc = self.progress.find_one({"date": date_str}, {"status": 1})
        return bool(doc) and doc.get("status") == "done"

    def mark_fetched(self, date_str: str, ticker_count: int):
        self.progress.update_one(
            {"date": date_str},
            {"$set": {
                "date":       date_str,
                "year":       int(date_str[:4]),
                "status":     "fetched",
                "row_count":  ticker_count,
                "updated_at": _now_iso(),
            }},
            upsert=True,
        )

    def mark_fetch_failed(self, date_str: str, reason: str):
        self.progress.update_one(
            {"date": date_str},
            {"$set": {
                "date":       date_str,
                "year":       int(date_str[:4]),
                "status":     "failed",
                "reason":     reason,
                "resolved":   False,
                "updated_at": _now_iso(),
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
# MP API：下載單日 Parquet
# ─────────────────────────────────────────────

def _download_parquet(asset_type: str, date_str: str) -> pd.DataFrame:
    """下載單日 parquet，返回 DataFrame（失敗返回空 DataFrame）。"""
    for attempt in range(3):
        try:
            resp = requests.get(
                f"{MP_BASE_URL}/download/{asset_type}/{date_str}",
                headers=_mp_headers(),
                timeout=30,
            )
            time.sleep(SLEEP_PER_REQ)

            if resp.status_code == 429:
                wait = 60 * (attempt + 1)
                print(f"  ⚠️ 429 ({asset_type} {date_str})，等待 {wait}s 重試...")
                time.sleep(wait)
                continue

            if resp.status_code == 404:
                print(f"  ℹ️ {asset_type} {date_str}: 404 無數據")
                return pd.DataFrame()

            if resp.status_code != 200:
                print(f"  ❌ 取得 download_url 失敗 ({asset_type} {date_str}): {resp.status_code}")
                return pd.DataFrame()

            download_url = resp.json().get("download_url", "")
            if not download_url:
                print(f"  ⚠️ download_url 為空 ({asset_type} {date_str})")
                return pd.DataFrame()

            parquet_resp = requests.get(download_url, timeout=120)
            if parquet_resp.status_code != 200:
                print(f"  ❌ 下載 parquet 失敗 ({asset_type} {date_str}): {parquet_resp.status_code}")
                return pd.DataFrame()

            return pd.read_parquet(io.BytesIO(parquet_resp.content))

        except Exception as e:
            print(f"  ❌ 下載異常 ({asset_type} {date_str}) attempt={attempt + 1}: {e}")
            time.sleep(5)

    return pd.DataFrame()


def _fetch_single_date(date_str: str) -> pd.DataFrame:
    """
    拉取單日 stock + etf 數據，合併過濾後返回 DataFrame。
    columns: symbol, date, open, high, low, close, volume（全小寫）
    """
    print(f"📥 下載 stock_daily {date_str}...")
    df_stock = _download_parquet("stock_daily", date_str)
    if not df_stock.empty:
        print(f"  ✅ stock_daily {date_str}: {len(df_stock)} rows")
    else:
        print(f"  ⚠️ stock_daily {date_str}: 空")

    print(f"📥 下載 etf_daily {date_str}...")
    df_etf = _download_parquet("etf_daily", date_str)
    if not df_etf.empty:
        print(f"  ✅ etf_daily {date_str}: {len(df_etf)} rows")
    else:
        print(f"  ⚠️ etf_daily {date_str}: 空")

    frames = [f for f in [df_stock, df_etf] if not f.empty]
    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df.columns = [c.lower() for c in df.columns]

    # 標準化欄位
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].str.upper().str.strip()
    if "date" not in df.columns:
        df["date"] = date_str

    # 過濾（與 DC 一致）
    if "close" in df.columns and "volume" in df.columns:
        before = len(df)
        df = df[(df["close"] > PRICE_MIN) & (df["volume"] > VOLUME_MIN)]
        print(f"  🔍 過濾後：{before} → {len(df)} rows")

    # 確保必需欄位存在
    required = {"symbol", "date", "open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        print(f"  ❌ 缺少欄位: {missing}")
        return pd.DataFrame()

    df = df[list(required)].copy()
    df = df.dropna(subset=["symbol", "date"])
    df = df.reset_index(drop=True)

    return df


# ─────────────────────────────────────────────
# 本地 staging 讀寫
# ─────────────────────────────────────────────

def _write_staging(date_str: str, df: pd.DataFrame) -> str:
    """寫入 ./staging/{date}.parquet，返回文件路徑。"""
    os.makedirs(STAGING_DIR, exist_ok=True)
    path = os.path.join(STAGING_DIR, f"{date_str}.parquet")
    df.to_parquet(path, index=False)
    size_mb = os.path.getsize(path) / 1024 / 1024
    print(f"  💾 寫入 {path} | {len(df)} rows | {size_mb:.1f} MB")
    return path


def _write_summary(summary: dict):
    """寫入 ./staging/summary.json。"""
    os.makedirs(STAGING_DIR, exist_ok=True)
    path = os.path.join(STAGING_DIR, "summary.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"  📋 summary.json 已寫入: {path}")


# ─────────────────────────────────────────────
# Telegram 告警（通過 COMM_HUB_URL / GAS）
# ─────────────────────────────────────────────

def _send_telegram(msg: str):
    url    = os.environ.get("COMM_HUB_URL", "")
    secret = os.environ.get("WEBHOOK_SECRET", "")
    if not url:
        print(f"  ⚠️ COMM_HUB_URL 未設定，跳過 Telegram: {msg[:50]}")
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
# 主邏輯
# ─────────────────────────────────────────────

def run_daily(db: FetchDB, trading_date: str):
    """
    daily 模式：
    1. 查未解決缺口 → Telegram 告警
    2. 判斷 trading_date 是否已 done → 跳過
    3. 確定需要拉取的日期列表（缺口 + 今日）
    4. 逐日拉取、過濾、寫 staging
    5. 寫 summary.json + Task_Log + MP_Full_Progress
    """
    t_start = time.monotonic()
    print(f"\n🔄 fetch_mp daily 開始 | 交易日: {trading_date}")

    # 1. 查缺口
    gaps = db.get_unresolved_gaps()
    if gaps:
        print(f"⚠️ 發現 {len(gaps)} 個未解決缺口: {gaps}")
        _send_telegram(
            f"⚠️ MP 數據缺口告警\n"
            f"發現 {len(gaps)} 個未解決缺口：{', '.join(gaps)}\n"
            f"本次將自動補跑"
        )
    else:
        print("✅ 無未解決缺口")

    # 2. 判斷今日是否已 done
    today_already_done = db.is_date_done(trading_date)
    if today_already_done:
        print(f"✅ {trading_date} 已是 done 狀態，跳過今日拉取")

    # 3. 確定待拉取日期列表（缺口優先，今日排末）
    pending_dates = list(gaps)
    if not today_already_done:
        if trading_date not in pending_dates:
            pending_dates.append(trading_date)

    if not pending_dates:
        print(f"✅ 無需拉取（今日已 done，無缺口），直接退出")
        _write_summary({
            "trading_date":  trading_date,
            "dates_fetched": [],
            "ticker_counts": {},
            "skipped":       True,
            "reason":        "already_done_no_gaps",
        })
        db.write_task_log("run_mp_fetch", "SKIPPED", {
            "trading_date": trading_date,
            "reason":       "already_done_no_gaps",
        })
        return

    print(f"📋 待拉取日期（共 {len(pending_dates)} 個）: {pending_dates}")

    # 4. 逐日拉取
    dates_fetched  = []
    ticker_counts  = {}
    failed_dates   = []

    for date_str in pending_dates:
        print(f"\n── {date_str} ──")
        df = _fetch_single_date(date_str)
        if df.empty:
            print(f"  ❌ {date_str} 拉取失敗或無數據")
            db.mark_fetch_failed(date_str, "empty_response")
            failed_dates.append(date_str)
            continue

        ticker_count = df["symbol"].nunique()
        print(f"  📊 {date_str}: {ticker_count} 個 ticker，{len(df)} rows")

        _write_staging(date_str, df)
        db.mark_fetched(date_str, ticker_count)

        dates_fetched.append(date_str)
        ticker_counts[date_str] = ticker_count

    # 5. 寫 summary.json
    summary = {
        "trading_date":  trading_date,
        "dates_fetched": dates_fetched,
        "ticker_counts": ticker_counts,
        "failed_dates":  failed_dates,
        "skipped":       False,
    }
    _write_summary(summary)

    elapsed = time.monotonic() - t_start
    status  = "SUCCESS" if not failed_dates else "PARTIAL"

    print(f"\n✅ fetch_mp daily 完成 | "
          f"成功: {len(dates_fetched)} 個日期 | "
          f"失敗: {len(failed_dates)} 個 | "
          f"耗時: {elapsed:.0f}s")

    db.write_task_log("run_mp_fetch", status, {
        "mode":          "daily",
        "trading_date":  trading_date,
        "dates_fetched": dates_fetched,
        "failed_dates":  failed_dates,
        "ticker_counts": ticker_counts,
        "elapsed_s":     round(elapsed, 1),
    })

    if failed_dates:
        _send_telegram(
            f"⚠️ MP fetch 部分失敗\n"
            f"失敗日期: {', '.join(failed_dates)}\n"
            f"成功: {len(dates_fetched)} 個"
        )


def run_rebuild(db: FetchDB, trading_date: str):
    """
    rebuild 模式：
    強制重拉 trading_date 單日，覆蓋任何現有狀態。
    用於特殊情況手動修復。
    """
    t_start = time.monotonic()
    print(f"\n🔧 fetch_mp rebuild 開始 | 目標日期: {trading_date}")

    df = _fetch_single_date(trading_date)
    if df.empty:
        print(f"❌ {trading_date} 拉取失敗")
        db.mark_fetch_failed(trading_date, "empty_response_rebuild")
        _write_summary({
            "trading_date":  trading_date,
            "dates_fetched": [],
            "ticker_counts": {},
            "failed_dates":  [trading_date],
            "skipped":       False,
        })
        db.write_task_log("run_mp_fetch", "FAILED", {
            "mode":         "rebuild",
            "trading_date": trading_date,
            "reason":       "empty_response",
        })
        sys.exit(1)

    ticker_count = df["symbol"].nunique()
    print(f"📊 {trading_date}: {ticker_count} 個 ticker，{len(df)} rows")

    _write_staging(trading_date, df)
    db.mark_fetched(trading_date, ticker_count)

    summary = {
        "trading_date":  trading_date,
        "dates_fetched": [trading_date],
        "ticker_counts": {trading_date: ticker_count},
        "failed_dates":  [],
        "skipped":       False,
    }
    _write_summary(summary)

    elapsed = time.monotonic() - t_start
    print(f"✅ fetch_mp rebuild 完成 | {ticker_count} 個 ticker | 耗時: {elapsed:.0f}s")

    db.write_task_log("run_mp_fetch", "SUCCESS", {
        "mode":         "rebuild",
        "trading_date": trading_date,
        "ticker_count": ticker_count,
        "elapsed_s":    round(elapsed, 1),
    })


# ─────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────

def main() -> int:
    trading_date = _trading_date()
    mode         = _mode()

    if not trading_date:
        print("❌ TRADING_DATE 未設定")
        return 1

    if not os.environ.get("MP_API_KEY"):
        print("❌ MP_API_KEY 未設定")
        return 1

    print(f"🚀 run_mp_fetch.py 啟動 | mode={mode} | trading_date={trading_date}")

    try:
        db = FetchDB()
    except Exception as e:
        print(f"❌ 資料庫連線失敗: {e}")
        return 1

    try:
        if mode == "rebuild":
            run_rebuild(db, trading_date)
        else:
            run_daily(db, trading_date)
        return 0
    except Exception as e:
        print(f"❌ 執行異常: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
