"""
run_mp_indicator_calc.py — BC job1：MP 指標計算 v1.0

職責（只做一件事）：
  讀 HF mp_data/ticker/XXX/d.csv
  → 計算 D1/W1 技術指標（EMA/ATR/RSI/RVOL/dist 等）
  → append 一行到 mp_data/ticker/XXX/indicators.csv

不做的事：
  - 不計算道氏結構（由 run_mp_dow_structure.py 負責）
  - 不計算 phase（由 run_phase_calc.py 負責）
  - 不寫 structure.json

觸發方式：
  DC reorganize wm ALL_DONE → dispatch GitHub Actions mp_nightly.yml → job1

進度追蹤：
  StockData.System_State  id="mp_indicator_calc"
  每天重置：date ≠ 今天 → 清空 completed_tickers

未收盤 bar 保護：
  讀 mp_data/meta/daily_status.json
  is_week_complete=false → W1 截斷最後一根，W1 欄位留空字串
  daily_status.json 不存在 → STATUS_MISSING，W1 不截斷，輸出 warning

環境變量（GitHub Actions Secrets）：
  HF_TOKEN   → HF Dataset 讀寫
  HF_REPO_ID → HF Dataset repo
  MONGO_URI  → MongoDB 進度存取

Python 3.9 兼容。
"""

import os
import io
import re
import sys
import json
import base64
import time
from datetime import datetime
from typing import Optional

import requests
import pandas as pd
import numpy as np
import ta
import pymongo
import pytz

# ─────────────────────────────────────────────
# 常數
# ─────────────────────────────────────────────

HF_REPO_ID    = os.getenv("HF_REPO_ID", "zhujun0511-AI/ai-telegram-bot-dataset")
HF_API_BASE   = "https://huggingface.co/api/datasets"
HF_TICKER_DIR = "mp_data/ticker"
HF_META_DIR   = "mp_data/meta"

BATCH_SIZE    = 200   # 每批 ticker 數，commit 一次
SLEEP_AFTER_COMMIT = 2.0  # commit 後等待（避免 HF rate limit）

EST_TZ = pytz.timezone("US/Eastern")
PROGRESS_KEY = "mp_indicator_calc"

# indicators.csv 欄位順序（固定，append 時用）
INDICATOR_COLS = [
    "date", "updated_at",
    # D1
    "ema9_d1", "ema20_d1", "ema50_d1",
    "atr22_d1", "atr_ratio_d1",
    "rsi14_d1", "rvol_d1",
    "trend_d1",
    "dist_52h_d", "dist_d1", "dist_pct_rank_60d",
    "ema9_slope_d1", "ema20_slope_d1",
    # W1
    "ema9_w1", "ema20_w1", "ema50_w1",
    "atr22_w1", "atr_ratio_w1",
    "rsi14_w1", "rvol_w1",
    "trend_w1",
]


# ─────────────────────────────────────────────
# 環境變量讀取
# ─────────────────────────────────────────────

def _hf_token() -> str:
    return os.getenv("HF_TOKEN", "")

def _hf_headers() -> dict:
    return {
        "Authorization": f"Bearer {_hf_token()}",
        "Content-Type":  "application/json",
    }

def _mongo_uri() -> str:
    return os.getenv("MONGO_URI", "")


# ─────────────────────────────────────────────
# MongoDB 進度追蹤
# ─────────────────────────────────────────────

class ProgressDB:
    def __init__(self):
        uri = _mongo_uri()
        if not uri:
            raise RuntimeError("MONGO_URI 未設定")
        self.client = pymongo.MongoClient(uri)
        self.col    = self.client["StockData"]["System_State"]

    def _today(self) -> str:
        return datetime.now(EST_TZ).strftime("%Y-%m-%d")

    def _now_iso(self) -> str:
        return datetime.now(EST_TZ).strftime("%Y-%m-%dT%H:%M:%S")

    def get(self) -> dict:
        doc = self.col.find_one({"id": PROGRESS_KEY})
        return doc or {}

    def reset_if_new_day(self, all_tickers: list) -> dict:
        """
        若今天是新的一天，重置進度。
        若 all_tickers 已存在且 date=今天，返回現有進度。
        """
        today = self._today()
        doc   = self.get()

        if doc.get("date") != today:
            print(f"📅 [進度] 新的一天（{today}），重置進度，共 {len(all_tickers)} 個 ticker")
            new_doc = {
                "id":                PROGRESS_KEY,
                "date":              today,
                "status":            "running",
                "all_tickers":       all_tickers,
                "total_tickers":     len(all_tickers),
                "completed_tickers": [],
                "completed_count":   0,
                "last_updated":      self._now_iso(),
            }
            self.col.update_one(
                {"id": PROGRESS_KEY},
                {"$set": new_doc},
                upsert=True,
            )
            return new_doc
        else:
            print(f"📅 [進度] 今天（{today}），斷點續跑，"
                  f"已完成 {doc.get('completed_count', 0)}/{doc.get('total_tickers', 0)}")
            return doc

    def save_all_tickers(self, all_tickers: list):
        """第一批掃描完後，把 all_tickers 存入進度（之後復用，不重掃）。"""
        self.col.update_one(
            {"id": PROGRESS_KEY},
            {"$set": {
                "all_tickers":   all_tickers,
                "total_tickers": len(all_tickers),
                "last_updated":  self._now_iso(),
            }},
            upsert=True,
        )

    def update_completed(self, completed: set):
        self.col.update_one(
            {"id": PROGRESS_KEY},
            {"$set": {
                "completed_tickers": sorted(list(completed)),
                "completed_count":   len(completed),
                "status":            "running",
                "last_updated":      self._now_iso(),
            }},
            upsert=True,
        )

    def mark_done(self):
        self.col.update_one(
            {"id": PROGRESS_KEY},
            {"$set": {"status": "done", "last_updated": self._now_iso()}},
            upsert=True,
        )

    def close(self):
        self.client.close()


# ─────────────────────────────────────────────
# HF Dataset 工具
# ─────────────────────────────────────────────

def _hf_download(path: str) -> Optional[bytes]:
    url = f"https://huggingface.co/datasets/{HF_REPO_ID}/resolve/main/{path}"
    for attempt in range(2):
        try:
            resp = requests.get(
                url,
                headers={"Authorization": f"Bearer {_hf_token()}"},
                timeout=30,
            )
            if resp.status_code == 200:
                return resp.content
            if resp.status_code == 404:
                return None
            if resp.status_code == 429:
                print(f"⚠️ [HF] 下載 429，等待 70 秒重試: {path}")
                time.sleep(70)
                continue
            print(f"⚠️ [HF] 下載失敗 {resp.status_code}: {path}")
            return None
        except Exception as e:
            print(f"❌ [HF] 下載異常: {path}: {e}")
            return None
    return None


def _hf_batch_commit(files: list, message: str) -> bool:
    """
    批次 commit 多個文件到 HF Dataset。
    files 格式：[{"path": str, "content_b64": str}, ...]
    """
    if not files:
        return True
    try:
        file_payloads = [
            {"path": f["path"], "content": f["content_b64"], "encoding": "base64"}
            for f in files
        ]
        url     = f"{HF_API_BASE}/{HF_REPO_ID}/commit/main"
        payload = {"summary": message, "files": file_payloads}

        for attempt in range(2):
            resp = requests.post(url, headers=_hf_headers(), json=payload, timeout=120)
            if resp.status_code in (200, 201):
                return True
            if resp.status_code == 429:
                print(f"⚠️ [HF] commit 429，等待 70 秒重試")
                time.sleep(70)
                continue
            print(f"❌ [HF] commit 失敗 {resp.status_code}: {resp.text[:200]}")
            return False
        return False
    except Exception as e:
        print(f"❌ [HF] commit 異常: {e}")
        return False


def _hf_scan_ticker_dirs() -> list:
    """
    cursor 翻頁掃描 mp_data/ticker/ 所有 ticker 目錄。
    返回 ticker 名稱列表（排除含底線的異常目錄）。
    """
    all_tickers = []
    next_url = (
        f"{HF_API_BASE}/{HF_REPO_ID}/tree/main/{HF_TICKER_DIR}"
        f"?recursive=false&expand=false"
    )
    page = 0
    while next_url:
        try:
            resp = requests.get(next_url, headers=_hf_headers(), timeout=30)
            if resp.status_code == 429:
                print(f"⚠️ [掃描] 429，等待 70 秒重試（第 {page+1} 頁）")
                time.sleep(70)
                resp = requests.get(next_url, headers=_hf_headers(), timeout=30)
            if resp.status_code != 200:
                print(f"❌ [掃描] 失敗 {resp.status_code}（第 {page+1} 頁）")
                break
            items = resp.json()
            if not isinstance(items, list):
                break
            page += 1
            dirs = [
                item["path"].split("/")[-1]
                for item in items
                if item.get("type") == "directory"
                and "_" not in item["path"].split("/")[-1]
            ]
            all_tickers.extend(dirs)
            print(f"  📄 掃描第 {page} 頁：{len(items)} items，"
                  f"{len(dirs)} 個 ticker，累計 {len(all_tickers)}")
            # Link header 取下一頁
            link = resp.headers.get("Link", "")
            m    = re.search(r'<([^>]+)>;\s*rel="next"', link)
            next_url = m.group(1) if m else None
        except Exception as e:
            print(f"❌ [掃描] 異常（第 {page+1} 頁）: {e}")
            break
    print(f"✅ [掃描] 完成：共 {len(all_tickers)} 個 ticker，{page} 頁")
    return all_tickers


# ─────────────────────────────────────────────
# daily_status.json 讀取
# ─────────────────────────────────────────────

def _load_daily_status() -> dict:
    """
    讀取 HF mp_data/meta/daily_status.json。
    不存在時返回 {}，調用方需處理 STATUS_MISSING。
    """
    raw = _hf_download(f"{HF_META_DIR}/daily_status.json")
    if raw is None:
        print("⚠️ [daily_status] 文件不存在，W1 計算將不截斷（保守處理）")
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as e:
        print(f"⚠️ [daily_status] 解析失敗: {e}，W1 計算將不截斷")
        return {}


# ─────────────────────────────────────────────
# 數據讀取與 resample
# ─────────────────────────────────────────────

def _read_d_csv(ticker: str) -> Optional[pd.DataFrame]:
    """讀取 d.csv，返回舊在前 DataFrame，欄位為全名（open/high/low/close/volume）。"""
    path = f"{HF_TICKER_DIR}/{ticker}/d.csv"
    raw  = _hf_download(path)
    if raw is None:
        return None
    try:
        df = pd.read_csv(io.BytesIO(raw))
        df.columns = [c.lower() for c in df.columns]
        required = {"date", "open", "high", "low", "close", "volume"}
        if not required.issubset(set(df.columns)):
            print(f"  ⚠️ {ticker}: d.csv 缺少必要欄位")
            return None
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["open", "high", "low", "close"])
        df = df.sort_values("date").reset_index(drop=True)
        if len(df) < 50:
            return None
        return df
    except Exception as e:
        print(f"  ❌ {ticker}: d.csv 解析失敗: {e}")
        return None


def _resample_weekly(df_d: pd.DataFrame) -> pd.DataFrame:
    """
    日線 → 週線（W-MON 錨點）。
    與 mp_reorganize._resample_weekly 完全一致（禁止跨文件 import，複製）。
    """
    df = df_d.copy()
    df = df.set_index("date")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close", "volume"])
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


# ─────────────────────────────────────────────
# 指標計算輔助函數（完整借鑒 DC indicator_logic.py）
# ─────────────────────────────────────────────

def _calc_atr_ratio(atr_series: pd.Series, window: int = 20) -> Optional[float]:
    if atr_series is None or len(atr_series) < window + 1:
        return None
    recent = atr_series.dropna().tail(window + 1)
    if len(recent) < window + 1:
        return None
    current_atr     = recent.iloc[-1]
    historical_mean = recent.iloc[:-1].mean()
    if historical_mean == 0:
        return None
    return round(float(current_atr / historical_mean), 3)


def _calc_ema_slope(ema_series: pd.Series, n: int, atr22: float) -> Optional[float]:
    """EMA 斜率 = (EMA[-1] - EMA[-(n+1)]) / n / ATR22"""
    if atr22 is None or atr22 == 0:
        return None
    clean = ema_series.dropna()
    if len(clean) < n + 1:
        return None
    slope = (float(clean.iloc[-1]) - float(clean.iloc[-(n + 1)])) / n / atr22
    return round(slope, 4)


def _calc_dist(close: float, ema20: float, atr22: float) -> Optional[float]:
    if atr22 is None or atr22 == 0:
        return None
    return round((close - ema20) / atr22, 3)


def _dist_52h(df_d: pd.DataFrame) -> Optional[float]:
    """當前收盤價距52週高點百分比（需 >= 252 根）。"""
    if len(df_d) < 252:
        return None
    recent_252    = df_d.tail(252)
    high_52w      = recent_252["h"].max()
    current_close = df_d.iloc[-1]["c"]
    if high_52w == 0:
        return None
    return round((current_close - high_52w) / high_52w * 100, 2)


# ─────────────────────────────────────────────
# 核心計算：D1 指標
# ─────────────────────────────────────────────

def _calc_d1(df_raw: pd.DataFrame) -> dict:
    """
    計算 D1 所有指標。
    輸入 df_raw：舊在前，全名欄位（open/high/low/close/volume）。
    計算前 rename 為單字母（c/h/l/v）。
    """
    df = df_raw.copy()
    df = df.rename(columns={"close": "c", "high": "h", "low": "l",
                             "open": "o", "volume": "v"})

    r = {}
    ema20_series = None
    atr22_series = None

    if len(df) >= 9:
        r["ema9_d1"] = round(ta.trend.ema_indicator(df["c"], window=9).iloc[-1], 2)

    if len(df) >= 50:
        ema20_series    = ta.trend.ema_indicator(df["c"], window=20)
        r["ema20_d1"]   = round(ema20_series.iloc[-1], 2)
        r["ema50_d1"]   = round(ta.trend.ema_indicator(df["c"], window=50).iloc[-1], 2)

    if len(df) >= 22:
        atr22_series    = ta.volatility.average_true_range(
            df["h"], df["l"], df["c"], window=22)
        r["atr22_d1"]   = round(atr22_series.iloc[-1], 2)

    if len(df) >= 14:
        r["rsi14_d1"]   = round(ta.momentum.rsi(df["c"], window=14).iloc[-1], 2)

    if len(df) >= 23:
        avg_v = df["v"].iloc[-23:-1].mean()
        r["rvol_d1"] = round(df["v"].iloc[-1] / avg_v, 2) if avg_v != 0 else 0.0

    # trend_d1（EMA 排列）
    if all(k in r for k in ["ema9_d1", "ema20_d1", "ema50_d1"]):
        if r["ema9_d1"] > r["ema20_d1"] > r["ema50_d1"]:
            r["trend_d1"] = "bull"
        elif r["ema9_d1"] < r["ema20_d1"] < r["ema50_d1"]:
            r["trend_d1"] = "bear"
        else:
            r["trend_d1"] = "neutral"

    # dist_52h_d（需要原始 h 欄位）
    dist_52h = _dist_52h(df)
    if dist_52h is not None:
        r["dist_52h_d"] = dist_52h

    atr22_val = r.get("atr22_d1")
    ema20_val = r.get("ema20_d1")

    # dist_d1
    if ema20_val is not None and atr22_val is not None:
        d = _calc_dist(float(df["c"].iloc[-1]), ema20_val, atr22_val)
        if d is not None:
            r["dist_d1"] = d

    # dist_pct_rank_60d
    if ema20_series is not None and atr22_series is not None:
        ema20_full = ema20_series.dropna()
        atr22_full = atr22_series.dropna()
        min_len    = min(len(ema20_full), len(atr22_full), len(df))
        if min_len >= 5:
            closes    = df["c"].values[-min_len:]
            ema20_arr = ema20_full.values[-min_len:]
            atr22_arr = atr22_full.values[-min_len:]
            dist_full = np.array([
                (c - e) / a if a != 0 else np.nan
                for c, e, a in zip(closes, ema20_arr, atr22_arr)
            ])
            dist_valid = dist_full[~np.isnan(dist_full)]
            hist_60    = dist_valid[-60:] if len(dist_valid) >= 5 else None
            if hist_60 is not None and len(hist_60) >= 5:
                today_dist = dist_valid[-1]
                pct_rank   = float(np.sum(hist_60 <= today_dist) / len(hist_60) * 100)
                r["dist_pct_rank_60d"] = round(pct_rank, 1)

    # atr_ratio_d1
    if atr22_series is not None:
        ratio = _calc_atr_ratio(atr22_series, window=20)
        if ratio is not None:
            r["atr_ratio_d1"] = ratio

    # ema9_slope_d1（n=5）
    if len(df) >= 9 + 5:
        ema9_series = ta.trend.ema_indicator(df["c"], window=9)
        slope = _calc_ema_slope(ema9_series, n=5, atr22=atr22_val)
        if slope is not None:
            r["ema9_slope_d1"] = slope

    # ema20_slope_d1（n=5）
    if ema20_series is not None and atr22_val is not None:
        slope = _calc_ema_slope(ema20_series, n=5, atr22=atr22_val)
        if slope is not None:
            r["ema20_slope_d1"] = slope

    return r


# ─────────────────────────────────────────────
# 核心計算：W1 指標
# ─────────────────────────────────────────────

def _calc_w1(df_raw: pd.DataFrame) -> dict:
    """
    計算 W1 所有指標。
    輸入 df_raw：舊在前，全名欄位（open/high/low/close/volume）。
    調用前已按 daily_status 截斷最後一根（若 is_week_complete=false）。
    """
    df = df_raw.copy()
    df = df.rename(columns={"close": "c", "high": "h", "low": "l",
                             "open": "o", "volume": "v"})

    r = {}
    ema20_series = None
    atr22_series = None

    if len(df) >= 9:
        r["ema9_w1"] = round(ta.trend.ema_indicator(df["c"], window=9).iloc[-1], 2)

    if len(df) >= 50:
        ema20_series  = ta.trend.ema_indicator(df["c"], window=20)
        r["ema20_w1"] = round(ema20_series.iloc[-1], 2)
        r["ema50_w1"] = round(ta.trend.ema_indicator(df["c"], window=50).iloc[-1], 2)

    if len(df) >= 22:
        atr22_series  = ta.volatility.average_true_range(
            df["h"], df["l"], df["c"], window=22)
        r["atr22_w1"] = round(atr22_series.iloc[-1], 2)

    if len(df) >= 14:
        r["rsi14_w1"] = round(ta.momentum.rsi(df["c"], window=14).iloc[-1], 2)

    if len(df) >= 51:
        avg_v = df["v"].iloc[-51:-1].mean()
        r["rvol_w1"] = round(df["v"].iloc[-1] / avg_v, 2) if avg_v != 0 else 0.0

    # atr_ratio_w1
    if atr22_series is not None:
        ratio = _calc_atr_ratio(atr22_series, window=20)
        if ratio is not None:
            r["atr_ratio_w1"] = ratio

    # trend_w1（EMA 排列）
    if all(k in r for k in ["ema9_w1", "ema20_w1", "ema50_w1"]):
        if r["ema9_w1"] > r["ema20_w1"] > r["ema50_w1"]:
            r["trend_w1"] = "bull"
        elif r["ema9_w1"] < r["ema20_w1"] < r["ema50_w1"]:
            r["trend_w1"] = "bear"
        else:
            r["trend_w1"] = "neutral"

    return r


# ─────────────────────────────────────────────
# indicators.csv 讀取與 append
# ─────────────────────────────────────────────

def _read_indicators_csv(ticker: str) -> Optional[pd.DataFrame]:
    """讀取現有 indicators.csv，不存在返回 None。"""
    path = f"{HF_TICKER_DIR}/{ticker}/indicators.csv"
    raw  = _hf_download(path)
    if raw is None:
        return None
    try:
        df = pd.read_csv(io.BytesIO(raw), dtype=str)
        df.columns = [c.lower() for c in df.columns]
        return df
    except Exception as e:
        print(f"  ⚠️ {ticker}: indicators.csv 解析失敗: {e}")
        return None


def _build_new_row(today_str: str, d1: dict, w1: dict) -> dict:
    """組裝今日一行（所有欄位，缺失填空字串）。"""
    now_str = datetime.now(EST_TZ).strftime("%Y-%m-%dT%H:%M:%S")
    row = {"date": today_str, "updated_at": now_str}
    for col in INDICATOR_COLS[2:]:  # 跳過 date, updated_at
        if col in d1:
            row[col] = d1[col]
        elif col in w1:
            row[col] = w1[col]
        else:
            row[col] = ""
    return row


def _append_or_update_row(existing: Optional[pd.DataFrame],
                           new_row: dict,
                           today_str: str) -> pd.DataFrame:
    """
    將新行 append 到 existing（舊在前新在後）。
    若最後一行 date == today_str，覆蓋（upsert 語義，防止重複 append）。
    """
    new_df = pd.DataFrame([new_row], columns=INDICATOR_COLS)
    # 確保所有欄位存在
    for col in INDICATOR_COLS:
        if col not in new_df.columns:
            new_df[col] = ""

    if existing is None or existing.empty:
        return new_df

    # 補齊 existing 缺少的欄位
    for col in INDICATOR_COLS:
        if col not in existing.columns:
            existing[col] = ""
    existing = existing[INDICATOR_COLS]

    # 最後一行若是今天，覆蓋
    if len(existing) > 0 and str(existing.iloc[-1].get("date", "")) == today_str:
        existing = existing.iloc[:-1]

    return pd.concat([existing, new_df], ignore_index=True)


def _df_to_csv_b64(df: pd.DataFrame) -> str:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return base64.b64encode(csv_bytes).decode()


# ─────────────────────────────────────────────
# 單 ticker 處理
# ─────────────────────────────────────────────

def _process_ticker(ticker: str,
                    today_str: str,
                    is_week_complete: bool) -> Optional[dict]:
    """
    處理單個 ticker，返回 {"path": ..., "content_b64": ...} 或 None（跳過）。
    """
    # 讀 d.csv
    df_d = _read_d_csv(ticker)
    if df_d is None:
        print(f"  ⚠️ {ticker}: d.csv 缺失或不足，跳過")
        return None

    # 計算 D1
    d1 = _calc_d1(df_d)

    # resample W1
    w1 = {}
    try:
        # d.csv 的 date 欄位是 pd.Timestamp，resample 前需確保 datetime index
        df_d_for_resample = df_d.copy()
        # date 已是 pd.Timestamp（_read_d_csv 做了 pd.to_datetime）
        df_w = _resample_weekly(df_d_for_resample)

        if is_week_complete:
            w1 = _calc_w1(df_w)
            w1_note = "完整"
        else:
            # 截斷最後一根未收盤週線
            if len(df_w) > 1:
                df_w_trimmed = df_w.iloc[:-1].reset_index(drop=True)
                w1 = _calc_w1(df_w_trimmed)
            w1_note = "截斷(partial)"
    except Exception as e:
        w1_note = f"W1計算異常:{e}"
        w1 = {}

    # 讀現有 indicators.csv
    existing = _read_indicators_csv(ticker)

    # 組裝新行
    new_row = _build_new_row(today_str, d1, w1)

    # Append / upsert
    updated_df = _append_or_update_row(existing, new_row, today_str)

    # 轉 CSV base64
    content_b64 = _df_to_csv_b64(updated_df)

    print(f"  ✅ {ticker} | D1:{len(d1)}欄 W1:{len(w1)}欄({w1_note}) | "
          f"indicators.csv 共 {len(updated_df)} 行")

    return {
        "path":        f"{HF_TICKER_DIR}/{ticker}/indicators.csv",
        "content_b64": content_b64,
    }


# ─────────────────────────────────────────────
# 主入口
# ─────────────────────────────────────────────

def main():
    start_time = time.monotonic()
    today_str  = datetime.now(EST_TZ).strftime("%Y-%m-%d")

    print(f"🚀 [mp_indicator_calc] 開始 | 日期: {today_str}")

    # 初始化進度 DB
    try:
        pdb = ProgressDB()
    except Exception as e:
        print(f"❌ [mp_indicator_calc] MongoDB 連接失敗: {e}")
        sys.exit(1)

    # 讀 daily_status.json
    daily_status     = _load_daily_status()
    is_week_complete = daily_status.get("is_week_complete", True)  # 不存在時保守不截斷
    if not daily_status:
        print("⚠️ [mp_indicator_calc] STATUS_MISSING：W1 不截斷（保守處理）")
    else:
        print(f"📋 [mp_indicator_calc] daily_status: "
              f"is_week_complete={is_week_complete} "
              f"last_w={daily_status.get('last_complete_w_date', 'N/A')}")

    # 讀或重置進度
    progress = pdb.get()
    all_tickers = progress.get("all_tickers", [])

    if not all_tickers:
        # 首次：掃描 HF 目錄
        print(f"📂 [mp_indicator_calc] 首次運行，掃描 HF ticker 目錄...")
        all_tickers = _hf_scan_ticker_dirs()
        if not all_tickers:
            print("❌ [mp_indicator_calc] 掃描結果為空，退出")
            pdb.close()
            sys.exit(1)
        pdb.save_all_tickers(all_tickers)
        print(f"✅ [mp_indicator_calc] 掃描完成，共 {len(all_tickers)} 個 ticker")

    # 重置（新的一天）或斷點續跑
    progress = pdb.reset_if_new_day(all_tickers)
    completed = set(progress.get("completed_tickers", []))
    total     = len(all_tickers)

    print(f"📊 [mp_indicator_calc] 總計 {total} 個 ticker，已完成 {len(completed)}")

    # 批次處理
    remaining = [t for t in all_tickers if t not in completed]
    print(f"▶️  [mp_indicator_calc] 剩餘 {len(remaining)} 個 ticker 待處理")

    batch_num  = 0
    ok_count   = 0
    skip_count = 0
    err_count  = 0

    for i in range(0, len(remaining), BATCH_SIZE):
        batch      = remaining[i: i + BATCH_SIZE]
        batch_num += 1
        hf_files   = []
        batch_ok   = []
        batch_skip = []
        batch_err  = []

        print(f"\n🔄 [mp_indicator_calc] 批次 {batch_num} | "
              f"ticker {i+1}~{min(i+BATCH_SIZE, len(remaining))}/{len(remaining)}")

        for ticker in batch:
            try:
                result = _process_ticker(ticker, today_str, is_week_complete)
                if result is not None:
                    hf_files.append(result)
                    batch_ok.append(ticker)
                else:
                    batch_skip.append(ticker)
            except Exception as e:
                print(f"  ❌ {ticker}: 異常: {e}")
                batch_err.append(ticker)

        # HF commit
        if hf_files:
            commit_msg = (
                f"mp_indicator_calc {today_str} "
                f"batch {batch_num} ({len(hf_files)} tickers)"
            )
            print(f"💾 [mp_indicator_calc] commit {len(hf_files)} 個文件...")
            commit_ok = _hf_batch_commit(hf_files, commit_msg)
            if commit_ok:
                print(f"✅ [mp_indicator_calc] commit 成功")
                completed.update(batch_ok)
                ok_count   += len(batch_ok)
                skip_count += len(batch_skip)
                err_count  += len(batch_err)
                pdb.update_completed(completed)
                time.sleep(SLEEP_AFTER_COMMIT)
            else:
                print(f"❌ [mp_indicator_calc] commit 失敗，本批進度不推進")
                # 不更新 completed，下次重試
        else:
            # 全部跳過，進度也推進（跳過的 ticker 不需要重試）
            completed.update(batch_ok + batch_skip)
            skip_count += len(batch_skip)
            err_count  += len(batch_err)
            pdb.update_completed(completed)

        elapsed = time.monotonic() - start_time
        print(f"📊 [mp_indicator_calc] 進度: {len(completed)}/{total} | "
              f"本批 OK:{len(batch_ok)} 跳過:{len(batch_skip)} ERR:{len(batch_err)} | "
              f"耗時 {elapsed:.0f}s")

    # 完成
    pdb.mark_done()
    pdb.close()

    elapsed_total = time.monotonic() - start_time
    print(f"\n✅ [mp_indicator_calc] 全部完成 | "
          f"總計 {total} 個 ticker | "
          f"OK:{ok_count} 跳過:{skip_count} ERR:{err_count} | "
          f"耗時 {elapsed_total:.0f}s ({elapsed_total/60:.1f}min)")


if __name__ == "__main__":
    main()
