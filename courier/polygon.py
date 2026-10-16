# -*- coding: utf-8 -*-
"""
courier/polygon.py — BC 的 Polygon（2026-10-16「唯一的門」）

來源（逐字搬來，行為不變；金鑰在門內讀 POLYGON_KEY，log 由呼叫端傳入以保持原 log 格式）：
  raw_writer.Polygon → RawPolygon（_get 節流 13 秒、429 等 65 秒、all_pages 跟 next_url）；
      daily_raw 的「原始回應 → 列」轉換留在 raw_writer（用它自己的 bar_row），門只回原始 JSON
  run_verify_weekend：_fetch_polygon、_fetch_polygon_reference、_polygon_ref_get、
      _fetch_polygon_active_map、_fetch_polygon_inactive（常數 13.5／65／2 與原檔一致）
Polygon 改名 Massive（api.polygon.io 將淘汰）時只改本檔。Python 3.9 相容。
"""
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import pytz
import requests

from courier.keys import scrub

PG_BASE = "https://api.polygon.io"
UA = {"User-Agent": "python-requests/2.32.3"}


def _key():
    return os.environ.get("POLYGON_KEY", "")


# ══ raw_writer 用 ══════════════════════════════════════════════════

class RawPolygon:
    PG_PAUSE = 13.0     # Polygon 免費 5 次/分鐘

    def __init__(self, log):
        self.key, self.last = _key(), 0.0
        self._log = log

    def _get(self, url, params=None):
        _log = self._log
        for attempt in range(4):
            wait = self.PG_PAUSE - (time.time() - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()
            try:
                r = requests.get(url, params=params, headers={**UA, "Authorization": "Bearer " + self.key}, timeout=40)
            except Exception as e:
                _log(f"  ⚠️ [Polygon] 連線異常 {type(e).__name__}")
                time.sleep(15)
                continue
            if r.status_code == 429:
                time.sleep(65)
                continue
            if r.status_code != 200:
                _log(f"  ⚠️ [Polygon] HTTP {r.status_code}")
                return None
            return r.json()
        return None

    def daily_raw_json(self, ticker, frm, to):
        """原 daily_raw 的請求部分：Polygon 未調整日線原始 JSON（轉成列由 raw_writer 做）。"""
        return self._get(f"{PG_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{frm}/{to}",
                         {"adjusted": "false", "sort": "asc", "limit": 5000})

    def all_pages(self, path, params):
        out, url, p = [], PG_BASE + path, dict(params)
        while url:
            j = self._get(url, p)
            if j is None:
                return None
            out += j.get("results") or []
            url, p = j.get("next_url"), None
        return out


# ══ run_verify_weekend 用（語義克隆自 DC _fetch_polygon v12.32：[]=確認空 / None=狀態未知）══

EST_TZ = pytz.timezone("US/Eastern")
POLYGON_DELAY     = 13.5
POLYGON_WAIT      = 65
POLYGON_MAX_RETRY = 2


def verify_fetch_polygon(ticker: str, period_label: str, start_date: str, end_date: str, _log=print):
    POLYGON_KEY = _key()
    period_map = {"D": (1, "day"), "W": (1, "week")}
    mult, p_api = period_map[period_label]
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{mult}/{p_api}/"
        f"{start_date}/{end_date}"
        f"?adjusted=true&extended_hours=false&sort=desc&limit=50000&apiKey={POLYGON_KEY}"
    )
    for attempt in range(POLYGON_MAX_RETRY):
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code in (429, 403):
                _log(f"  ⚠️ [{ticker}/{period_label}] 限速/封鎖 "
                     f"({resp.status_code})，等待 {POLYGON_WAIT}s 重試")
                time.sleep(POLYGON_WAIT)
                continue
            if resp.status_code != 200:
                _log(f"  ❌ Polygon HTTP {resp.status_code} "
                     f"({ticker}/{period_label})，狀態未知")
                return None
            results = resp.json().get("results", [])
            if not results:
                return []
            processed = []
            for r in results:
                dt_obj = datetime.fromtimestamp(r["t"] / 1000, EST_TZ)
                # W period：Polygon 返回週日戳，統一修正為當週週一（W-MON 錨定）
                # v12.35 修復：週日 weekday()==6，減6天會退到上一週的週一，不是這週。
                # 正確做法：週日 +1 天 = 這週的週一。
                if period_label == "W":
                    dt_obj = dt_obj + timedelta(days=1)
                processed.append({
                    "t": dt_obj.strftime("%Y-%m-%d %H:%M:%S"),
                    "o": r["o"], "h": r["h"],
                    "l": r["l"], "c": r["c"], "v": r["v"],
                })
            return processed
        except Exception as e:
            # ⚠️ scrub：requests 連線例外的 str(e) 會帶出含 apiKey 的 Polygon URL（見 courier/keys / DANGER_ZONES §5）
            _log(f"  ❌ Polygon 連線異常 ({ticker}/{period_label}): {scrub(e)}")
            if attempt == POLYGON_MAX_RETRY - 1:
                return None
            time.sleep(POLYGON_WAIT)
    return None


def verify_fetch_reference(ticker: str, _log=print) -> Optional[dict]:
    """
    查詢 Polygon Reference API 取得 ticker 上市日期等身份資訊（v1.1 新增）。
    回傳 None → 狀態未知（限速/連線異常），下次再試
    回傳 {}   → Polygon 確認查無此 ticker 的身份資料
    回傳 dict → 正常取得 {list_date, cik, company_name}
    """
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={_key()}"
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code in (429, 403):
            _log(f"  ⚠️ [{ticker}] Reference API 限速/封鎖 ({resp.status_code})")
            return None
        if resp.status_code != 200:
            _log(f"  ❌ [{ticker}] Reference API HTTP {resp.status_code}")
            return None
        data = resp.json().get("results")
        if not data:
            return {}
        return {
            "list_date":    data.get("list_date"),
            "cik":          data.get("cik"),
            "company_name": data.get("name"),
        }
    except Exception as e:
        _log(f"  ❌ [{ticker}] Reference API 連線異常: {scrub(e)}")
        return None


def _polygon_ref_get(url: str, params: dict, _log=print) -> Optional[dict]:
    """Polygon Reference GET，429 退避重試。失敗回 None。（2026-10-01 E）"""
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                _log(f"  ⚠️ Reference API 限速 429，等 {POLYGON_WAIT}s 重試")
                time.sleep(POLYGON_WAIT)
                continue
            if resp.status_code != 200:
                _log(f"  ❌ Reference API HTTP {resp.status_code}")
                return None
            return resp.json()
        except Exception as e:
            _log(f"  ❌ Reference API 連線異常: {scrub(e)}")
            time.sleep(POLYGON_DELAY)
    return None


def verify_fetch_active_map(_log=print) -> Optional[dict]:
    """（2026-10-01 E）分頁拉 Polygon 全部「現役」股票/ETF 代號 → {TICKER: {name, cik}}。失敗回 None。"""
    POLYGON_KEY = _key()
    url = "https://api.polygon.io/v3/reference/tickers"
    params = {"market": "stocks", "active": "true", "limit": 1000, "apiKey": POLYGON_KEY}
    out, pages = {}, 0
    while url and pages < 40:
        data = _polygon_ref_get(url, params, _log)
        if data is None:
            return None
        for r in data.get("results") or []:
            tk = (r.get("ticker") or "").upper()
            if tk:
                out[tk] = {"name": r.get("name"), "cik": r.get("cik")}
        url = data.get("next_url")
        params = {"apiKey": POLYGON_KEY}   # next_url 已帶其餘查詢參數
        pages += 1
        time.sleep(POLYGON_DELAY)
    return out


def verify_fetch_inactive(ticker: str, _log=print) -> Optional[dict]:
    """（2026-10-01 E）查某代號的「已停止交易」紀錄。None＝狀態未知；{}＝查無；dict＝最近一筆。"""
    data = _polygon_ref_get("https://api.polygon.io/v3/reference/tickers",
                            {"ticker": ticker, "active": "false", "limit": 10, "apiKey": _key()}, _log)
    if data is None:
        return None
    rows = [r for r in (data.get("results") or []) if r.get("delisted_utc")]
    if not rows:
        return {}
    r = max(rows, key=lambda x: x.get("delisted_utc") or "")
    return {"name": r.get("name"), "cik": r.get("cik"), "delisted_utc": r.get("delisted_utc")}
