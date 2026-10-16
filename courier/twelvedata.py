# -*- coding: utf-8 -*-
"""
courier/twelvedata.py — BC 的 TwelveData（2026-10-16「唯一的門」）

來源（逐字搬來，行為不變；金鑰在門內讀，不再由呼叫端傳入）：
  td_ready_gate.td_snapshot → gate_snapshot（定稿閘門抽樣）
  raw_writer.TD 類別 → RawTD（1day adjust=none 補史；節流 8.5 秒、429 處理不變）
Python 3.9 相容。
"""
import os
import time

import requests

TD_URL = "https://api.twelvedata.com/time_series"
UA = {"User-Agent": "python-requests/2.32.3"}


def gate_snapshot(samples, log):
    """一次批量查抽樣最新日線 → {sym: {date, close, volume}}；失敗回 None。（原 td_ready_gate.td_snapshot）"""
    key = os.environ.get("TWELVE_KEY", "")
    try:
        r = requests.get(TD_URL,
                         params={"symbol": ",".join(samples), "interval": "1day", "outputsize": "1"},
                         headers={**UA, "Authorization": "apikey " + key}, timeout=40)
        j = r.json()
    except Exception as e:
        log(f"⚠️ TD 連線異常 {type(e).__name__}")
        return None
    if j.get("code") == 429 or j.get("status") == "error":
        log(f"⚠️ TD {j.get('code')} {str(j.get('message', ''))[:80]}")
        return None
    out = {}
    for s in samples:
        v = ((j.get(s) or {}).get("values") or [None])[0]
        if v:
            try:
                out[s] = {"date": v["datetime"][:10], "close": v["close"], "volume": float(v.get("volume") or 0)}
            except (KeyError, ValueError, TypeError):
                pass
    return out


class RawTD:
    """原 raw_writer.TD（逐字；金鑰改在門內讀 TWELVE_KEY，log 由呼叫端傳入以保持原 log 格式）。"""
    TD_PAUSE = 8.5      # TD 免費 8 credits/分鐘

    def __init__(self, log):
        self.key, self.last, self.day_exhausted = os.environ.get("TWELVE_KEY", ""), 0.0, False
        self._log = log

    def daily_none(self, ticker, start_date=None, outputsize=5000):
        """TD 1day adjust=none。回 values 或 None（失敗/額度用盡）。"""
        _log = self._log
        if self.day_exhausted or not self.key:
            return None
        params = {"symbol": ticker, "interval": "1day", "adjust": "none", "outputsize": str(outputsize)}
        if start_date:
            params["start_date"] = start_date
        for attempt in range(5):
            wait = self.TD_PAUSE - (time.time() - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()
            try:
                r = requests.get(TD_URL, params=params, headers={**UA, "Authorization": "apikey " + self.key}, timeout=40)
                j = r.json()
            except Exception as e:
                _log(f"  ⚠️ [TD] {ticker} 連線異常 {type(e).__name__}")
                time.sleep(10)
                continue
            if j.get("status") == "ok":
                return j.get("values") or []
            msg = str(j.get("message", ""))[:120]
            if j.get("code") == 429 and "day" in msg.lower():
                self.day_exhausted = True
                _log("  ⛔ [TD] 今日額度用盡，停止本輪補史")
                return None
            if j.get("code") == 429:
                _log(f"  ⏳ [TD] {ticker} 每分鐘額度撞車（{msg[:60]}），65 秒後重試")
                time.sleep(65)
                continue
            _log(f"  ⚠️ [TD] {ticker} {j.get('code')} {msg}")
            return None
        return None
