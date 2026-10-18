# -*- coding: utf-8 -*-
"""
courier/public_data.py — BC 讀公開資料的門（2026-10-18；歷史板塊歸屬 tools/sector_history.py 用）

只讀、不寫外部；全部是公開資料：
  - 維基百科「List of S&P 500 companies」某時點以前的最後一個版本（含當時的 GICS 板塊）
  - GitHub hanshof/sp500_constituents（MIT）：S&P 500 歷史成分（每個日期一列）
  - SEC EDGAR：代號 → CIK 對照表、每家公司的 submissions（SIC、曾用名）
SEC 要求請求帶聯絡人標頭：讀 secret `SEC_USER_AGENT`（Jun 10-18 同意；只在門內讀、不印出）。
限速：維基每秒約 1 次、SEC 每秒最多 4 次（SEC 上限 10）；失敗重試。Python 3.9 相容。
"""
import os
import time

import requests

WIKI_API = "https://en.wikipedia.org/w/api.php"
WIKI_TITLE = "List of S&P 500 companies"
HANSHOF_RAW = "https://raw.githubusercontent.com/hanshof/sp500_constituents/main/"
SEC_TICKERS = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS = "https://data.sec.gov/submissions/CIK%010d.json"

_last = {"wiki": 0.0, "sec": 0.0}
_GAP = {"wiki": 1.0, "sec": 0.25}


def has_sec_contact() -> bool:
    return bool(os.environ.get("SEC_USER_AGENT", "").strip())


def _ua(kind):
    if kind == "sec":
        ua = os.environ.get("SEC_USER_AGENT", "").strip()
        if not ua:
            raise RuntimeError("缺 SEC_USER_AGENT（SEC 要求聯絡人標頭）")
        return ua
    return "OpenClaw-research/0.1 (one-off historical sector mapping; read-only)"


def _get(kind, url, params=None, tries=5):
    last = None
    for k in range(tries):
        gap = _GAP.get(kind, 0.5) - (time.time() - _last.get(kind, 0.0))
        if gap > 0:
            time.sleep(gap)
        _last[kind] = time.time()
        try:
            r = requests.get(url, params=params, timeout=60,
                             headers={"User-Agent": _ua(kind), "Accept-Encoding": "gzip, deflate"})
            if r.status_code == 404:
                return None
            if r.status_code in (429, 500, 502, 503, 504):
                last = "HTTP %d" % r.status_code
                time.sleep(5 * (k + 1))
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last = type(e).__name__
            time.sleep(5 * (k + 1))
    raise RuntimeError("%s 讀取失敗 %d 次：%s" % (kind, tries, last))


def wiki_sp500_revision(before_iso):
    """回 (版本時間, wikitext)：before_iso（例 2010-01-01T00:00:00Z）以前的最後一個版本；沒有＝None。"""
    r = _get("wiki", WIKI_API, params={
        "action": "query", "prop": "revisions", "titles": WIKI_TITLE, "rvstart": before_iso, "rvdir": "older",
        "rvlimit": 1, "rvprop": "timestamp|content", "rvslots": "main", "format": "json", "formatversion": 2})
    if r is None:
        return None
    revs = r.json()["query"]["pages"][0].get("revisions") or []
    if not revs:
        return None
    return revs[0]["timestamp"], revs[0]["slots"]["main"]["content"]


def hanshof_csv(name):
    """GitHub hanshof/sp500_constituents 的 CSV 檔（只准兩個已知檔名）。回文字。"""
    if name not in ("sp_500_historical_components.csv", "sp500_constituents.csv"):
        raise ValueError("不認得的檔名：%s" % name)
    r = _get("gh", HANSHOF_RAW + name)
    return r.text if r is not None else None


def sec_company_tickers():
    """SEC 代號對照表 → {代號: cik}（代號大寫，. 換成 -）。"""
    r = _get("sec", SEC_TICKERS)
    return {v["ticker"].upper().replace(".", "-"): int(v["cik_str"]) for v in r.json().values()}


def sec_profile(cik):
    """SEC submissions 精簡：{cik, name, sic, sic_desc, former_names, tickers}；查無＝None。"""
    r = _get("sec", SEC_SUBMISSIONS % int(cik))
    if r is None:
        return None
    j = r.json()
    return {"cik": int(cik), "name": j.get("name"), "sic": j.get("sic") or None,
            "sic_desc": j.get("sicDescription") or None,
            "former_names": [{"name": x.get("name"), "from": (x.get("from") or "")[:10], "to": (x.get("to") or "")[:10]}
                             for x in (j.get("formerNames") or [])],
            "tickers": j.get("tickers") or []}
