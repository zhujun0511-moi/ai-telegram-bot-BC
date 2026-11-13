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

SSGA_HOLDINGS = "https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/holdings-daily-us-en-%s.xlsx"
TV_SCAN = "https://scanner.tradingview.com/america/scan"

_last = {"wiki": 0.0, "sec": 0.0}
_GAP = {"wiki": 1.0, "sec": 0.25, "ssga": 1.0, "tv": 3.0}


def has_sec_contact() -> bool:
    return bool(os.environ.get("SEC_USER_AGENT", "").strip())


def _ua(kind):
    if kind == "sec":
        ua = os.environ.get("SEC_USER_AGENT", "").strip()
        if not ua:
            raise RuntimeError("缺 SEC_USER_AGENT（SEC 要求聯絡人標頭）")
        return ua
    if kind in ("ssga", "tv"):
        return "Mozilla/5.0 (compatible; OpenClaw-list-review/0.1; read-only)"
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


SEC_TICKET_FORMS = ("25", "25-NSE", "15-12B", "15-12G", "15-15D", "15F-12B", "8-K", "S-4", "425", "DEFM14A",
                    "SC TO-T", "SC 14D9", "10-12B", "8-A12B", "8-A12G")


def sec_recent_filings(cik, since):
    """2026-10-28 工單預審：SEC 最近申報（since 以後、只留與下市／併購／分拆／轉板相關的表格）。
    回 [{form, date, items}]（新到舊）；查無＝[]。items＝8-K 事項代碼（例 2.01 完成併購、3.01 下市通知）。"""
    r = _get("sec", SEC_SUBMISSIONS % int(cik))
    if r is None:
        return []
    rec = ((r.json().get("filings") or {}).get("recent")) or {}
    forms, dates, items = rec.get("form") or [], rec.get("filingDate") or [], rec.get("items") or []
    out = []
    for k, f in enumerate(forms):
        d = dates[k] if k < len(dates) else ""
        if d >= since and f in SEC_TICKET_FORMS:
            out.append({"form": f, "date": d, "items": items[k] if k < len(items) else ""})
    return out


# ══ 2026-11-13 清單審查 tools/list_review.py 用（Jun 11-13 同意；登記 MongoDB_Standard_v3 六-D 2026-11-13）══

def _xlsx_rows(content):
    """最小 xlsx 讀取（只用標準庫，免裝 openpyxl）：第一張工作表 → [{欄字母: 值}]。"""
    import io
    import re
    import zipfile
    import xml.etree.ElementTree as ET
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    z = zipfile.ZipFile(io.BytesIO(content))
    shared = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).iter(ns + "si"):
            shared.append("".join(t.text or "" for t in si.iter(ns + "t")))
    sheets = sorted(n for n in z.namelist() if n.startswith("xl/worksheets/sheet"))
    out = []
    for row in ET.fromstring(z.read(sheets[0])).iter(ns + "row"):
        cells = {}
        for c in row.iter(ns + "c"):
            col = re.match(r"[A-Z]+", c.get("r") or "")
            if not col:
                continue
            typ, v = c.get("t"), c.find(ns + "v")
            if typ == "inlineStr":
                val = "".join(t.text or "" for t in c.iter(ns + "t"))
            elif v is None:
                val = None
            elif typ == "s":
                val = shared[int(v.text)]
            else:
                val = v.text
            cells[col.group(0)] = val
        out.append(cells)
    return out


def ssga_xl_holdings(xl):
    """SSGA 公開的 XL 當天持股（xl 例 "XLB"）→ (as_of 字串, [{ticker, name, weight}])；查無＝None。
    只取有代號、有權重的列（現金、期貨等沒有代號的列略過）。代號大寫、保留點（BRK.B）。"""
    if not (isinstance(xl, str) and len(xl) <= 5 and xl.upper().startswith("XL")):
        raise ValueError("不認得的 XL：%r" % (xl,))
    r = _get("ssga", SSGA_HOLDINGS % xl.lower())
    if r is None:
        return None
    rows = _xlsx_rows(r.content)
    as_of, head, out = None, None, []
    for cells in rows:
        a, b = (cells.get("A") or "").strip(), (cells.get("B") or "").strip()
        if a.lower().startswith("holdings"):
            as_of = b.replace("As of", "").strip()
        if head is None:
            if b.lower() == "ticker":
                head = {v.strip().lower(): k for k, v in cells.items() if isinstance(v, str)}
            continue
        tk = (cells.get(head.get("ticker", "B")) or "").strip().upper()
        w = cells.get(head.get("weight", "E"))
        if not tk or tk in ("-", "CASH") or w in (None, "", "-"):
            continue
        try:
            w = float(w)
        except ValueError:
            continue
        out.append({"ticker": tk, "name": (cells.get(head.get("name", "A")) or "").strip(), "weight": w})
    return as_of, out


TV_COLUMNS = ("name", "exchange", "type", "sector", "industry", "country", "description")


def tradingview_lookup(tickers, chunk=300):
    """TradingView 篩選器（⚠️ 非官方介面；Jun 11-13：至多季度審查才用）→ {代號: {exchange,type,sector,industry,country,description}}。
    代號照 TradingView 寫法（點，例 BRK.B）；同代號多個交易所時優先 NYSE／NASDAQ／AMEX。查不到的不在回傳裡。"""
    import requests
    rank = {"NYSE": 0, "NASDAQ": 1, "AMEX": 2, "CBOE": 3}
    best = {}
    tickers = sorted({t.strip().upper() for t in tickers if t and t.strip()})
    for k in range(0, len(tickers), chunk):
        part = tickers[k:k + chunk]
        body = {"filter": [{"left": "name", "operation": "in_range", "right": part}],
                "columns": list(TV_COLUMNS), "range": [0, len(part) * 3]}
        last = None
        for attempt in range(4):
            gap = _GAP["tv"] - (time.time() - _last.get("tv", 0.0))
            if gap > 0:
                time.sleep(gap)
            _last["tv"] = time.time()
            try:
                resp = requests.post(TV_SCAN, json=body, timeout=60, headers={"User-Agent": _ua("tv")})
                if resp.status_code in (429, 500, 502, 503, 504):
                    last = "HTTP %d" % resp.status_code
                    time.sleep(10 * (attempt + 1))
                    continue
                resp.raise_for_status()
                data = resp.json().get("data") or []
                break
            except (requests.RequestException, ValueError) as e:
                last = type(e).__name__
                time.sleep(10 * (attempt + 1))
        else:
            raise RuntimeError("tv 讀取失敗：%s" % last)
        for row in data:
            d = dict(zip(TV_COLUMNS, row.get("d") or []))
            tk = (d.get("name") or "").upper()
            if tk not in part:
                continue
            ex = d.get("exchange") or ""
            old = best.get(tk)
            if old is None or rank.get(ex, 9) < rank.get(old.get("exchange"), 9):
                best[tk] = {c: d.get(c) for c in TV_COLUMNS if c != "name"}
    return best
