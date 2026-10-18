# -*- coding: utf-8 -*-
"""
sector_history.py — 歷史板塊歸屬（2026-10-18 Jun：「按時間取得集合總成，我們自己再拆分，有問題的回去詳查」）

用途：擺動點考試的「SPY／XL 板塊／個股互動」前置——每支股票在每個時間點屬於哪個 XL 板塊（不偷看未來）。
登記：MongoDB_Standard_v3 §六-D `swing_ai/sector_history/{日期}/`；BC secret SEC_USER_AGENT。
來源（全部公開、只讀，經門 courier/public_data）：
  1. 維基百科 S&P 500 每月的歷史版本 → 當月成分＋當時 GICS 板塊（2018 通訊服務改組自動反映）
  2. GitHub hanshof/sp500_constituents（MIT）→ 歷史成分，交叉核對
  3. SEC EDGAR → 全市場每家公司的 SIC 行業代碼、CIK、曾用名
做法：
  - S&P 成分股：當月照 GICS → XL。
  - 其他股票：用 S&P 成分的（GICS, SIC）配對學出「SIC → XL」對照表（留一驗證量準確率），學不到的才退回手寫規則。
  - 固定規則：電信類在 2018-09 通訊服務成立前歸 XLK（📝 未經官方核實，信心標 rule_unverified）；
    媒體類 2018-09 前歸 XLY；地產 2016-09 前歸 XLF（GICS 地產板塊 2016-09 才成立）。
  - ETF／查不到的／對照信心低的 → problems.csv，之後回頭詳查。
日誌只印筆數。用法：
  python sector_history.py [--dry-run] [--months 2010-01,2018-01] [--limit-tickers 50] [--out _sector_out]
Python 3.9 相容。
"""
import argparse
import csv
import datetime as dt
import io
import json
import os
import re
from collections import Counter, defaultdict

from courier import public_data as PD
from courier.dataset import RawHF

REPO = "zhujun0511-AI/ai-telegram-bot-dataset"
OUT_PREFIX = "swing_ai/sector_history"
GICS2XL = {"Information Technology": "XLK", "Health Care": "XLV", "Financials": "XLF",
           "Consumer Discretionary": "XLY", "Consumer Staples": "XLP", "Energy": "XLE", "Industrials": "XLI",
           "Materials": "XLB", "Utilities": "XLU", "Real Estate": "XLRE", "Communication Services": "XLC",
           "Telecommunication Services": "XLK", "Telecommunications Services": "XLK"}   # 電信→XLK：📝 未核實
SECTORS = sorted(GICS2XL, key=len, reverse=True)       # 長名先比（Consumer Staples 不會被 Consumer 誤配）
SYM_RE = re.compile(r"\{\{\s*(?:nyse|nasdaq|bats|cboe|amex)\w*\s*\|\s*([A-Za-z.\-]+)", re.I)
SYM_RE2 = re.compile(r"(?:ticker|symbol|sym)=([A-Za-z.\-]+)", re.I)
COMM_START, RE_START = "2018-09", "2016-09"


def norm(t):
    return t.strip().upper().replace(".", "-")


# ── 1. 維基百科：某月成分＋GICS ──
def parse_wiki(text):
    c = text
    end = c.find("{|", c.find("{|") + 2)              # 只取第一張表（現任成分）；第二張是異動
    tbl = c[:end] if end > 0 else c
    out = {}
    for row in tbl.split("|-")[1:]:
        m = SYM_RE.search(row) or SYM_RE2.search(row)
        if not m:
            continue
        sec = next((s for s in SECTORS if s in row), None)
        out[norm(m.group(1))] = sec
    return out


def months_between(a, b):
    y, m = int(a[:4]), int(a[5:7])
    out = []
    while "%04d-%02d" % (y, m) <= b:
        out.append("%04d-%02d" % (y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def xl_for(gics, month):
    if gics is None:
        return None
    if gics.startswith("Telecommunication") and month >= COMM_START:
        return "XLC"
    return GICS2XL.get(gics)


# ── 2. SIC 對照：手寫規則（只當退路）＋從 S&P 學 ──
HAND = [(100, 999, "XLP"), (1000, 1099, "XLB"), (1200, 1399, "XLE"), (1400, 1499, "XLB"), (1520, 1531, "XLY"),
        (1540, 1799, "XLI"), (2000, 2199, "XLP"), (2200, 2399, "XLY"), (2400, 2499, "XLB"), (2500, 2599, "XLY"),
        (2600, 2699, "XLB"), (2700, 2799, "XLC"), (2833, 2836, "XLV"), (2840, 2844, "XLP"), (2800, 2899, "XLB"),
        (2900, 2999, "XLE"), (3000, 3099, "XLB"), (3100, 3199, "XLY"), (3200, 3399, "XLB"), (3400, 3499, "XLI"),
        (3570, 3579, "XLK"), (3500, 3599, "XLI"), (3630, 3639, "XLY"), (3600, 3699, "XLK"), (3711, 3716, "XLY"),
        (3720, 3729, "XLI"), (3700, 3799, "XLI"), (3812, 3812, "XLI"), (3841, 3851, "XLV"), (3800, 3899, "XLK"),
        (3900, 3999, "XLY"), (4000, 4799, "XLI"), (4800, 4899, "XLC"), (4955, 4955, "XLI"), (4900, 4999, "XLU"),
        (5122, 5122, "XLV"), (5140, 5149, "XLP"), (5000, 5199, "XLI"), (5411, 5411, "XLP"), (5912, 5912, "XLP"),
        (5200, 5999, "XLY"), (6500, 6599, "XLRE"), (6798, 6798, "XLRE"), (6000, 6799, "XLF"), (7000, 7099, "XLY"),
        (7200, 7299, "XLY"), (7370, 7379, "XLK"), (7300, 7399, "XLI"), (7500, 7599, "XLY"), (7800, 7899, "XLC"),
        (7900, 7999, "XLY"), (8000, 8099, "XLV"), (8200, 8299, "XLY"), (8700, 8799, "XLI")]


def hand_rule(sic):
    if not sic:
        return None
    s = int(sic)
    for lo, hi, xl in HAND:
        if lo <= s <= hi:
            return xl
    return None


def learn_sic_map(pairs):
    """pairs＝[(sic4, xl)]（每家公司一筆，用它最後一次在 S&P 時的 GICS）。
    回 {sic4: {xl, n, share, loo_hits, level}}＋整體留一準確率；4 碼樣本不足時用 3 碼、2 碼多數。"""
    by = {4: defaultdict(Counter), 3: defaultdict(Counter), 2: defaultdict(Counter)}
    for sic, xl in pairs:
        for k in (4, 3, 2):
            by[k][sic[:k]][xl] += 1

    def predict(sic, drop=None):
        for k in (4, 3, 2):
            c = by[k].get(sic[:k])
            if not c:
                continue
            c = Counter(c)
            if drop:
                c[drop] -= 1
                c = +c
            if sum(c.values()) >= 3 or (k == 2 and c):
                xl, n = c.most_common(1)[0]
                return xl, n / sum(c.values()), sum(c.values()), k
        return None, 0.0, 0, None

    hits = sum(1 for sic, xl in pairs if predict(sic, drop=xl)[0] == xl)
    return predict, (hits / len(pairs) if pairs else None), len(pairs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="不寫 dataset，結果寫本機 --out")
    ap.add_argument("--months", default="", help="只做這些月份（逗號分隔，試跑用）")
    ap.add_argument("--limit-tickers", type=int, default=0, help="只查前 N 支（試跑用）")
    ap.add_argument("--out", default="_sector_out")
    ap.add_argument("--step", default="build", choices=("build", "polygon", "llm", "apply"),
                    help="build＝建歸屬表；polygon／llm／apply＝問題股詳查三步（2026-10-18）")
    ap.add_argument("--date", default="", help="詳查哪一版歸屬表（swing_ai/sector_history/{日期}；留空＝最新）")
    ap.add_argument("--max-batches", type=int, default=0, help="llm 步驟最多呼叫幾次（試跑用）")
    a = ap.parse_args()
    if a.step != "build":
        return review_main(a)
    if not PD.has_sec_contact():
        raise SystemExit("⛔ 缺 SEC_USER_AGENT secret")
    today = dt.date.today()
    months = [x.strip() for x in a.months.split(",") if x.strip()] or months_between("2000-01", today.strftime("%Y-%m"))
    hf = RawHF(write=not a.dry_run, repo=REPO, written_by="sector_history")

    # 我們的股票清單（dataset 有日線的、名稱不帶 _）
    files = hf.list_files()
    uni = sorted({p.split("/")[2] for p in files if p.startswith("mp_data/ticker/") and p.endswith("/d.csv")})
    uni = [t for t in uni if "_" not in t]
    if a.limit_tickers:
        uni = uni[:a.limit_tickers]
    print("股票清單 %d 支｜月份 %d 個（%s～%s）" % (len(uni), len(months), months[0], months[-1]), flush=True)

    # ① 每月 S&P 成分＋GICS
    hist = list(csv.DictReader(io.StringIO(PD.hanshof_csv("sp_500_historical_components.csv"))))
    sp_rows, month_stat, last_gics = [], [], {}
    rev_cache = {}
    for mth in months:
        got = PD.wiki_sp500_revision(mth + "-01T00:00:00Z")
        w = {}
        if got:
            ts, text = got
            w = rev_cache.get(ts) or parse_wiki(text)
            rev_cache[ts] = w
        h = None
        for r in hist:
            if r["date"] < mth + "-01":
                h = r
        hs = set(norm(x) for x in h["tickers"].split(",")) if h else set()
        for t, g in sorted(w.items()):
            xl = xl_for(g, mth)
            sp_rows.append({"month": mth, "ticker": t, "gics_sector": g or "", "xl": xl or "",
                            "in_hanshof": int(t in hs), "source": "wiki"})
            if xl:
                last_gics[t] = (mth, xl)
        month_stat.append({"month": mth, "wiki": len(w), "wiki_no_sector": sum(1 for g in w.values() if not g),
                           "hanshof": len(hs), "both": len(set(w) & hs), "wiki_rev": got[0] if got else None})
    low = [m for m in month_stat if m["wiki"] < 450]
    print("S&P 每月：%d 列｜成分少於 450 支的月份 %d 個（早年版本格式或缺）" % (len(sp_rows), len(low)), flush=True)

    # ② SEC：代號 → CIK → SIC
    t2c = PD.sec_company_tickers()
    profiles, kind, cache = {}, {}, {}
    need = set(norm(t) for t in uni) | set(last_gics)
    for t in sorted(need):
        cik = t2c.get(t)
        k = "stock"
        if cik is None and "-" in t and t2c.get(t.split("-")[0]):
            cik, k = t2c[t.split("-")[0]], "share_class_or_preferred"   # 例 NEE.S → NEE（對回母公司）
        if cik is None:
            kind[t] = "unknown"
            continue
        if cik not in cache:
            cache[cik] = PD.sec_profile(cik)
        p = cache[cik]
        kind[t] = k if p else "unknown"
        if p:
            profiles[t] = p
    print("SEC：查到 %d／%d 支（%d 家公司）｜查無 %d" % (len(profiles), len(need), len(cache),
                                                 sum(1 for v in kind.values() if v == "unknown")), flush=True)

    # ③ 學 SIC → XL
    pairs = [(str(profiles[t]["sic"]).zfill(4), xl) for t, (_, xl) in last_gics.items()
             if t in profiles and profiles[t].get("sic")]
    predict, loo, n_pairs = learn_sic_map(pairs)
    sics = sorted({str(p["sic"]).zfill(4) for p in profiles.values() if p.get("sic")})
    sic_map = {}
    for s in sics:
        xl, share, n, level = predict(s)
        if xl is None:
            hx = hand_rule(s)
            sic_map[s] = {"xl": hx, "n": 0, "share": None, "level": "hand_rule" if hx else None,
                          "confidence": "low" if hx else "none"}
        else:
            conf = "high" if (level == 4 and n >= 5 and share >= 0.8) else ("medium" if share >= 0.6 else "low")
            sic_map[s] = {"xl": xl, "n": n, "share": round(share, 3), "level": "sic%d" % level, "confidence": conf}
    print("SIC 對照：S&P 學習樣本 %d 家｜留一準確率 %s｜代碼 %d 個（高 %d／中 %d／低 %d／無 %d）" % (
        n_pairs, "%.1f%%" % (100 * loo) if loo is not None else "NA", len(sic_map),
        *(sum(1 for v in sic_map.values() if v["confidence"] == c) for c in ("high", "medium", "low", "none"))),
          flush=True)

    # ④ 每支股票每月歸屬 → 合併成段
    sp_by = defaultdict(dict)
    for r in sp_rows:
        if r["xl"]:
            sp_by[r["ticker"]][r["month"]] = r["xl"]
    seg_rows, problems = [], []
    for t0 in uni:
        t = norm(t0)
        k = kind.get(t, "unknown")
        p = profiles.get(t)
        sic = str(p["sic"]).zfill(4) if p and p.get("sic") else None
        sm = sic_map.get(sic) if sic else None
        if k == "unknown":
            problems.append({"ticker": t0, "reason": "SEC 查無（可能是 ETF／外國公司／已改名）"})
        elif not sic:
            problems.append({"ticker": t0, "reason": "SEC 無 SIC 代碼"})
        elif sm["confidence"] in ("low", "none"):
            problems.append({"ticker": t0, "reason": "SIC %s（%s）對照信心低：%s" % (sic, p.get("sic_desc"), sm["level"])})
        if k == "share_class_or_preferred":
            problems.append({"ticker": t0, "reason": "優先股或分級股：已對回母公司 %s，待確認" % (p or {}).get("name")})
        cur = None
        for mth in months:
            if mth in sp_by.get(t, {}):
                xl, src, conf = sp_by[t][mth], "gics", "high"
            elif sm and sm["xl"]:
                xl, src, conf = sm["xl"], "sic_map", sm["confidence"]
                if xl == "XLC" and mth < COMM_START:          # 通訊服務成立前：電信 → XLK、媒體 → XLY
                    xl, src, conf = ("XLK" if sic.startswith("48") else "XLY"), "rule", "rule_unverified"
                if xl == "XLRE" and mth < RE_START:
                    xl, src, conf = "XLF", "rule", "rule"
            else:
                xl, src, conf = None, None, None
            key = (xl, src, conf)
            if cur and cur["key"] == key:
                cur["end"] = mth
            else:
                if cur:
                    seg_rows.append(cur)
                cur = {"ticker": t0, "start": mth, "end": mth, "key": key}
        if cur:
            seg_rows.append(cur)
    print("歸屬：%d 支、%d 段｜問題股 %d 筆" % (len(uni), len(seg_rows), len(problems)), flush=True)

    # ⑤ 輸出
    def to_csv(rows, cols):
        b = io.StringIO()
        w = csv.DictWriter(b, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})
        return b.getvalue()

    base = "%s/%s" % (OUT_PREFIX, today.isoformat())
    hf.add(base + "/sp500_monthly.csv", to_csv(sp_rows, ["month", "ticker", "gics_sector", "xl", "in_hanshof", "source"]))
    hf.add(base + "/sec_profiles.jsonl", "".join(json.dumps(dict(v, ticker=k, kind=kind.get(k)), ensure_ascii=False) + "\n"
                                                for k, v in sorted(profiles.items())))
    hf.add(base + "/sic_map.json", json.dumps(sic_map, ensure_ascii=False, indent=1))
    hf.add(base + "/sector_history.csv", to_csv(
        [{"ticker": s["ticker"], "start": s["start"], "end": s["end"], "xl": s["key"][0] or "",
          "source": s["key"][1] or "", "confidence": s["key"][2] or ""} for s in seg_rows],
        ["ticker", "start", "end", "xl", "source", "confidence"]))
    hf.add(base + "/problems.csv", to_csv(problems, ["ticker", "reason"]))
    manifest = {"created": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "months": [months[0], months[-1]],
                "n_months": len(months), "tickers": len(uni), "sp500_rows": len(sp_rows), "month_stat": month_stat,
                "sec_found": len(profiles), "sec_companies": len(cache), "sic_learn_pairs": n_pairs,
                "sic_loo_accuracy": loo, "segments": len(seg_rows), "problems": len(problems),
                "kinds": dict(Counter(kind.get(norm(t), "unknown") for t in uni)),
                "rules": {"telecom_before_2018_09": "XLK（📝 未經官方核實）", "media_before_2018_09": "XLY",
                          "real_estate_before_2016_09": "XLF"},
                "sources": {"wiki": PD.WIKI_TITLE, "hanshof": "github hanshof/sp500_constituents (MIT)", "sec": "EDGAR"}}
    hf.add(base + "/manifest.json", json.dumps(manifest, ensure_ascii=False, indent=1))
    n = hf.flush(a.out, "sector history %s" % today.isoformat())
    print("寫出 %d 個檔（%s）：%s" % (n, "本機試跑" if a.dry_run else "私有 dataset", a.out if a.dry_run else base), flush=True)


# ══ 問題股詳查（2026-10-18 Jun：先用 Polygon 分類、剩下的兩個免費模型詳查、兩者一致才採用）══
XLS = ("XLK", "XLF", "XLV", "XLY", "XLP", "XLE", "XLI", "XLB", "XLU", "XLRE", "XLC")
FUND_TYPES = ("ETF", "ETN", "ETV", "ETS", "FUND", "SP")
DERIV_TYPES = ("PFD", "WARRANT", "RIGHT", "UNIT")
COMPANY_TYPES = ("CS", "ADRC", "ADRP", "ADRS", "OS", "GDR", "NYRS")
BATCH = 40


def _latest_version(hf):
    vs = sorted({p.split("/")[2] for p in hf.list_files() if p.startswith(OUT_PREFIX + "/") and p.count("/") >= 3
                 and p.split("/")[2][:2] == "20"})
    return vs[-1] if vs else None


def _jsonl(text):
    return [json.loads(x) for x in (text or "").splitlines() if x.strip()]


def classify_problems(problems, profiles, poly):
    """純函數：問題股 × SEC 資料 × Polygon 總表 → (kind_updates, review_queue)。
    poly＝{正規化代號: Polygon 列}。ETF 類＝不歸板塊；優先股／權證類＝對回母公司；公司類＝送模型詳查。"""
    upd, queue, seen = [], [], set()
    for r in problems:
        t = norm(r["ticker"])
        if t in seen:
            continue
        seen.add(t)
        pg = poly.get(t)
        pr = profiles.get(t) or {}
        typ = (pg or {}).get("type")
        if typ in FUND_TYPES:
            upd.append({"ticker": r["ticker"], "kind": "fund", "type": typ, "name": pg.get("name"), "xl": None,
                        "note": "Polygon 類型 %s：基金／ETF 不歸板塊" % typ})
        elif typ in DERIV_TYPES:
            upd.append({"ticker": r["ticker"], "kind": "derivative", "type": typ, "name": pg.get("name"),
                        "parent": t.split("-")[0], "note": "Polygon 類型 %s：對回母公司 %s" % (typ, t.split("-")[0])})
        elif typ in COMPANY_TYPES or (typ is None and pr.get("name")):
            queue.append({"ticker": r["ticker"], "name": (pg or {}).get("name") or pr.get("name"), "type": typ,
                          "sic": pr.get("sic"), "sic_desc": pr.get("sic_desc"), "reason": r["reason"]})
        else:
            upd.append({"ticker": r["ticker"], "kind": "unknown", "type": typ, "name": (pg or {}).get("name"),
                        "note": "Polygon 現役總表查無或類型不明（%s）：留人工" % typ})
    return upd, queue


def build_prompt(items):
    lines = ["%d. %s | %s | SIC: %s" % (i + 1, it["ticker"], it.get("name") or "?", it.get("sic_desc") or "?")
             for i, it in enumerate(items)]
    head = ("You classify US-listed companies into the 11 Select Sector SPDR sectors (GICS-based): "
            + ", ".join(XLS) + ".\n"
            "For each company below give its CURRENT sector. If it moved sectors in the September 2018 GICS change "
            "(creation of Communication Services), also give the sector before that change. If it is not an operating "
            "company (fund, SPAC/blank check, shell) answer null. If you do not know the company, answer null with "
            "confidence low. Do NOT guess.\n"
            "Answer ONLY a JSON array, one object per company, in the same order, each like "
            + json.dumps({"ticker": "ABC", "xl": "XLK", "before_2018": None, "confidence": "high"})
            + " (xl and before_2018 may be null; confidence is high, medium or low).\n\n")
    return head + "\n".join(lines)


def parse_answer(text, items):
    """解析模型回答 → {代號: {xl, before_2018, confidence}}；格式不對或代號對不上的丟掉（寧缺勿錯）。"""
    if not text:
        return {}
    a, b = text.find("["), text.rfind("]")
    if a < 0 or b <= a:
        return {}
    try:
        arr = json.loads(text[a:b + 1])
    except ValueError:
        return {}
    want = {it["ticker"].upper() for it in items}
    out = {}
    for x in arr if isinstance(arr, list) else []:
        if not isinstance(x, dict):
            continue
        t = str(x.get("ticker", "")).upper()
        xl, b18 = x.get("xl"), x.get("before_2018")
        if t in want and (xl is None or xl in XLS) and (b18 is None or b18 in XLS):
            out[t] = {"xl": xl, "before_2018": b18, "confidence": x.get("confidence")}
    return out


def agree(ans_by_model, models):
    """兩個模型都答、xl 相同且非 null、信心都不是 low → 採用；其他＝人工。回 (採用, 人工)。"""
    a, b = (ans_by_model.get(m, {}) for m in models)
    take, manual = {}, []
    for t in sorted(set(a) | set(b)):
        x, y = a.get(t), b.get(t)
        if x and y and x["xl"] and x["xl"] == y["xl"] and "low" not in (x.get("confidence"), y.get("confidence")):
            take[t] = {"xl": x["xl"], "before_2018": x["before_2018"] if x["before_2018"] == y["before_2018"] else None}
        else:
            manual.append({"ticker": t, "a": x, "b": y})
    return take, manual


def _dump(rows):
    return "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)


def review_main(a):
    hf = RawHF(write=not a.dry_run, repo=REPO, written_by="sector_history_review")
    ver = a.date or _latest_version(hf)
    if not ver:
        raise SystemExit("⛔ 找不到歸屬表版本")
    base = "%s/%s" % (OUT_PREFIX, ver)
    rv = base + "/review"
    if a.step == "polygon":
        from courier import polygon as PG
        problems = list(csv.DictReader(io.StringIO(hf.read(base + "/problems.csv") or "")))
        profiles = {norm(p["ticker"]): p for p in _jsonl(hf.read(base + "/sec_profiles.jsonl"))}
        ref = PG.sector_fetch_reference_all(_log=print)
        if ref is None:
            raise SystemExit("⛔ Polygon 代號總表讀取失敗")
        poly = {norm(r["ticker"]): r for r in ref}
        upd, queue = classify_problems(problems, profiles, poly)
        hf.add(rv + "/polygon_ref.jsonl", _dump(ref))
        hf.add(rv + "/kind_updates.jsonl", _dump(upd))
        hf.add(rv + "/review_queue.jsonl", _dump(queue))
        hf.add(rv + "/progress.json", json.dumps({"stage": "polygon_done", "queue": len(queue), "batches_done": {}},
                                                 ensure_ascii=False, indent=1))
        n = hf.flush(a.out, "sector review polygon %s" % ver)
        c = Counter(u["kind"] for u in upd)
        print("詳查第 1 步（Polygon）：代號總表 %d 筆｜問題股 %d 支 → 基金／ETF %d、衍生品對回母公司 %d、不明留人工 %d｜"
              "送模型詳查 %d 支（約 %d 批×2 個模型）｜寫出 %d 個檔" % (
                  len(ref), len({norm(r["ticker"]) for r in problems}), c.get("fund", 0), c.get("derivative", 0),
                  c.get("unknown", 0), len(queue), -(-len(queue) // BATCH), n), flush=True)
        return
    if a.step == "llm":
        from courier import openrouter as OR
        if not OR.has_key():
            raise SystemExit("⛔ 缺 OPENROUTER_API_KEY")
        queue = _jsonl(hf.read(rv + "/review_queue.jsonl"))
        prog = json.loads(hf.read(rv + "/progress.json") or "{}")
        done = prog.setdefault("batches_done", {})
        old = _jsonl(hf.read(rv + "/llm_review.jsonl"))
        new_rows, stop, n_calls = [], False, 0
        batches = [queue[i:i + BATCH] for i in range(0, len(queue), BATCH)]
        for m in OR.ALLOWED_MODELS:
            for k, items in enumerate(batches):
                if stop or str(k) in done.get(m, []):
                    continue
                if a.max_batches and n_calls >= a.max_batches:
                    stop = True
                    break
                text, status = OR.chat(m, build_prompt(items))
                n_calls += 1
                if status == "quota":
                    print("OpenRouter 今日額度用完 → 存進度，下次接著跑", flush=True)
                    stop = True
                    break
                ans = parse_answer(text, items)
                new_rows.append({"model": m, "batch": k, "tickers": [it["ticker"] for it in items], "raw": text,
                                 "parsed": ans, "status": status})
                if status == "ok" and ans:
                    done.setdefault(m, []).append(str(k))
        prog["stage"] = "llm_partial" if stop else "llm_done"
        hf.add(rv + "/llm_review.jsonl", _dump(old + new_rows))
        hf.add(rv + "/progress.json", json.dumps(prog, ensure_ascii=False, indent=1))
        hf.flush(a.out, "sector review llm %s" % ver)
        print("詳查第 2 步（模型）：本次呼叫 %d 次｜已完成批數 %s" % (n_calls, {m: len(v) for m, v in done.items()}),
              flush=True)
        return
    if a.step == "apply":
        from courier.openrouter import ALLOWED_MODELS
        rows = _jsonl(hf.read(rv + "/llm_review.jsonl"))
        by_model = {}
        for r in rows:
            by_model.setdefault(r["model"], {}).update(r["parsed"])
        take, manual = agree(by_model, ALLOWED_MODELS)
        hf.add(rv + "/llm_agreed.json", json.dumps(take, ensure_ascii=False, indent=1))
        hf.add(rv + "/manual_review.jsonl", _dump(manual))
        hf.flush(a.out, "sector review apply %s" % ver)
        print("詳查第 3 步（整理）：兩模型一致採用 %d 支｜留人工 %d 支（併入新版歸屬表另案）" % (len(take), len(manual)),
              flush=True)


if __name__ == "__main__":
    main()
