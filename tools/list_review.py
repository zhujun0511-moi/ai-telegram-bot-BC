# -*- coding: utf-8 -*-
"""
tools/list_review.py — DC 股票清單審查（2026-11-13 Jun「照範圍做」；只出報告、不改清單、不寫 Mongo、不發通知）

登記：MongoDB_Standard_v3 六-D「2026-11-13 新產品登記」→ dataset swing_ai/list_review/{日期}/。
標準（Jun 11-13 拍板）：
  - 父板塊（XLB、XLP 這一層）照 SSGA 公布的 XL 實際持股；
  - 子籃子照 TradingView 的板塊／行業；TradingView 至多季度審查才抓（--tv-refresh），平時讀最新快照。
檢查六類：identity 身份／parent 父板塊／basket 子籃子／importance 重要度／candidate 可加入／rule 清單規矩。
嚴重度：act＝要處理｜review＝建議看｜info＝僅供參考。門檻是起始值（📝），看過第一份報告再和 Jun 調。

用法：
  python tools/list_review.py [--dry-run] [--tv-refresh] [--out _list_review_out]
  --dry-run：不寫 dataset，寫到本機 --out 目錄。Python 3.9 相容。日誌只印筆數。
"""
import argparse
import csv
import datetime as dt
import io
import json
import os
import statistics
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from courier import mongo as M                 # noqa: E402
from courier import public_data as PD         # noqa: E402
from courier.dataset import RawHF             # noqa: E402

REPO = "zhujun0511-AI/ai-telegram-bot-dataset"
OUT_PREFIX = "swing_ai/list_review"
TV_PREFIX = OUT_PREFIX + "/tv_snapshot"
VERSION = "list_review v1 (2026-11-13)"

XLS = ("XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY")

# 子籃子行業一致性檢查的豁免名單（主題籃本來就跨行業；📝 待 Jun 確認）
THEMATIC = {"AIQ", "PAVE", "GRID", "ARKK", "XLY1", "IPO", "BOTZ", "URA", "LIT", "XLK8", "UFO", "FINX", "BITQ"}
# 中概 ADR 黑名單（Jun 08-09 硬規則；TradingView 國家欄另抓 China／Hong Kong）
CHINA_ADR = {"BABA", "PDD", "JD", "BIDU", "NIO", "LI", "XPEV", "TCOM", "BILI", "NTES", "TME", "ZTO", "BEKE", "VIPS",
             "FUTU", "TAL", "EDU", "IQ", "WB", "YMM", "HTHT", "ZK", "QFIN", "LU", "GDS", "KC", "TIGR", "BZ", "ATAT"}

# 📝 門檻（起始值）
STALE_DAYS = 5            # 日線落後幾個交易日算停更
VOL_WINDOW = 60           # 成交額取近幾根日線的中位數
LOW_VS_BASKET = 0.2       # 成交額 < 籃子中位數的幾成算偏低
LOW_ABS_DV = 20e6         # 成交額中位數低於多少美元另列
MINOR_WEIGHT = 0.2        # XL 權重（%）低於多少算邊緣
CAND_WEIGHT = 1.0         # XL 權重（%）多少以上、我們沒收的列為可加入
TV_STALE_DAYS = 100       # TradingView 快照超過幾天算過期
BASKET_MIN_TV = 3         # 籃子至少幾支有 TradingView 資料才做一致性檢查


def _today():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")


class Report:
    def __init__(self):
        self.rows = []

    def add(self, kind, severity, ticker, basket, detail, evidence="", suggestion="", text=""):
        self.rows.append({"kind": kind, "severity": severity, "ticker": ticker or "", "basket": basket or "",
                          "detail": detail, "evidence": evidence, "suggestion": suggestion,
                          "text": text or detail})


# ── 輸入 ─────────────────────────────────────────────────────────────

def load_lists():
    cfg = M.find_one("StockData", "Configs", {"type": "ticker_lists"}) or {}
    lists = cfg.get("lists") or {}
    sh = cfg.get("sector_hierarchy") or {}
    return {
        "full_set": sorted({t.upper() for t in cfg.get("full_set") or [] if isinstance(t, str)}),
        "list_1": {t.upper() for t in lists.get("list_1") or []},
        "list_2": {t.upper() for t in lists.get("list_2") or []},
        "list_4": {t.upper() for t in lists.get("list_4") or []},
        "list_5": {t.upper() for t in lists.get("list_5") or []},
        "sub_sectors": sh.get("sub_sectors") or {},
        "parents": sh.get("parents") or {},
        "membership": sh.get("stock_membership") or {},
    }


def load_identity(tickers):
    out = {}
    for d in M.find("StockData", "Ticker_Identity", {"ticker": {"$in": sorted(tickers)}},
                    {"ticker": 1, "active": 1, "delisted": 1, "delisted_utc": 1, "polygon_name": 1, "identity_flag": 1}):
        out[str(d.get("ticker", "")).upper()] = d
    return out


def load_daily(tickers):
    """{代號: (最新日, 近 VOL_WINDOW 根成交額中位數)}；以及 SPY 當交易日曆（新在前）。"""
    out = {}
    for d in M.find("StockData", "Bars", {"period": "D", "ticker": {"$in": sorted(tickers)}},
                    {"ticker": 1, "bars": {"$slice": VOL_WINDOW}}):
        bars = d.get("bars") or []
        newest = str(bars[0].get("t"))[:10] if bars else None
        dv = [float(b.get("c") or 0) * float(b.get("v") or 0) for b in bars if b.get("c") and b.get("v")]
        out[str(d.get("ticker", "")).upper()] = (newest, statistics.median(dv) if dv else None)
    ref = M.find_one("StockData", "Bars", {"ticker": "SPY", "period": "D"}, {"bars": {"$slice": 30}})
    ref_dates = [str(b.get("t"))[:10] for b in (ref or {}).get("bars") or []]
    return out, ref_dates


def load_holdings():
    """{XL: (as_of, [{ticker,name,weight}])}；抓不到的 XL 不在回傳裡。"""
    out = {}
    for xl in XLS:
        try:
            got = PD.ssga_xl_holdings(xl)
        except Exception as e:                       # 單一檔失敗不擋整份報告
            print("⚠️ %s 持股讀取失敗：%s" % (xl, type(e).__name__))
            continue
        if got and got[1]:
            out[xl] = got
    return out


def load_tv(hf, stocks, refresh, limit=0):
    """回 (目前快照 {代號: 欄位}, 快照日期, 上一份快照 或 None)。refresh＝這次重抓並存新快照。
    limit＞0 只送前 limit 支（本機測試用，避免佔用季度那次的全量查詢）。"""
    prev, prev_date = None, None
    try:
        ptr = hf.read(TV_PREFIX + "/latest.json") if hf else None
        if ptr:
            meta = json.loads(ptr)
            prev_date = meta.get("date")
            text = hf.read(meta["path"])
            if text:
                prev = {r["ticker"]: r for r in csv.DictReader(io.StringIO(text))}
    except Exception as e:
        print("⚠️ 讀不到舊 TradingView 快照（%s）" % type(e).__name__)
    if not refresh:
        return prev or {}, prev_date, None
    got = PD.tradingview_lookup(stocks[:limit] if limit else stocks)
    date = _today()
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    cols = ["ticker", "exchange", "type", "sector", "industry", "country", "description"]
    w.writerow(cols)
    snap = {}
    for tk in sorted(got):
        row = dict(got[tk], ticker=tk)
        snap[tk] = {c: row.get(c) or "" for c in cols}
        w.writerow([snap[tk][c] for c in cols])
    path = "%s/%s.csv" % (TV_PREFIX, date)
    hf.add(path, buf.getvalue())
    hf.add(TV_PREFIX + "/latest.json", json.dumps({"date": date, "path": path, "rows": len(snap),
                                                   "previous": prev_date}, ensure_ascii=False, indent=1))
    return snap, date, prev


# ── 檢查 ─────────────────────────────────────────────────────────────

def check_identity(rep, stocks, ident, daily, ref_dates, tv, tv_prev):
    for tk in stocks:
        d = ident.get(tk)
        if d is None:
            rep.add("identity", "info", tk, "", "身份表沒有這支（Ticker_Identity 查無）",
                    text="%s：身份表查無，未能核對是否仍上市" % tk)
        else:
            if d.get("delisted") or d.get("active") is False:
                rep.add("identity", "act", tk, "", "已下市或非現役",
                        evidence="delisted=%s active=%s delisted_utc=%s" % (d.get("delisted"), d.get("active"),
                                                                           d.get("delisted_utc")),
                        suggestion="從 DC 清單移除",
                        text="%s 已下市或非現役（%s），仍在清單" % (tk, d.get("delisted_utc") or "日期不明"))
            if d.get("identity_flag"):
                rep.add("identity", "review", tk, "", "身份旗標：%s" % d.get("identity_flag"),
                        evidence="polygon_name=%s" % d.get("polygon_name"),
                        text="%s 身份有變動（%s），要人工確認是不是同一家公司" % (tk, d.get("identity_flag")))
        newest = (daily.get(tk) or (None, None))[0]
        if newest is None:
            rep.add("identity", "review", tk, "", "沒有日線資料", text="%s 沒有日線資料" % tk)
        elif ref_dates:
            lag = sum(1 for x in ref_dates if x > newest)
            if lag >= STALE_DAYS:
                rep.add("identity", "act", tk, "", "日線停更 %d 個交易日以上" % lag, evidence="最新 %s" % newest,
                        text="%s 日線停在 %s（落後 %d 天以上），可能已下市或換代號" % (tk, newest, lag))
        if tv_prev and tk in tv and tk in tv_prev:
            a, b = tv_prev[tk].get("exchange"), tv[tk].get("exchange")
            if a and b and a != b:
                rep.add("identity", "review", tk, "", "換交易所 %s → %s" % (a, b),
                        text="%s 換交易所：%s → %s" % (tk, a, b))


def _is_synth(key):
    """自建合成籃（XL<父><n>，例 XLB2）；真 ETF 籃（JETS、SMH…）回 False。"""
    k = key.upper()
    return k.startswith("XL") and k.rstrip("0123456789") in XLS and k != k.rstrip("0123456789")


def check_parent(rep, lists, held):
    """父板塊：籃子掛的父板塊裡，至少一個要真的持有這支股票。持股表沒有這支＝未驗證（不當錯）。
    主題籃（THEMATIC）本來就跨板塊，不查。多數成員都屬別的 XL＝整籃掛錯（報籃子一條）；否則報不合群的個股。"""
    unverified = set()
    for key, sub in sorted(lists["sub_sectors"].items()):
        if key.upper() in THEMATIC:
            continue
        parents = set(p.upper() for p in sub.get("parents") or [])
        verified, misfit = [], []
        for tk in sub.get("members") or []:
            tk = tk.upper()
            if tk not in lists["membership"]:
                continue                              # ETF 等非個股
            h = set(held.get(tk, {}))
            if not h:
                unverified.add(tk)
                continue
            verified.append(tk)
            if not (parents & h):
                misfit.append(tk)
        if not misfit:
            continue
        majority = Counter(x for tk in verified for x in held[tk]).most_common(1)[0][0]
        if len(misfit) * 2 > len(verified) and majority not in parents:
            rep.add("parent", "review", "", key,
                    "籃子 %s 掛在 %s，但已驗證的 %d 支裡 %d 支屬別的 XL（最多是 %s）" % (
                        key, "/".join(sorted(parents)), len(verified), len(misfit), majority),
                    evidence="不合群：" + "、".join(misfit), suggestion="list_6 把 %s 改掛（或加掛）%s" % (key, majority),
                    text="籃子 %s 的父板塊可能掛錯：掛 %s，成員多數屬 %s" % (key, "/".join(sorted(parents)), majority))
            continue
        for tk in misfit:
            h = set(held[tk])
            hold_txt = "、".join("%s %.2f%%" % (x, held[tk][x]) for x in sorted(h))
            rep.add("parent", "act" if _is_synth(key) else "review", tk, key,
                    "籃子 %s 掛在 %s，但實際持有它的是 %s" % (key, "/".join(sorted(parents)), hold_txt),
                    evidence="SSGA 持股", suggestion="移到 %s 底下的籃子，或確認是刻意跨板塊" % "/".join(sorted(h)),
                    text="%s 放在 %s（%s），但它屬於 %s" % (tk, key, "/".join(sorted(parents)), "/".join(sorted(h))))
    return unverified


def _basket_profile(members, tv):
    secs = Counter(tv[m]["sector"] for m in members if m in tv and tv[m].get("sector"))
    inds = Counter(tv[m]["industry"] for m in members if m in tv and tv[m].get("industry"))
    return secs, inds


def check_basket(rep, lists, tv):
    if not tv:
        return
    prof = {}
    for key, sub in lists["sub_sectors"].items():
        mem = [m.upper() for m in sub.get("members") or [] if m.upper() in lists["membership"]]
        prof[key] = (mem,) + _basket_profile(mem, tv)
    for key, (mem, secs, inds) in sorted(prof.items()):
        if key in THEMATIC or sum(secs.values()) < BASKET_MIN_TV:
            continue
        top_sec, n_top = secs.most_common(1)[0]
        if n_top * 2 <= sum(secs.values()):
            continue                                   # 籃子本身沒有過半的板塊，不判誰是異類
        for tk in mem:
            t = tv.get(tk)
            if not t or not t.get("sector") or t["sector"] == top_sec:
                continue
            best = []
            for k2, (m2, s2, i2) in prof.items():
                if k2 == key or k2 in THEMATIC or not s2:
                    continue
                if s2.most_common(1)[0][0] == t["sector"]:
                    best.append((i2.get(t.get("industry"), 0), k2))
            best.sort(reverse=True)
            sug = "、".join(k for _, k in best[:3]) if best else "沒有同板塊的籃子"
            rep.add("basket", "review", tk, key,
                    "籃子多數是「%s」，%s 是「%s／%s」" % (top_sec, tk, t["sector"], t.get("industry")),
                    evidence="TradingView", suggestion="可考慮的籃子：%s" % sug,
                    text="%s 在 %s 是異類（籃子多數 %s，它是 %s／%s）；可考慮：%s" % (
                        tk, key, top_sec, t["sector"], t.get("industry"), sug))


def _weight_txt(held, tk):
    h = held.get(tk) or {}
    return "、".join("%s %.2f%%" % (x, h[x]) for x in sorted(h)) if h else "不在任何 XL"


def check_importance(rep, lists, daily, held):
    low = defaultdict(list)                            # 同一支在多個籃子都偏低，合併成一條
    for key, sub in sorted(lists["sub_sectors"].items()):
        mem = [m.upper() for m in sub.get("members") or [] if m.upper() in lists["membership"]]
        dvs = [daily[m][1] for m in mem if m in daily and daily[m][1]]
        if len(dvs) < 3:
            continue
        med = statistics.median(dvs)
        for tk in mem:
            dv = (daily.get(tk) or (None, None))[1]
            if not dv:
                continue
            w = max(list((held.get(tk) or {}).values()) or [0.0])
            if dv < LOW_VS_BASKET * med and w < MINOR_WEIGHT:
                low[tk].append((key, med))
    for tk in sorted(low):
        dv = daily[tk][1]
        bk = "、".join("%s(中位 %.0fM)" % (k, m / 1e6) for k, m in low[tk])
        rep.add("importance", "review", tk, "、".join(k for k, _ in low[tk]),
                "成交額 %.1fM 不到所在籃子中位數的 %d%%；XL：%s" % (dv / 1e6, LOW_VS_BASKET * 100, _weight_txt(held, tk)),
                evidence=bk, suggestion="考慮移出這些籃子",
                text="%s 已邊緣化：成交額 %.1fM／天，所在籃子 %s；%s" % (tk, dv / 1e6, bk, _weight_txt(held, tk)))
    for tk in sorted(lists["membership"]):
        dv = (daily.get(tk) or (None, None))[1]
        if dv and dv < LOW_ABS_DV:
            rep.add("importance", "info", tk, "", "近 %d 天成交額中位數 %.1fM 偏低" % (VOL_WINDOW, dv / 1e6),
                    text="%s 成交額偏低（%.1fM／天）" % (tk, dv / 1e6))


def check_candidate(rep, lists, holdings, tv):
    have = set(lists["full_set"]) | set(lists["membership"])
    for xl, (as_of, rows) in sorted(holdings.items()):
        for r in rows:
            tk = r["ticker"]
            if r["weight"] < CAND_WEIGHT or tk in have:
                continue
            if tk in CHINA_ADR or (tv.get(tk, {}).get("country") in ("China", "Hong Kong")):
                continue
            rep.add("candidate", "review", tk, xl, "%s 權重 %.2f%%，我們沒收" % (xl, r["weight"]),
                    evidence="SSGA %s" % as_of, suggestion="考慮加入 %s 底下的籃子" % xl,
                    text="%s（%s）在 %s 權重 %.2f%%，清單沒有" % (tk, r["name"], xl, r["weight"]))


def check_rules(rep, lists, tv):
    etf_like = lists["list_1"] | lists["list_2"] | set(XLS) | {k.upper() for k in lists["sub_sectors"]}
    for tk in sorted(set(lists["full_set"]) | set(lists["membership"])):
        t = tv.get(tk, {})
        if tk in CHINA_ADR or t.get("country") in ("China", "Hong Kong"):
            rep.add("rule", "act", tk, "", "中概股（黑名單或 TradingView 國家＝%s）" % (t.get("country") or "黑名單"),
                    suggestion="依 Jun 硬規則移除", text="%s 是中概股，違反清單規則" % tk)
    for key, sub in sorted(lists["sub_sectors"].items()):
        k = key.upper()
        if not sub.get("parents"):
            rep.add("rule", "review", "", k, "子籃子沒有父板塊（list_6 孤兒）", text="%s 在 list_6 沒有父板塊" % k)
        if k not in lists["list_2"] and k not in lists["list_5"]:
            rep.add("rule", "review", "", k, "籃子鍵不在 list_2 也不在 list_5（回溯不到指標）",
                    text="籃子 %s 不在 list_2／list_5，下游查不到它的指標" % k)
        for m in sub.get("members") or []:
            if m.upper() in etf_like:
                rep.add("rule", "info", m.upper(), k, "成員是 ETF／籃子代號（不進個股反查，確認是否刻意）",
                        text="%s 的成員 %s 是 ETF 代號" % (k, m.upper()))
    known = {k.upper() for k in lists["sub_sectors"]}
    for p, subs in sorted(lists["parents"].items()):
        for s in subs or []:
            if s.upper() not in known:
                rep.add("rule", "review", "", s.upper(), "list_6 的 %s 引用了不存在的子籃子" % p,
                        text="list_6 %s 引用不存在的 %s" % (p, s.upper()))


# ── 輸出 ─────────────────────────────────────────────────────────────

SEV_ORDER = {"act": 0, "review": 1, "info": 2}
KIND_ZH = {"identity": "身份", "parent": "父板塊", "basket": "子籃子", "importance": "重要度",
           "candidate": "可加入", "rule": "清單規矩"}
SEV_ZH = {"act": "要處理", "review": "建議看", "info": "僅供參考"}


def build_outputs(rep, meta):
    rows = sorted(rep.rows, key=lambda r: (SEV_ORDER[r["severity"]], r["kind"], r["basket"], r["ticker"]))
    cols = ["kind", "severity", "ticker", "basket", "detail", "evidence", "suggestion"]
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(cols)
    alerts = []
    for i, r in enumerate(rows):
        w.writerow([r[c] for c in cols])
        alerts.append({"kind": r["kind"], "severity": r["severity"], "ticker": r["ticker"], "basket": r["basket"],
                       "text": r["text"], "report_row": i + 2})
    cnt = Counter((r["kind"], r["severity"]) for r in rows)
    lines = ["# 清單審查 %s" % meta["date"], "",
             "| 類型 | 要處理 | 建議看 | 僅供參考 |", "|---|---|---|---|"]
    for k in KIND_ZH:
        lines.append("| %s | %d | %d | %d |" % (KIND_ZH[k], cnt[(k, "act")], cnt[(k, "review")], cnt[(k, "info")]))
    lines += ["", "- 清單：個股 %d 支、子籃子 %d 個、full_set %d" % (meta["stocks"], meta["baskets"], meta["full_set"]),
              "- XL 持股：%s" % (meta["holdings"] or "全部讀取失敗"),
              "- 父板塊未驗證（不在任何 XL 持股）：%d 支" % meta["unverified"],
              "- TradingView 快照：%s%s" % (meta["tv_date"] or "沒有", "（⚠️ 已過期，季度審查時加 --tv-refresh）" if meta["tv_stale"] else ""),
              "- 門檻：停更 %d 天、成交額窗 %d 根、偏低 <籃子中位 %d%%、絕對 <%.0fM、邊緣權重 <%.1f%%、可加入權重 ≥%.1f%%" % (
                  STALE_DAYS, VOL_WINDOW, LOW_VS_BASKET * 100, LOW_ABS_DV / 1e6, MINOR_WEIGHT, CAND_WEIGHT),
              "- 子籃子豁免（主題籃）：%s" % "、".join(sorted(THEMATIC))]
    return buf.getvalue(), json.dumps(alerts, ensure_ascii=False, indent=1), "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--tv-refresh", action="store_true")
    ap.add_argument("--out", default="_list_review_out")
    ap.add_argument("--tv-limit", type=int, default=0, help="只送前 N 支給 TradingView（本機測試用）")
    a = ap.parse_args()
    M.configure(local_fallback=True)
    date = _today()
    try:
        hf = RawHF(write=not a.dry_run, repo=REPO, written_by="bc/list_review")
    except Exception as e:
        if not a.dry_run:
            raise
        print("⚠️ 試跑：dataset 連不上（%s），不讀舊快照" % type(e).__name__)
        hf = None

    lists = load_lists()
    stocks = sorted(lists["membership"])
    print("清單：個股 %d、子籃子 %d、full_set %d" % (len(stocks), len(lists["sub_sectors"]), len(lists["full_set"])))
    ident = load_identity(set(stocks) | set(lists["full_set"]))
    daily, ref_dates = load_daily(stocks)
    holdings = load_holdings()
    held = defaultdict(dict)
    for xl, (_, rows) in holdings.items():
        for r in rows:
            held[r["ticker"]][xl] = r["weight"]
    print("身份 %d、日線 %d、XL 持股 %d 檔" % (len(ident), len(daily), len(holdings)))
    if a.tv_refresh and hf is None:
        hf = RawHF(write=False, repo=REPO, written_by="bc/list_review")
    tv, tv_date, tv_prev = load_tv(hf, stocks, a.tv_refresh, a.tv_limit)
    tv_stale = (not tv_date) or (dt.date.fromisoformat(date) - dt.date.fromisoformat(tv_date)).days > TV_STALE_DAYS
    print("TradingView 快照 %s：%d 支" % (tv_date or "無", len(tv)))

    rep = Report()
    check_identity(rep, stocks, ident, daily, ref_dates, tv, tv_prev)
    unverified = check_parent(rep, lists, held)
    check_basket(rep, lists, tv)
    check_importance(rep, lists, daily, held)
    check_candidate(rep, lists, holdings, tv)
    check_rules(rep, lists, tv)

    meta = {"date": date, "stocks": len(stocks), "baskets": len(lists["sub_sectors"]),
            "full_set": len(lists["full_set"]), "unverified": len(unverified), "tv_date": tv_date,
            "tv_stale": tv_stale,
            "holdings": "、".join("%s(%s, %d 檔)" % (x, holdings[x][0], len(holdings[x][1])) for x in sorted(holdings))}
    report_csv, alerts_json, summary = build_outputs(rep, meta)
    hbuf = io.StringIO()
    hw = csv.writer(hbuf, lineterminator="\n")
    hw.writerow(["xl", "ticker", "name", "weight"])
    for xl in sorted(holdings):
        for r in holdings[xl][1]:
            hw.writerow([xl, r["ticker"], r["name"], r["weight"]])
    manifest = dict(meta, created=dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), version=VERSION, rows=len(rep.rows),
                    by_kind=dict(Counter(r["kind"] for r in rep.rows)),
                    by_severity=dict(Counter(r["severity"] for r in rep.rows)))
    base = "%s/%s" % (OUT_PREFIX, date)
    files = [(base + "/report.csv", report_csv), (base + "/alerts.json", alerts_json),
             (base + "/summary.md", summary), (base + "/xl_holdings.csv", hbuf.getvalue()),
             (base + "/manifest.json", json.dumps(manifest, ensure_ascii=False, indent=1))]
    if hf is None:
        hf = type("Local", (), {})()
        hf.pending = []
    for p, t in files:
        hf.pending.append((p, t))
    if isinstance(hf, RawHF):
        n = hf.flush(a.out, "list review %s (%d rows)" % (date, len(rep.rows)))
    else:
        for p, t in hf.pending:
            fp = os.path.join(a.out, p)
            os.makedirs(os.path.dirname(fp), exist_ok=True)
            with open(fp, "w", encoding="utf-8", newline="\n") as f:
                f.write(t)
        n = len(hf.pending)
    print("寫出 %d 檔（%s）；共 %d 條：%s" % (n, "試跑本機" if a.dry_run else "dataset", len(rep.rows),
                                       dict(Counter(r["severity"] for r in rep.rows))))
    return 0


if __name__ == "__main__":
    sys.exit(main())
