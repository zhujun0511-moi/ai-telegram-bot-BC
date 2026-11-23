# -*- coding: utf-8 -*-
"""
tools/change_table_fetch.py — 10-20～11-20 變動表的 Polygon 部分（一次性；2026-11-23 本地端需求，Jun「同意」）

ListingEvents（listing_events.py）11-21 才開始，之前一個月的改名／下市／合併要補。這裡只抓原始事實，
分析、人工核對、交付給本地端的 CSV 都在數據中心窗口做（不在公開 repo）。
  ① Polygon 歷史現役總表兩份（/v3/reference/tickers date=START、date=END）→ 用 listing_events.diff_snapshots 比對
  ② 清單內（DC full_set ∪ 本地端名冊）消失代號逐支查現役／下市（delisted_utc）
  ③ 清單內改名代號查 ticker events（確切改代號日期）
  ④ 拆股 execution_date 在 START+1～AHEAD；非一般現金股利（dividend_type≠CD）只留類型與日期，不留金額
輸出：私有 dataset swing_ai/_probe/change_table_<TAG>/（登記 MongoDB_Standard_v3 2026-11-23）。
公開 repo：日誌只印數量，不印代號。
用法：python tools/change_table_fetch.py [--start 2026-10-17] [--end 2026-11-20] [--ahead 2026-11-30] [--dry-run]
Python 3.9 相容。
"""
import argparse
import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import listing_events as LE        # noqa: E402

LOOKUP_CAP = 400      # 消失代號逐支查上限（每支 2 次查詢、13 秒一次 → 約 3 小時內）
EVENTS_CAP = 200      # 改名代號查 ticker events 上限
GONE_COLS = ["ticker", "kind", "effective", "exchange_new", "confidence", "entity", "name", "type", "primary_exchange", "scope"]
REN_COLS = ["old", "new", "entity", "name_old", "name_new", "exchange_old", "exchange_new", "scope"]
EV_COLS = ["queried", "entity", "date", "ticker"]
SPLIT_COLS = ["ticker", "execution_date", "split_from", "split_to", "scope"]
DIV_COLS = ["ticker", "ex_dividend_date", "declaration_date", "pay_date", "dividend_type", "frequency", "scope"]


def snapshot(raw, day):
    rows = raw.all_pages("/v3/reference/tickers", {"market": "stocks", "active": "true", "date": day, "limit": 1000})
    if rows is None:
        return None
    return {r["ticker"]: {c: (r.get(c) or "") for c in LE.SNAP_COLS} for r in rows if r.get("ticker")}


def date_param_check(start, end, probes):
    """歷史總表是否真的按日期回（不是悄悄回今天）：probes＝[(代號, 應在 start, 應在 end)]。"""
    out = {}
    for t, in_s, in_e in probes:
        out[t] = ((t in start) == in_s) and ((t in end) == in_e)
    return out


def ticker_events(raw, key):
    j = raw._get(LE_EVENTS_URL % key, {"types": "ticker_change"})
    if not j:
        return None
    res = j.get("results") or {}
    return res.get("composite_figi") or "", [(e.get("date") or "", (e.get("ticker_change") or {}).get("ticker") or "")
                                             for e in (res.get("events") or [])]


LE_EVENTS_URL = "https://api.polygon.io/vX/reference/tickers/%s/events"
# 2026-11-16 BKH→HZN、11-12 SHNY→GLLD：10-17 應有舊代號、無新代號；11-20 反之（日期參數失效時會被抓出來）
PROBES = [("BKH", True, False), ("HZN", False, True), ("SHNY", True, False), ("GLLD", False, True)]


def run(raw, hf, scope, start, end, ahead, tag, log=print):
    dc, local_syms, local_ents = scope
    scope_syms = dc | local_syms

    def sc(sym, ent):
        if not LE.in_scope(sym, ent, scope_syms, local_ents):
            return ""
        return "dc_list" if LE.norm(sym) in dc else "local_universe"

    s0, s1 = snapshot(raw, start), snapshot(raw, end)
    if not s0 or not s1 or len(s0) < 5000 or len(s1) < 5000:
        log("⛔ 歷史總表讀取失敗或筆數異常（%s／%s），不寫入" % (len(s0) if s0 else s0, len(s1) if s1 else s1))
        return 1
    chk = date_param_check(s0, s1, PROBES)
    log("歷史總表：%s %d 筆、%s %d 筆；日期參數抽查 %d/%d 對" % (start, len(s0), end, len(s1), sum(chk.values()), len(chk)))
    d = LE.diff_snapshots(s0, s1)

    ren = []
    for old, new in d["renamed"]:
        s = sc(old, LE.entity_of(s0[old])) or sc(new, LE.entity_of(s1[new]))
        ren.append({"old": old, "new": new, "entity": LE.entity_of(s1[new]), "name_old": s0[old].get("name"),
                    "name_new": s1[new].get("name"), "exchange_old": s0[old].get("primary_exchange"),
                    "exchange_new": s1[new].get("primary_exchange"), "scope": s})
    evs = []
    for i, r in enumerate([r for r in ren if r["scope"]]):
        if i >= EVENTS_CAP:
            break
        got = ticker_events(raw, r["new"])
        if got is None:
            evs.append({"queried": r["new"], "entity": r["entity"], "date": "", "ticker": ""})
            continue
        for day, t in got[1]:
            evs.append({"queried": r["new"], "entity": r["entity"], "date": day, "ticker": t})

    gone_rows = []
    gone = [t for t in d["disappeared"] if sc(t, LE.entity_of(s0[t]))]
    for i, t in enumerate(gone):
        lk = LE._lookup(raw, t) if i < LOOKUP_CAP else None
        kind, eff, exn, conf = LE.classify_gone(lk)
        gone_rows.append({"ticker": t, "kind": kind, "effective": eff, "exchange_new": exn, "confidence": conf,
                          "entity": LE.entity_of(s0[t]), "name": s0[t].get("name"), "type": s0[t].get("type"),
                          "primary_exchange": s0[t].get("primary_exchange"), "scope": sc(t, LE.entity_of(s0[t]))})

    d_from = (dt.date.fromisoformat(start) + dt.timedelta(days=1)).isoformat()
    sp = raw.all_pages("/v3/reference/splits", {"execution_date.gte": d_from, "execution_date.lte": ahead, "limit": 1000})
    dv = raw.all_pages("/v3/reference/dividends", {"ex_dividend_date.gte": d_from, "ex_dividend_date.lte": ahead, "limit": 1000})
    splits = [{"ticker": x.get("ticker"), "execution_date": x.get("execution_date"), "split_from": x.get("split_from"),
               "split_to": x.get("split_to"), "scope": sc(x.get("ticker") or "", "")} for x in (sp or [])]
    divs = [{c: x.get(c) for c in DIV_COLS if c != "scope"} for x in (dv or []) if (x.get("dividend_type") or "CD") != "CD"]
    for x in divs:
        x["scope"] = sc(x.get("ticker") or "", "")

    appeared = [{"ticker": t, "type": s1[t].get("type"), "list_date": s1[t].get("list_date"),
                 "primary_exchange": s1[t].get("primary_exchange"), "entity": LE.entity_of(s1[t])} for t in d["appeared"]]
    reused = [{"ticker": t, "entity_old": LE.entity_of(s0[t]), "entity_new": LE.entity_of(s1[t]),
               "scope": sc(t, LE.entity_of(s0[t])) or sc(t, LE.entity_of(s1[t]))} for t in d["reused"]]
    mkt = [{"ticker": t, "exchange_old": s0[t].get("primary_exchange"), "exchange_new": s1[t].get("primary_exchange"),
            "scope": sc(t, LE.entity_of(s1[t]))} for t in d["market_changed"]]

    meta = {"start": start, "end": end, "ahead": ahead, "created": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "date_param_check": chk, "snap_rows": [len(s0), len(s1)],
            "counts": {"renamed": len(ren), "renamed_in_scope": sum(1 for r in ren if r["scope"]),
                       "disappeared_in_scope": len(gone), "lookup_cap": LOOKUP_CAP, "events_rows": len(evs),
                       "appeared": len(appeared), "reused": len(reused), "market_changed": len(mkt),
                       "splits": len(splits), "splits_query_ok": sp is not None,
                       "dividends_nonregular": len(divs), "dividends_query_ok": dv is not None},
            "note": "一次性；變動表 Polygon 部分。不含價格（股利不留金額）。"}
    P = "swing_ai/_probe/change_table_%s/" % tag
    snap_cols = LE.SNAP_COLS
    hf.add(P + "snap_start.csv", LE.to_csv(snap_cols, [dict(v, ticker=k) for k, v in sorted(s0.items())]))
    hf.add(P + "snap_end.csv", LE.to_csv(snap_cols, [dict(v, ticker=k) for k, v in sorted(s1.items())]))
    hf.add(P + "renamed.csv", LE.to_csv(REN_COLS, ren))
    hf.add(P + "rename_events.csv", LE.to_csv(EV_COLS, evs))
    hf.add(P + "gone_lookup.csv", LE.to_csv(GONE_COLS, gone_rows))
    hf.add(P + "appeared.csv", LE.to_csv(["ticker", "type", "list_date", "primary_exchange", "entity"], appeared))
    hf.add(P + "reused.csv", LE.to_csv(["ticker", "entity_old", "entity_new", "scope"], reused))
    hf.add(P + "market_changed.csv", LE.to_csv(["ticker", "exchange_old", "exchange_new", "scope"], mkt))
    hf.add(P + "splits.csv", LE.to_csv(SPLIT_COLS, splits))
    hf.add(P + "dividends_nonregular.csv", LE.to_csv(DIV_COLS, divs))
    hf.add(P + "meta.json", json.dumps(meta, ensure_ascii=False, indent=1) + "\n")
    n = hf.flush("_change_table_out", "change table %s→%s" % (start, end))
    log("改名 %d（清單內 %d）、清單內消失 %d、新出現 %d、易主 %d、換交易所 %d、拆股 %d、非一般股利 %d；寫入 %d 檔" % (
        len(ren), meta["counts"]["renamed_in_scope"], len(gone), len(appeared), len(reused), len(mkt), len(splits), len(divs), n))
    return 0


EXTRA_EVENTS_CAP = 60
EXTRA_COLS = ["ticker", "found", "active_ticker", "active_market", "active_exchange", "active_type", "active_name",
              "active_entity", "inactive_n", "delisted_utc", "inactive_name", "inactive_entity"]


def run_extra(raw, hf, tag, log=print):
    """2026-11-23 補查：MPQ 總表 active 但不在 Polygon 交易所名單的代號，逐支查現役（不限市場，含 OTC）與下市。
    清單由數據中心窗口放在私有 dataset（代號不進公開 repo）。"""
    P = "swing_ai/_probe/change_table_%s/" % tag
    txt = hf.read(P + "extra_tickers.txt")
    if not txt:
        log("⛔ 找不到補查清單，不查")
        return 1
    tickers = [t.strip() for t in txt.splitlines() if t.strip() and not t.startswith("#")]
    rows, evs, fail = [], [], 0
    for t in tickers:
        lk = LE._lookup(raw, t)
        if lk is None:
            fail += 1
            rows.append({"ticker": t, "found": "query_failed"})
            continue
        a = (lk.get("active") or [{}])[0] if lk.get("active") else {}
        ina = sorted(lk.get("inactive") or [], key=lambda r: r.get("delisted_utc") or "", reverse=True)
        i0 = ina[0] if ina else {}
        rows.append({"ticker": t, "found": "active" if a else ("inactive" if ina else "none"),
                     "active_ticker": a.get("ticker"), "active_market": a.get("market"), "active_exchange": a.get("primary_exchange"),
                     "active_type": a.get("type"), "active_name": a.get("name"), "active_entity": LE.entity_of(a) if a else "",
                     "inactive_n": len(ina), "delisted_utc": (i0.get("delisted_utc") or "")[:10], "inactive_name": i0.get("name"),
                     "inactive_entity": LE.entity_of(i0) if i0 else ""})
    cand = [r for r in rows if r["found"] in ("active", "inactive")]
    for r in cand[:EXTRA_EVENTS_CAP]:
        got = ticker_events(raw, r["ticker"])
        for day, nt in (got[1] if got else [("", "")]):
            evs.append({"queried": r["ticker"], "entity": r.get("active_entity") or r.get("inactive_entity") or "", "date": day, "ticker": nt})
    meta = {"mode": "extra", "created": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "tickers": len(tickers),
            "found": {k: sum(1 for r in rows if r["found"] == k) for k in ("active", "inactive", "none", "query_failed")},
            "events_queried": min(len(cand), EXTRA_EVENTS_CAP), "events_cap": EXTRA_EVENTS_CAP, "note": "一次性補查；不含價格"}
    hf.add(P + "extra_lookup.csv", LE.to_csv(EXTRA_COLS, rows))
    hf.add(P + "extra_events.csv", LE.to_csv(EV_COLS, evs))
    hf.add(P + "extra_meta.json", json.dumps(meta, ensure_ascii=False, indent=1) + "\n")
    n = hf.flush("_change_table_out", "change table extra lookup %d" % len(tickers))
    log("補查 %d 支：現役 %d、只有下市紀錄 %d、查無 %d、查詢失敗 %d；查改名事件 %d 支；寫入 %d 檔" % (
        len(tickers), meta["found"]["active"], meta["found"]["inactive"], meta["found"]["none"], fail, meta["events_queried"], n))
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--extra", action="store_true", help="補查模式：讀私有 dataset 的 extra_tickers.txt 逐支查")
    ap.add_argument("--start", default="2026-10-17")
    ap.add_argument("--end", default="2026-11-20")
    ap.add_argument("--ahead", default="2026-11-30")
    ap.add_argument("--tag", default="20261123")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    from courier import mongo, keys
    from courier.dataset import RawHF
    from courier.polygon import RawPolygon
    repo = keys.env_strip("HF_REPO_ID") or "zhujun0511-AI/ai-telegram-bot-dataset"
    mongo.configure(strict=True, serverSelectionTimeoutMS=20000)
    hf = RawHF(write=not a.dry_run, repo=repo, written_by="BC change_table_fetch")
    if a.extra:
        return run_extra(RawPolygon(print), hf, a.tag)
    dc, syms, ents, symv = LE._scope(mongo, hf)
    print("清單：DC %d 支＋本地端總表 %d 代號／%d 實體（名冊 v%s）" % (len(dc), len(syms), len(ents), symv))
    return run(RawPolygon(print), hf, (dc, syms, ents), a.start, a.end, a.ahead, a.tag)


if __name__ == "__main__":
    sys.exit(main())
