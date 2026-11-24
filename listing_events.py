# -*- coding: utf-8 -*-
"""
listing_events.py — 每日上市事件（2026-11-21 Jun：「應當有每日的固定進程，查詢當日的退市、換市場、新上市等情況」）

每天美東 07:00（週二～週六）比對 Polygon 現役總表（market=stocks）今天與上一份快照：
  新上市（全市場，只收普通股／ETF 類）、下市／疑似下市、換市場、改名、代號易主（清單內）、未來 14 天已公告拆股（清單內）。
清單＝DC Configs.full_set ∪ 本地端總表（最新 _catalog/symbols 的 MPQ 實體與代號，未下市者）。
輸出：Mongo CommData.ListingEvents（只放事實、保留 30 天、本地端定時讀）＋dataset swing_ai/listing_events/（事件永久、快照 35 天）＋Telegram。
規則表附件（身份／拆股）不在這裡自動發，仍由數據中心窗口確認後才發（Jun 11-21）。
登記：MongoDB_Standard_v3「2026-11-21 新集合＋新產品登記」。公開 repo：日誌只印數量、不印代號（代號只進 Telegram／Mongo）。
用法：python listing_events.py [--dry-run]
Python 3.9 相容。
"""
import argparse
import csv
import datetime as dt
import io
import os
import re
import sys
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
DB, COL = "CommData", "ListingEvents"
PREFIX = "swing_ai/listing_events"
KEEP_MONGO_DAYS = 30          # Jun 11-21
KEEP_SNAPSHOT_DAYS = 35
SPLIT_AHEAD_DAYS = 14
LOOKUP_CAP = 60               # 每次最多逐支查 Polygon 的消失代號（免費 5 次/分）
REPORT_TYPE = "data_stale_watch"   # 沿用斷流看門狗，不新增 report_type（避免動 REPORT_CHANNEL_TYPES）
CRON_EDT, CRON_EST = "0 11 * * 2-6", "0 12 * * 2-6"   # 美東 07:00：夏令 UTC 11:00、冬令 UTC 12:00
NEW_LISTING_TYPES = ("CS", "OS", "ADRC", "GDR", "ETF", "ETN", "ETV", "ETS")
SNAP_COLS = ["ticker", "name", "type", "primary_exchange", "cik", "composite_figi", "share_class_figi", "list_date"]
EVENT_COLS = ["_id", "detected", "kind", "symbol", "new_symbol", "entity", "name", "type", "exchange_old", "exchange_new",
              "effective", "scope", "confidence", "split_from", "split_to"]
KINDS = ("new_listing", "delisted", "suspected_delisted", "market_changed", "renamed", "ticker_reused", "split_announced")


# ══ 純函數（tests/test_listing_events.py）══════════════════════════

_PFD_SERIES = re.compile(r"^([A-Z]+)p([A-Z]+)$")


def norm(t):
    """代號比對用：大寫、橫線換點（Polygon 寫 BRK.B，DC 清單可能寫 BRK-B）。
    2026-11-24（Jun 同意）：Polygon 有系列的優先股寫 ABRpD（小寫 p），MPQ 寫 ABR.D；先換成 ABR.D 再大寫，
    否則大寫成 ABRPD 永遠對不上。結尾單獨一個 p（DCOMp）大寫後即 MPQ 的 DCOMP，不需換。"""
    t = (t or "").strip()
    m = _PFD_SERIES.match(t)
    if m:
        t = m.group(1) + "." + m.group(2)
    return t.upper().replace("-", ".")


def entity_of(r):
    figi = (r.get("share_class_figi") or "").strip()
    if figi:
        return "FIGI:" + figi
    cik = re.sub(r"\D", "", r.get("cik") or "")
    if cik:
        return "CIK:" + cik.zfill(10)
    return "NAME:" + (r.get("name") or "").strip()


def same_company(a, b):
    """兩筆 Polygon 列是否同一家：兩邊都有 FIGI 比 FIGI，否則兩邊都有 CIK 比 CIK，否則無法判斷（None）。"""
    fa, fb = (a.get("share_class_figi") or "").strip(), (b.get("share_class_figi") or "").strip()
    if fa and fb:
        return fa == fb
    ca, cb = re.sub(r"\D", "", a.get("cik") or ""), re.sub(r"\D", "", b.get("cik") or "")
    if ca and cb:
        return ca.zfill(10) == cb.zfill(10)
    return None


def diff_snapshots(prev, cur):
    """prev／cur：{ticker: row}。回 dict：appeared、disappeared、renamed[(舊,新)]、reused、market_changed（代號清單）。
    改名＝消失的代號與新出現的代號同一家；配對到的不再算新上市／消失。"""
    appeared = sorted(set(cur) - set(prev))
    disappeared = sorted(set(prev) - set(cur))
    renamed, used_new = [], set()
    for old in disappeared:
        for new in appeared:
            if new not in used_new and same_company(prev[old], cur[new]) is True:
                renamed.append((old, new))
                used_new.add(new)
                break
    reused, market_changed = [], []
    for t in sorted(set(prev) & set(cur)):
        sc = same_company(prev[t], cur[t])
        if sc is False:
            reused.append(t)
            continue
        ea, eb = (prev[t].get("primary_exchange") or "").strip(), (cur[t].get("primary_exchange") or "").strip()
        if ea and eb and ea != eb:
            market_changed.append(t)
    # 改名到一個原本屬於別家的代號（例 INDV→SUPN：舊 Supernus 被併、代號交給 Indivior）：易主照報，另配成改名
    paired = {o for o, _ in renamed}
    for old in disappeared:
        if old in paired:
            continue
        for t in reused:
            if same_company(prev[old], cur[t]) is True:
                renamed.append((old, t))
                paired.add(old)
                break
    gone_renamed = paired
    return {"appeared": [t for t in appeared if t not in used_new],
            "disappeared": [t for t in disappeared if t not in gone_renamed],
            "renamed": renamed, "reused": reused, "market_changed": market_changed}


def in_scope(symbol, entity, scope_syms, scope_ents):
    return norm(symbol) in scope_syms or (entity in scope_ents and not entity.startswith("NAME:"))


def classify_gone(lookup):
    """消失代號逐支查 Polygon 的結果 → (kind, effective, exchange_new, confidence)。
    lookup：{"active": [列…（任何市場）], "inactive": [列…]}；None＝沒查（超過上限或查詢失敗）。"""
    if lookup is None:
        return "suspected_delisted", "", "", "suspect"
    act = lookup.get("active") or []
    if act:
        r = act[0]
        return "market_changed", "", (r.get("primary_exchange") or r.get("market") or ""), "high"
    dl = sorted(((r.get("delisted_utc") or "")[:10] for r in (lookup.get("inactive") or []) if r.get("delisted_utc")), reverse=True)
    if dl:
        return "delisted", dl[0], "", "high"
    return "suspected_delisted", "", "", "suspect"


def event(detected, kind, symbol, row, scope, **kw):
    assert kind in KINDS, kind
    row = row or {}
    d = {"_id": "%s-%s-%s" % (detected, kind, kw.get("id_key") or symbol), "detected": detected, "kind": kind, "symbol": symbol,
         "new_symbol": kw.get("new_symbol", ""), "entity": kw.get("entity") or (entity_of(row) if row else ""),
         "name": row.get("name") or "", "type": row.get("type") or "", "exchange_old": kw.get("exchange_old", ""),
         "exchange_new": kw.get("exchange_new", ""), "effective": kw.get("effective", ""), "scope": scope,
         "confidence": kw.get("confidence", "high"), "split_from": kw.get("split_from", ""), "split_to": kw.get("split_to", ""),
         "evidence": kw.get("evidence") or {k: row.get(k) for k in SNAP_COLS + ["delisted_utc", "market", "active"] if row.get(k) is not None}}
    return d


def prune_query(today):
    cutoff = (dt.date.fromisoformat(today) - dt.timedelta(days=KEEP_MONGO_DAYS)).isoformat()
    return {"detected": {"$lt": cutoff}}


def schedule_ok(event_name, sched, now_et):
    """排程觸發時，只放行美東 07:00 那一個 cron（夏令 UTC 11:00、冬令 UTC 12:00）；手動觸發一律放行。"""
    if event_name != "schedule":
        return True
    want = CRON_EDT if now_et.utcoffset() == dt.timedelta(hours=-4) else CRON_EST
    return (sched or "").strip() == want


def to_csv(cols, rows):
    b = io.StringIO()
    w = csv.DictWriter(b, fieldnames=cols, extrasaction="ignore", lineterminator="\n")
    w.writeheader()
    for r in rows:
        w.writerow({c: ("" if r.get(c) is None else r.get(c)) for c in cols})
    return b.getvalue()


def parse_csv(text):
    return list(csv.DictReader(io.StringIO(text or "")))


def message(today, events, first_run):
    by = {}
    for e in events:
        by.setdefault(e["kind"], []).append(e)
    lab = {"delisted": "下市", "suspected_delisted": "疑似下市", "market_changed": "換市場", "renamed": "改名",
           "ticker_reused": "代號易主", "split_announced": "拆股公告", "new_listing": "新上市"}
    lines = ["📋 上市事件（%s 美東）" % today]
    if first_run:
        lines.append("首次執行：只存現役總表，明天起比對新上市／下市／改名。")
    lines.append("｜".join("%s %d" % (lab[k], len(by.get(k, []))) for k in KINDS))
    for k in ("delisted", "suspected_delisted", "market_changed", "renamed", "ticker_reused", "split_announced"):
        for e in by.get(k, []):
            extra = {"renamed": "→ %s" % e["new_symbol"], "market_changed": "%s→%s" % (e["exchange_old"] or "?", e["exchange_new"] or "?"),
                     "delisted": "下市日 %s" % e["effective"], "split_announced": "%s 生效 %s:%s" % (e["effective"], e["split_from"], e["split_to"]),
                     "suspected_delisted": "明天再確認", "ticker_reused": "換了一家公司"}.get(k, "")
            lines.append("• %s %s %s（%s）" % (lab[k], e["symbol"], extra, e["scope"]))
    nl = [e for e in by.get("new_listing", []) if e["type"] in ("CS", "OS", "ADRC", "GDR")]
    if nl:
        lines.append("新上市普通股 %d 支：%s%s" % (len(nl), "、".join(e["symbol"] for e in nl[:20]), "…" if len(nl) > 20 else ""))
    if not any(by.get(k) for k in KINDS if k != "new_listing"):
        lines.append("清單內沒有下市／換市場／改名／拆股公告。")
    return "\n".join(lines)


# ══ 連網 ══════════════════════════════════════════════════════════

def _snapshot(raw):
    rows = raw.all_pages("/v3/reference/tickers", {"market": "stocks", "active": "true", "limit": 1000})
    if rows is None:
        return None
    return {r["ticker"]: {c: (r.get(c) or "") for c in SNAP_COLS} for r in rows if r.get("ticker")}


def _lookup(raw, t):
    act = raw.all_pages("/v3/reference/tickers", {"ticker": t, "active": "true", "limit": 10})
    ina = raw.all_pages("/v3/reference/tickers", {"ticker": t, "active": "false", "limit": 50})
    if act is None or ina is None:
        return None
    act = [r for r in act if r.get("ticker") == t]
    return {"active": act, "inactive": [r for r in ina if r.get("ticker") == t]}


def _scope(mongo, hf):
    cfg = mongo.find_one("StockData", "Configs", {"type": "ticker_lists"}, {"full_set": 1}) or {}
    dc = {norm(t) for t in (cfg.get("full_set") or [])}
    vers = sorted({int(m.group(1)) for p in hf.list_files() for m in [re.match(r"_catalog/symbols/v(\d+)/mpq\.csv$", p)] if m})
    syms, ents = set(), set()
    if vers:
        for r in parse_csv(hf.read("_catalog/symbols/v%d/mpq.csv" % vers[-1])):
            s = r.get("mpq_symbol") or ""
            if (r.get("delisted") or "").strip() or s.upper().endswith("-DELISTED"):
                continue
            syms.add(norm(s))
            if (r.get("entity_id") or "") and not r["entity_id"].startswith("MPQ:"):
                ents.add(r["entity_id"])
    return dc, syms, ents, (vers[-1] if vers else None)


def run(dry_run=False, log=print):
    from courier import mongo, keys
    from courier.dataset import RawHF
    from courier.polygon import RawPolygon
    from courier.outbound import notify
    now = dt.datetime.now(ET)
    if not schedule_ok(os.environ.get("GITHUB_EVENT_NAME", ""), os.environ.get("IN_SCHED", ""), now):
        log("排程：這個 cron 不是美東 07:00（夏令／冬令另一個），跳過")
        return 0
    today = now.date().isoformat()
    repo = keys.env_strip("HF_REPO_ID") or "zhujun0511-AI/ai-telegram-bot-dataset"
    mongo.configure(strict=True, serverSelectionTimeoutMS=20000)
    hf = RawHF(write=not dry_run, repo=repo, written_by="BC listing_events")
    raw = RawPolygon(log)

    dc, local_syms, local_ents, symv = _scope(mongo, hf)
    scope_syms = dc | local_syms
    log("清單：DC %d 支＋本地端總表 %d 代號／%d 實體（名冊 v%s）" % (len(dc), len(local_syms), len(local_ents), symv))
    sc = lambda sym, ent: ("dc_list" if norm(sym) in dc else "local_universe") if in_scope(sym, ent, scope_syms, local_ents) else None

    cur = _snapshot(raw)
    if not cur or len(cur) < 5000:
        log("⛔ Polygon 現役總表讀取失敗或筆數異常（%s），不比對、不寫入" % (len(cur) if cur else cur))
        if not dry_run:
            notify("❌ [上市事件] Polygon 現役總表讀取失敗或筆數異常，今天沒有比對", report_type=REPORT_TYPE)
        return 1
    files = hf.list_files()
    snaps = sorted(m.group(1) for p in files for m in [re.match(PREFIX + r"/snapshots/(\d{4}-\d{2}-\d{2})\.csv$", p)] if m)
    prev_day = max([d for d in snaps if d < today], default=None)
    events = []
    first_run = prev_day is None
    if not first_run:
        prev = {r["ticker"]: r for r in parse_csv(hf.read("%s/snapshots/%s.csv" % (PREFIX, prev_day)))}
        dfr = diff_snapshots(prev, cur)
        for t in dfr["appeared"]:
            if cur[t].get("type") in NEW_LISTING_TYPES:
                events.append(event(today, "new_listing", t, cur[t], sc(t, entity_of(cur[t])) or "market",
                                    effective=cur[t].get("list_date") or "", exchange_new=cur[t].get("primary_exchange") or ""))
        for old, new in dfr["renamed"]:
            s = sc(old, entity_of(prev[old])) or sc(new, entity_of(cur[new]))
            if s:
                events.append(event(today, "renamed", old, cur[new], s, new_symbol=new,
                                    exchange_old=prev[old].get("primary_exchange") or "", exchange_new=cur[new].get("primary_exchange") or ""))
        for t in dfr["reused"]:
            s = sc(t, entity_of(prev[t])) or sc(t, entity_of(cur[t]))
            if s:
                events.append(event(today, "ticker_reused", t, cur[t], s, evidence={"before": prev[t], "after": cur[t]}))
        for t in dfr["market_changed"]:
            s = sc(t, entity_of(cur[t]))
            if s:
                events.append(event(today, "market_changed", t, cur[t], s, exchange_old=prev[t].get("primary_exchange") or "",
                                    exchange_new=cur[t].get("primary_exchange") or ""))
        gone = [t for t in dfr["disappeared"] if sc(t, entity_of(prev[t]))]
        for i, t in enumerate(gone):
            lk = _lookup(raw, t) if i < LOOKUP_CAP else None
            kind, eff, exn, conf = classify_gone(lk)
            ev = dict(prev[t], **({"delisted_utc": eff} if eff else {}))
            events.append(event(today, kind, t, ev, sc(t, entity_of(prev[t])), effective=eff, confidence=conf,
                                exchange_old=prev[t].get("primary_exchange") or "", exchange_new=exn))
        log("比對 %s→%s：新出現 %d、消失 %d（清單內 %d）、改名 %d、易主 %d、換交易所 %d" % (
            prev_day, today, len(dfr["appeared"]), len(dfr["disappeared"]), len(gone), len(dfr["renamed"]),
            len(dfr["reused"]), len(dfr["market_changed"])))
    # 前幾天的疑似下市：再確認
    since = (now.date() - dt.timedelta(days=10)).isoformat()
    sus = list(mongo.find(DB, COL, {"kind": "suspected_delisted", "detected": {"$gte": since, "$lt": today},
                                    "resolved": {"$exists": False}}))
    for d in sus[:LOOKUP_CAP]:
        t = d["symbol"]
        if t in cur:
            if not dry_run:
                mongo.update_one(DB, COL, {"_id": d["_id"]}, {"$set": {"resolved": "reappeared", "written_at": now.isoformat()}})
            continue
        kind, eff, exn, conf = classify_gone(_lookup(raw, t))
        if kind != "suspected_delisted":
            events.append(event(today, kind, t, dict(d.get("evidence") or {}, delisted_utc=eff), d.get("scope") or "local_universe",
                                entity=d.get("entity"), effective=eff, exchange_old=d.get("exchange_old") or "", exchange_new=exn))
            if not dry_run:
                mongo.update_one(DB, COL, {"_id": d["_id"]}, {"$set": {"resolved": kind, "written_at": now.isoformat()}})
    # 已公告拆股（未來 14 天，清單內）
    sp = raw.all_pages("/v3/reference/splits", {"execution_date.gte": today,
                                                 "execution_date.lte": (now.date() + dt.timedelta(days=SPLIT_AHEAD_DAYS)).isoformat(),
                                                 "limit": 1000}) or []
    seen = {(d.get("symbol"), d.get("effective")) for d in mongo.find(
        DB, COL, {"kind": "split_announced", "effective": {"$gte": today}, "detected": {"$lt": today}}, {"symbol": 1, "effective": 1})}
    for s in sp:
        t = s.get("ticker") or ""
        if (t, s.get("execution_date")) in seen:      # 前幾天已報過，不重報
            continue
        row = cur.get(t) or {}
        scp = sc(t, entity_of(row) if row else "")
        if scp:
            events.append(event(today, "split_announced", t, row, scp, id_key="%s-%s" % (t, s.get("execution_date")),
                                effective=s.get("execution_date") or "", split_from=s.get("split_from"), split_to=s.get("split_to"),
                                evidence={"polygon_id": s.get("id"), "ticker": t}))
    cnt = {k: sum(1 for e in events if e["kind"] == k) for k in KINDS}
    log("事件：" + "、".join("%s %d" % (k, v) for k, v in cnt.items()) + ("（首次執行，只存快照）" if first_run else ""))
    if dry_run:
        log("試跑：不寫 Mongo、不寫 dataset、不發通知")
        return 0
    # 寫入：Mongo（只 $set 我方欄位，不碰 local_seen_at）→ 刪 30 天前 → dataset → Telegram
    mongo.create_index(DB, COL, "detected")
    wa = now.isoformat()
    for e in events:
        doc = {k: v for k, v in e.items() if k != "_id"}
        doc["written_at"] = wa
        mongo.update_one(DB, COL, {"_id": e["_id"]}, {"$set": doc}, upsert=True)
    pr = mongo.delete_many(DB, COL, prune_query(today))
    hf.add("%s/snapshots/%s.csv" % (PREFIX, today), to_csv(SNAP_COLS, [dict(v, ticker=k) for k, v in sorted(cur.items())]))
    hf.add("%s/events/%s.csv" % (PREFIX, today), to_csv(EVENT_COLS, events))
    old_cut = (now.date() - dt.timedelta(days=KEEP_SNAPSHOT_DAYS)).isoformat()
    for d in snaps:
        if d < old_cut:
            hf.delete("%s/snapshots/%s.csv" % (PREFIX, d))
    hf.flush(".", "listing events %s" % today)
    log("寫入：Mongo %d 筆、刪 30 天前 %d 筆；dataset 快照＋事件已上傳" % (len(events), pr.deleted_count))
    notify(message(today, events, first_run), report_type=REPORT_TYPE)
    return 0


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="只讀取比對，不寫 Mongo／dataset、不通知")
    a = ap.parse_args()
    return run(dry_run=a.dry_run)


if __name__ == "__main__":
    sys.exit(main())
