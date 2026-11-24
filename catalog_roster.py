# -*- coding: utf-8 -*-
"""
catalog_roster.py — 新 MPQ 名冊（證券名冊 v3 起；2026-11-24 Jun「照範圍做」；對應本地端 ADR-078 D2／D3／D10）

一列＝資產＋代號＋實體＋分段（身份表 v6＋附件的 mpq 段；附件新增、實體已有 mpq 段的 polygon 段＝renamed；
ListingEvents 的 new_listing＝listing）。數據中心只填自己的欄：Polygon 寫法、市場（交易所／OTC／已下市／改名離開／未明）、
進入目前市場日、名稱類別交易所、CIK／FIGI、最後動到該段的身份表附件、動過該實體的拆股附件、逐支查核狀態與問題。
本地端工序欄（S4、接續、考試庫…）不在這裡。

步驟（一次執行依序做，隨時可讓路）：
  bulk     Polygon 三份總表（stocks 現役、otc 現役、stocks 已下市）一次填大部分列；7 天重做一次或身份／拆股附件變了就重做
  check    對不上（或只對上代號）且段未結束的列逐支查（/v3/reference/tickers?ticker=，不限市場，現役＋下市）；每 50 支寫回一次
  publish  全部查完、或週日：發布 _catalog/symbols/v<N>/（版本發布後不改；內容沒變就不發）
讓路：開始前與每查 20 支，用 GitHub API 看 raw_writer_daily／nightly_chain／listing_events／bc_tickets 有沒有未完成的 run，
      有就先寫回進度再結束（Jun 11-23：正式作業一開跑就停），下次從斷點續。
登記：MongoDB_Standard_v3 2026-11-24 條。⛔ 只放私有 dataset；不含價格與量；公開日誌只印數量、不印代號。
Python 3.9 相容。
"""
import argparse
import csv
import datetime as dt
import io
import json
import os
import re
import sys
import time

import catalog_symbols as CS
import listing_events as LE
import ticker_identity as TI
from catalog_publish import CAT, _sha, published, to_csv

SYM_DIR = CAT + "/symbols"
WORK = SYM_DIR + "/_work"
ID_DIR, SPLIT_DIR = CAT + "/identity", CAT + "/splits"
ID_VER, SPLIT_VER = 6, 11                    # 身份表固定 v6（v7 不用、不發 v8）；拆股表 v11
EVENTS_DIR = "swing_ai/listing_events/events"
SAVE_EVERY, YIELD_EVERY = 50, 20
BULK_MAX_AGE_DAYS = 7
GUARD = ("raw_writer_daily.yml", "nightly_chain.yml", "listing_events.yml", "bc_tickets.yml")
ID_COLS = ["entity_id", "source", "symbol", "from", "to", "evidence", "note", "table_version", "asset"]
ROSTER_COLS = ["mpq_symbol", "asset", "entity_id", "name", "type", "primary_exchange", "delisted", "match", "table_version",
               "segment_from", "segment_to", "origin", "polygon_symbol", "market", "market_since", "cik", "share_class_figi",
               "identity_patch", "splits_patches", "checked_at", "check_status", "issue"]
TICKER_COLS = CS.TICKER_COLS[:-1] + ["polygon_ticker", "market", "table_version"]
KEY = ("asset", "mpq_symbol", "entity_id", "segment_from")
SCHEMA = {
    "schema": "catalog_symbols_v3",
    "說明": "新 MPQ 名冊（本地端 ADR-078 格式的數據中心欄）。一列＝資產＋代號＋實體＋分段。不含價格與量。",
    "mpq.csv": {
        "mpq_symbol": "代號（MPQ 寫法；origin＝listing／renamed 時為交易所代號）", "asset": "stock／etf",
        "entity_id": "身份表該段實體", "name": "名稱", "type": "Polygon 類型", "primary_exchange": "主要交易所（MIC）",
        "delisted": "Polygon 下市日（交易所）", "match": "entity＝同實體｜symbol＝只有代號相同｜none＝Polygon 查無",
        "table_version": "本表版本號", "segment_from": "身份表該段起日", "segment_to": "身份表該段結束日（當天已不屬於這段；空＝未結束）",
        "origin": "mpq＝MPQ 段｜renamed＝改名繼承的新代號｜listing＝每日上市事件的新上市",
        "polygon_symbol": "Polygon 寫法（優先股如 ABRpD）；查無空",
        "market": "exchange＝交易所現役｜otc＝OTC 現役｜delisted＝已下市｜renamed_away＝段已結束、實體改用別的代號｜unknown＝查不清",
        "market_since": "進入目前市場的日期（OTC＝最後一次交易所下市日；delisted＝下市日）；不知道空",
        "cik": "CIK（10 碼）", "share_class_figi": "Polygon 原值",
        "identity_patch": "最後動到這段的身份表附件（沒有＝v6）", "splits_patches": "動過這個實體的拆股附件（「|」分隔）",
        "checked_at": "最後查核日（UTC）", "check_status": "pending＝待逐支查｜done＝已查｜failed＝查詢失敗（下次重查）",
        "issue": "查不清的原因"},
    "tickers.csv": "同 v2，另加 polygon_ticker（Polygon 原寫法）、market（exchange／otc／delisted）",
    "changes.csv": "與上一版 mpq.csv 不同的列＋change（added／changed／removed）；上一版是 v2（舊格式）時全部算 added",
}


def today_utc():
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def parse(text):
    return list(csv.DictReader(io.StringIO(text or "")))


# ── 附件 ──
def patch_nums(files, base):
    pat = re.compile(r"^%s/patches/p(\d+)/manifest\.json$" % re.escape(base))
    return sorted({int(m.group(1)) for p in files for m in [pat.match(p)] if m})


def apply_identity(rows, patches):
    """純函數：v6 identity 列＋[(附件名, identity_patch 列)] → 套好的列（_patch＝最後動到的附件、_added＝附件新增）。
    set_to／set_from 對不到或對到多列、add 已存在 → ValueError（整份不套）。"""
    rows = [dict(r, _patch="", _added=False) for r in rows]
    k = lambda r: (r["entity_id"], r["source"], r["symbol"], r["from"])
    for name, prows in patches:
        for p in prows:
            a = p.get("action")
            hit = [r for r in rows if k(r) == k(p)]
            if a in ("set_to", "set_from"):
                if len(hit) != 1:
                    raise ValueError("%s %s 對到 %d 列" % (name, a, len(hit)))
                if a == "set_to":
                    hit[0]["to"] = p.get("to") or ""
                else:
                    hit[0]["from"] = p.get("to") or ""
                if p.get("note"):
                    hit[0]["note"] = p["note"]
                hit[0]["_patch"] = name
            elif a == "add":
                if hit:
                    raise ValueError("%s add 已存在" % name)
                rows.append(dict({c: p.get(c) or "" for c in ID_COLS}, _patch=name, _added=True))
            else:
                raise ValueError("%s 不認得的動作 %s" % (name, a))
    return rows


def splits_patch_map(patches):
    """[(附件名, splits_patch 列)] → {entity_id: [附件名…]}。"""
    m = {}
    for name, prows in patches:
        for p in prows:
            e = p.get("entity_id") or ""
            if e and name not in m.setdefault(e, []):
                m[e].append(name)
    return m


# ── 列 ──
def _base(sym):
    return sym[:-len("-DELISTED")] if sym.upper().endswith("-DELISTED") else sym


def roster_rows(id_rows, listing_rows, smap):
    """純函數：套好附件的身份表列＋ListingEvents new_listing 列＋拆股附件對照 → 名冊骨架（欄位未填）。"""
    mpq_ents = {r["entity_id"] for r in id_rows if r["source"] == "mpq"}
    mpq_syms = {}
    for r in id_rows:
        if r["source"] == "mpq":
            mpq_syms.setdefault(r["entity_id"], set()).add(_base(r["symbol"]).upper())
    out, seen = [], set()

    def add(r, origin, asset):
        row = {c: "" for c in ROSTER_COLS}
        row.update(mpq_symbol=r["symbol"], asset=asset, entity_id=r["entity_id"], segment_from=r.get("from") or "",
                   segment_to=r.get("to") or "", origin=origin, identity_patch=r.get("_patch") or "v%d" % ID_VER,
                   splits_patches="|".join(smap.get(r["entity_id"], [])), check_status="pending")
        key = tuple(row[c] for c in KEY)
        if key not in seen:
            seen.add(key)
            out.append(row)

    for r in id_rows:
        if r["source"] == "mpq":
            add(r, "mpq", r.get("asset") or "stock")
        elif (r["source"] == "polygon" and r.get("_added") and r["entity_id"] in mpq_ents
              and r["symbol"].upper() not in mpq_syms.get(r["entity_id"], set())):
            add(r, "renamed", "stock")
    have = {LE.norm(_base(x["mpq_symbol"])) for x in out if not x["segment_to"]}
    for e in listing_rows:
        if e.get("kind") != "new_listing" or not e.get("symbol") or LE.norm(e["symbol"]) in have:
            continue
        add({"symbol": e["symbol"], "entity_id": e.get("entity") or "NAME:" + (e.get("name") or ""),
             "from": e.get("effective") or e.get("detected") or ""}, "listing", "etf" if (e.get("type") or "") == "ETF" else "stock")
        have.add(LE.norm(e["symbol"]))
    out.sort(key=lambda x: tuple(x[c] for c in KEY))
    return out


def ticker_table(stk, otc, ina):
    """Polygon 三份總表 → 名冊用的代號列（market＝exchange／otc／delisted）。"""
    cik2figi = {}
    for r in list(stk) + list(otc) + list(ina):
        if r.get("share_class_figi") and TI._cik(r) is not None:
            cik2figi.setdefault(TI._cik(r), set()).add(r["share_class_figi"])
    out = []
    for market, active, rows in (("exchange", 1, stk), ("otc", 1, otc), ("delisted", 0, ina)):
        for r in rows:
            t = (r.get("ticker") or "").strip()
            if not t:
                continue
            c = TI._cik(r)
            out.append({"symbol": CS.norm(t), "polygon_ticker": t, "name": r.get("name") or "", "type": r.get("type") or "",
                        "active": active, "primary_exchange": r.get("primary_exchange") or "",
                        "currency": r.get("currency_name") or "", "delisted": "" if active else (r.get("delisted_utc") or "")[:10],
                        "cik": "%010d" % c if c is not None else "", "composite_figi": r.get("composite_figi") or "",
                        "share_class_figi": r.get("share_class_figi") or "", "entity_id": TI.entity_of(r, cik2figi) or "",
                        "market": market})
    out.sort(key=lambda x: (x["symbol"], x["active"], x["delisted"], x["market"]))
    return out


def _pick(cands, ended):
    """段已結束優先取已下市列（最晚），否則現役（交易所先於 OTC）。"""
    act = sorted((c for c in cands if c["active"] == 1), key=lambda c: c["market"] != "exchange")
    ina = sorted((c for c in cands if c["active"] == 0), key=lambda c: c["delisted"])
    if ended:
        return (ina or act or [None])[-1] if ina else (act or [None])[0]
    return (act or ina[::-1] or [None])[0]


def _since(hit, by_t):
    if not hit:
        return ""
    if hit["market"] == "otc":
        prior = sorted(x["delisted"] for x in by_t.get(hit["symbol"], []) if x["active"] == 0 and x["delisted"])
        return prior[-1] if prior else ""
    return hit["delisted"] if hit["market"] == "delisted" else ""


def _set_hit(row, hit, how, since, today):
    row.update(name=hit["name"], type=hit["type"], primary_exchange=hit["primary_exchange"], delisted=hit["delisted"],
               match=how, polygon_symbol=hit["polygon_ticker"], market=hit["market"], market_since=since,
               cik=hit["cik"], share_class_figi=hit["share_class_figi"], checked_at=today)


def bulk_fill(rows, tickers, today):
    """純函數：名冊骨架＋代號列 → 填好的列（就地改並回傳）。同實體對上、或段已結束＝done；其餘 pending 等逐支查。"""
    by_e, by_t = {}, {}
    for x in tickers:
        if x["entity_id"]:
            by_e.setdefault(x["entity_id"], []).append(x)
        by_t.setdefault(x["symbol"], []).append(x)
    open_syms = {}
    for r in rows:
        if not r["segment_to"]:
            open_syms.setdefault(r["entity_id"], set()).add(_base(r["mpq_symbol"]).upper())
    for r in rows:
        ended = bool(r["segment_to"]) or r["mpq_symbol"].upper().endswith("-DELISTED")
        hit, how = None, "none"
        if by_e.get(r["entity_id"]):
            hit, how = _pick(by_e[r["entity_id"]], ended), "entity"
        else:
            c = by_t.get(CS.norm(_base(r["mpq_symbol"])))
            if c:
                hit, how = _pick(c, ended), "symbol"
        r.update(match=how, issue="", checked_at=today)
        if hit:
            _set_hit(r, hit, how, _since(hit, by_t), today)
        if ended and (open_syms.get(r["entity_id"], set()) - {_base(r["mpq_symbol"]).upper()}):
            r["market"], r["market_since"] = "renamed_away", r["segment_to"]
        elif not hit:
            r["market"] = "unknown"
        r["check_status"] = "done" if (how == "entity" or ended) else "pending"
        if ended and not hit:
            r["issue"] = "Polygon 查無（段已結束，不逐支查）"
    return rows


def check_one(raw, row, today):
    """逐支查一列（就地改）。回 True＝查完（含查無）、False＝查詢失敗。先試原寫法，帶點再試 Polygon 優先股寫法 XpY。"""
    b = _base(row["mpq_symbol"])
    cands = [b] + ([b.replace(".", "p", 1)] if re.match(r"^[A-Z]+\.[A-Z]+$", b) else [])
    lk, used = None, ""
    for c in cands:
        lk = LE._lookup(raw, c)
        if lk is None:
            row.update(check_status="failed", checked_at=today, issue="Polygon 查詢失敗")
            return False
        used = c
        if lk["active"] or lk["inactive"]:
            break
    act = sorted(lk["active"], key=lambda x: (x.get("market") or "") != "stocks")
    ina = sorted(lk["inactive"], key=lambda x: (x.get("delisted_utc") or ""))
    row.update(checked_at=today, check_status="done", issue="")
    if not act and not ina:
        row.update(match="none", market="unknown", polygon_symbol="", issue="Polygon 查無（試過 %s）" % "、".join(cands))
        return True
    h = act[0] if act else ina[-1]
    ent = LE.entity_of(h)
    last_ex = (ina[-1].get("delisted_utc") or "")[:10] if ina else ""
    market = ("exchange" if (h.get("market") or "") == "stocks" else "otc") if act else "delisted"
    row.update(name=h.get("name") or "", type=h.get("type") or "", primary_exchange=h.get("primary_exchange") or "",
               delisted="" if act else last_ex, polygon_symbol=used, market=market,
               market_since=last_ex if market in ("otc", "delisted") else "",
               cik=("%010d" % TI._cik(h)) if TI._cik(h) is not None else "", share_class_figi=h.get("share_class_figi") or "",
               match="entity" if ent == row["entity_id"] else "symbol")
    if row["match"] == "symbol" and not row["entity_id"].startswith("MPQ:"):
        row["issue"] = "代號相同但實體不同（可能易主）"
    return True


def merge_previous(rows, prev):
    """重做 bulk 後，仍 pending 的列若上次已逐支查完（30 天內）就沿用上次結果。"""
    old = {tuple(p.get(c, "") for c in KEY): p for p in prev}
    cut = (dt.date.fromisoformat(today_utc()) - dt.timedelta(days=30)).isoformat()
    for r in rows:
        p = old.get(tuple(r[c] for c in KEY))
        if r["check_status"] == "pending" and p and p.get("check_status") == "done" and (p.get("checked_at") or "") >= cut:
            for c in ("name", "type", "primary_exchange", "delisted", "match", "polygon_symbol", "market", "market_since",
                      "cik", "share_class_figi", "checked_at", "check_status", "issue"):
                r[c] = p.get(c, "")
    return rows


def changes(rows, prev):
    old = {tuple(p.get(c, "") for c in KEY): p for p in prev}
    new = {tuple(r[c] for c in KEY): r for r in rows}
    cmp = [c for c in ROSTER_COLS if c not in ("table_version", "checked_at")]
    out = []
    for k, r in new.items():
        p = old.get(k)
        if p is None:
            out.append(dict(r, change="added"))
        elif any((p.get(c) or "") != (r.get(c) or "") for c in cmp):
            out.append(dict(r, change="changed"))
    for k, p in old.items():
        if k not in new:
            out.append(dict({c: p.get(c, "") for c in ROSTER_COLS}, change="removed"))
    return out


def stats(rows):
    s = {}
    for r in rows:
        for k in ("market_" + (r["market"] or "blank"), "status_" + r["check_status"], "origin_" + r["origin"], "match_" + r["match"]):
            s[k] = s.get(k, 0) + 1
    return dict(sorted(s.items()))


def plan_publish(files, read, rows, tickers, inputs):
    """回 (adds, 摘要)；內容沒變回 ([], 摘要)。"""
    pub = published(files, SYM_DIR)
    ver = (pub[-1] + 1) if pub else 1
    strip = lambda rs, cols: to_csv([{k: v for k, v in x.items() if k not in ("table_version", "checked_at")} for x in rs],
                                    [c for c in cols if c not in ("table_version", "checked_at")])
    csha = _sha(strip(rows, ROSTER_COLS) + strip(tickers, TICKER_COLS))
    prev = []
    if pub:
        last = json.loads(read("%s/v%d/manifest.json" % (SYM_DIR, pub[-1])))
        if last.get("content_sha256") == csha:
            return [], "沿用 v%d（內容未變）" % pub[-1]
        prev = parse(read("%s/v%d/mpq.csv" % (SYM_DIR, pub[-1])))
    rows = [dict(r, table_version=ver) for r in rows]
    tk = [dict(t, table_version=ver) for t in tickers]
    ch = changes(rows, prev if prev and "segment_from" in prev[0] else [])
    f = {"mpq.csv": to_csv(rows, ROSTER_COLS), "tickers.csv": to_csv(tk, TICKER_COLS),
         "changes.csv": to_csv(ch, ROSTER_COLS + ["change"]), "schema.json": json.dumps(SCHEMA, ensure_ascii=False, indent=1)}
    man = {"version": ver, "created": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "source": "polygon /v3/reference/tickers（stocks 現役、otc 現役、stocks 已下市＋逐支查）＋%s/v%d 附件＋%s/v%d 附件＋%s" % (
               ID_DIR, ID_VER, SPLIT_DIR, SPLIT_VER, EVENTS_DIR),
           "files": {k: ({"sha256": _sha(v), "rows": max(v.count("\n") - 1, 0)} if k.endswith(".csv") else {"sha256": _sha(v)})
                     for k, v in f.items()},
           "identity_version": ID_VER, "splits_version": SPLIT_VER, "inputs": inputs, "content_sha256": csha,
           "previous": pub[-1] if pub else None, "stats": stats(rows), "changes": len(ch)}
    base = "%s/v%d" % (SYM_DIR, ver)
    adds = [("%s/%s" % (base, k), v) for k, v in f.items()] + [(base + "/manifest.json", json.dumps(man, ensure_ascii=False, indent=1))]
    return adds, "發布 v%d：%d 列、變動 %d" % (ver, len(rows), len(ch))


# ── 外部 ──
def gh_busy():
    """有正式作業未完成 → 回 workflow 檔名；沒有 → None；不在 Actions 內 → None；API 失敗 → 'api_error'（當作忙）。"""
    tok, repo = os.environ.get("GITHUB_TOKEN"), os.environ.get("GITHUB_REPOSITORY")
    if not tok or not repo:
        return None
    import requests
    for st in ("in_progress", "queued"):
        try:
            j = requests.get("https://api.github.com/repos/%s/actions/runs" % repo, params={"status": st, "per_page": 50},
                             headers={"Authorization": "Bearer " + tok, "Accept": "application/vnd.github+json"}, timeout=20).json()
        except Exception:
            return "api_error"
        if "workflow_runs" not in j:
            return "api_error"
        for r in j["workflow_runs"]:
            name = (r.get("path") or "").split("/")[-1]
            if name in GUARD:
                return name
    return None


def load_inputs(files, read):
    """讀身份表 v6＋附件、拆股附件、ListingEvents → (身份列, 拆股對照, 上市事件列, inputs 指紋)。"""
    idb, spb = "%s/v%d" % (ID_DIR, ID_VER), "%s/v%d" % (SPLIT_DIR, SPLIT_VER)
    ip = ["p%d" % n for n in patch_nums(files, idb)]
    sp = ["p%d" % n for n in patch_nums(files, spb)]
    id_rows = apply_identity(parse(read(idb + "/identity.csv")),
                             [(p, parse(read("%s/patches/%s/identity_patch.csv" % (idb, p)))) for p in ip])
    smap = splits_patch_map([(p, parse(read("%s/patches/%s/splits_patch.csv" % (spb, p)))) for p in sp])
    ev_files = sorted(p for p in files if p.startswith(EVENTS_DIR + "/") and p.endswith(".csv"))
    ev = [e for p in ev_files for e in parse(read(p))]
    inputs = {"identity": "v%d+%s" % (ID_VER, "+".join(ip)), "splits": "v%d+%s" % (SPLIT_VER, "+".join(sp)),
              "listing_events_last": ev_files[-1].split("/")[-1][:-4] if ev_files else ""}
    return id_rows, smap, ev, inputs


def fetch_all(raw):
    stk = raw.all_pages("/v3/reference/tickers", {"market": "stocks", "active": "true", "limit": 1000})
    otc = raw.all_pages("/v3/reference/tickers", {"market": "otc", "active": "true", "limit": 1000})
    ina = raw.all_pages("/v3/reference/tickers", {"market": "stocks", "active": "false", "limit": 1000})
    return stk, otc, ina


def run(raw, hf, now=None, busy=gh_busy, budget_min=300, force_bulk=False, publish=None, log=print):
    """回 0＝正常（含讓路）、1＝失敗未寫。"""
    t0 = time.time()
    now = now or dt.datetime.now(dt.timezone.utc)
    today = now.date().isoformat()
    b = busy()
    if b:
        log("正式作業在跑（%s），這次讓路" % b)
        return 0
    files = set(hf.list_files())
    try:
        id_rows, smap, ev, inputs = load_inputs(files, hf.read)
    except ValueError as e:
        log("⛔ 附件套不上：%s；整次不寫" % e)
        return 1
    state = json.loads(hf.read(WORK + "/state.json") or "{}")
    prev = parse(hf.read(WORK + "/roster.csv"))
    tickers = parse(hf.read(WORK + "/tickers.csv"))
    age = (now.date() - dt.date.fromisoformat(state["bulk_at"])).days if state.get("bulk_at") else None

    def save(rows, tickers, note):
        state.update(updated_at=now.strftime("%Y-%m-%dT%H:%M:%SZ"), stats=stats(rows))
        hf.add(WORK + "/roster.csv", to_csv(rows, ROSTER_COLS))
        if tickers is not None:
            hf.add(WORK + "/tickers.csv", to_csv(tickers, TICKER_COLS))
        hf.add(WORK + "/state.json", json.dumps(state, ensure_ascii=False, indent=1))
        hf.flush("_roster_out", note)

    if force_bulk or age is None or age >= BULK_MAX_AGE_DAYS or state.get("inputs") != inputs or not prev:
        stk, otc, ina = fetch_all(raw)
        if stk is None or otc is None or ina is None or len(stk) < 5000:
            log("⛔ Polygon 總表抓取失敗或筆數異常；不寫")
            return 1
        tickers = ticker_table(stk, otc, ina)
        rows = merge_previous(bulk_fill(roster_rows(id_rows, ev, smap), tickers, today), prev)
        state.update(bulk_at=today, inputs=inputs, polygon_rows={"stocks": len(stk), "otc": len(otc), "inactive": len(ina)})
        save(rows, tickers, "roster bulk %s" % today)
        log("bulk：%d 列；%s" % (len(rows), json.dumps(stats(rows), ensure_ascii=False)))
    else:
        rows = prev
        for t in tickers:
            t["active"] = int(t["active"])
    todo = [r for r in rows if r["check_status"] in ("pending", "failed")]
    log("待逐支查 %d 列" % len(todo))
    done_n, dirty, yielded = 0, False, False
    for i, r in enumerate(todo):
        if i and i % YIELD_EVERY == 0:
            b = busy()
            if b:
                log("正式作業開跑（%s），寫回進度後讓路" % b)
                yielded = True
                break
        if time.time() - t0 > budget_min * 60:
            log("到時間上限，寫回進度")
            break
        check_one(raw, r, today)
        done_n += 1
        dirty = True
        if done_n % SAVE_EVERY == 0:
            save(rows, None, "roster check +%d" % SAVE_EVERY)
            dirty = False
    if dirty:
        save(rows, None, "roster check")
    log("本次逐支查 %d 列；%s" % (done_n, json.dumps(stats(rows), ensure_ascii=False)))
    left = sum(1 for r in rows if r["check_status"] in ("pending", "failed"))
    want = publish if publish is not None else ((left == 0 or now.weekday() == 6) and not yielded)
    if want:
        adds, summ = plan_publish(set(hf.list_files()), hf.read, rows, tickers, inputs)
        for p, t in adds:
            hf.add(p, t)
        if adds:
            hf.flush("_roster_pub", summ)
        log(summ)
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget-min", type=int, default=300, help="逐支查最多跑幾分鐘")
    ap.add_argument("--force-bulk", action="store_true")
    ap.add_argument("--publish", choices=("auto", "yes", "no"), default="auto")
    ap.add_argument("--dry-run", action="store_true", help="不寫 dataset")
    a = ap.parse_args()
    from courier import keys
    from courier.dataset import RawHF
    from courier.polygon import RawPolygon
    repo = keys.env_strip("HF_REPO_ID") or "zhujun0511-AI/ai-telegram-bot-dataset"
    hf = RawHF(write=not a.dry_run, repo=repo, written_by="BC catalog_roster")
    pub = {"auto": None, "yes": True, "no": False}[a.publish]
    return run(RawPolygon(print), hf, budget_min=a.budget_min, force_bulk=a.force_bulk, publish=pub)


if __name__ == "__main__":
    sys.exit(main())
