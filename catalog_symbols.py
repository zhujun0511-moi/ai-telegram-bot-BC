# -*- coding: utf-8 -*-
"""
catalog_symbols.py — 證券名冊 _catalog/symbols/v<N>/（2026-11-18 Jun 同意；回應本地端 ADR-073）

輸入：Polygon /v3/reference/tickers 現役＋已下市（全部類型，含權證、單位、權利、特別股）＋身份表 v<N>（可指定版本）
輸出：
  tickers.csv  一筆＝一個代號在一段時間（Polygon 原樣欄位＋照身份表規則推出的 entity_id）
  mpq.csv      身份表每段 MPQ 代號對到的名稱、類別、交易所、下市日
規則：
- entity_id 用 ticker_identity.entity_of（與身份表同一套）；權證等沒有 FIGI／CIK 時可能空。
- mpq 對應先看實體（同 entity_id），對不上再看代號（去掉 -DELISTED 後同代號），都沒有＝none；
  同一實體多筆時，MPQ 段若已下市優先取已下市列，否則取現役列。
- 沒有上市日：Polygon 總表沒有這欄，逐支查明細在免費方案不可行；用 MPQ 第一根日期。
登記：MongoDB_Standard_v3 2026-11-18 條。寫入方＝catalog_rules.py --step symbols。⛔ 只放私有 dataset。日誌只印筆數。
Python 3.9 相容。
"""
import csv
import io
import json
from collections import defaultdict

import ticker_identity as TI
from catalog_publish import CAT, _sha, published, to_csv

SYM_DIR = CAT + "/symbols"
ID_DIR = CAT + "/identity"
TICKER_COLS = ["symbol", "name", "type", "active", "primary_exchange", "currency", "delisted", "cik",
               "composite_figi", "share_class_figi", "entity_id", "table_version"]
MPQ_COLS = ["mpq_symbol", "asset", "entity_id", "name", "type", "primary_exchange", "delisted", "match", "table_version"]
SCHEMA = {
    "schema": "catalog_symbols_v1",
    "tickers.csv": {
        "symbol": "Polygon 代號（點換成橫線）", "name": "名稱", "type": "Polygon 類型：CS、ETF、ETN、ADRC、PFD、WARRANT、UNIT、RIGHT…",
        "active": "1＝現役｜0＝已下市", "primary_exchange": "主要交易所（MIC）", "currency": "幣別",
        "delisted": "下市日 YYYY-MM-DD（現役空）", "cik": "CIK（10 碼，可空）",
        "composite_figi": "Polygon 原值", "share_class_figi": "Polygon 原值",
        "entity_id": "照身份表規則推出的實體（FIGI:／CIK:／NAME:；可空）", "table_version": "本表版本號"},
    "mpq.csv": {
        "mpq_symbol": "身份表的 MPQ 代號", "asset": "stock／etf", "entity_id": "身份表該段的實體",
        "name": "名稱", "type": "Polygon 類型", "primary_exchange": "主要交易所", "delisted": "下市日",
        "match": "entity＝同實體對上｜symbol＝代號相同、實體對不上｜none＝Polygon 查無", "table_version": "本表版本號"},
    "限制": ["沒有上市日：Polygon 總表沒有，請用 MPQ 第一根日期",
           "Polygon 代號被新公司重用時，舊公司的記錄可能已不在總表（身份表 v7 的 SUPN、NORD 即此）"]}


def norm(t):
    return (t or "").strip().upper().replace(".", "-")


def ticker_rows(active_rows, inactive_rows, version):
    """純函數：Polygon 兩份總表 → tickers.csv 的列。"""
    cik2figi = defaultdict(set)
    for r in list(active_rows) + list(inactive_rows):
        if r.get("share_class_figi") and TI._cik(r) is not None:
            cik2figi[TI._cik(r)].add(r["share_class_figi"])
    out = []
    for active, rows in ((1, active_rows), (0, inactive_rows)):
        for r in rows:
            t = norm(r.get("ticker"))
            if not t:
                continue
            c = TI._cik(r)
            out.append({"symbol": t, "name": r.get("name") or "", "type": r.get("type") or "", "active": active,
                        "primary_exchange": r.get("primary_exchange") or "", "currency": r.get("currency_name") or "",
                        "delisted": "" if active else (r.get("delisted_utc") or "")[:10],
                        "cik": "%010d" % c if c is not None else "",
                        "composite_figi": r.get("composite_figi") or "", "share_class_figi": r.get("share_class_figi") or "",
                        "entity_id": TI.entity_of(r, cik2figi) or "", "table_version": version})
    out.sort(key=lambda x: (x["symbol"], x["active"], x["delisted"]))
    return out


def _pick(cands, delisted):
    """同實體或同代號的多筆裡挑一筆：MPQ 段已下市優先取已下市列（最晚下市），否則取現役列。"""
    act = [c for c in cands if c["active"] == 1]
    ina = sorted((c for c in cands if c["active"] == 0), key=lambda c: c["delisted"])
    if delisted:
        return (ina or act)[-1]
    return (act or ina[::-1] or [None])[0]


def mpq_rows(id_rows, tickers, version):
    """純函數：身份表的 mpq 列＋tickers.csv 的列 → mpq.csv 的列（每個 MPQ 代號×資產一列）。"""
    by_e, by_t = defaultdict(list), defaultdict(list)
    for x in tickers:
        if x["entity_id"]:
            by_e[x["entity_id"]].append(x)
        by_t[x["symbol"]].append(x)
    seen, out = set(), []
    for r in id_rows:
        if (r.get("source") or "") != "mpq":
            continue
        k = (r["symbol"], r.get("asset") or "")
        if k in seen:
            continue
        seen.add(k)
        delisted = r["symbol"].upper().endswith("-DELISTED") or bool(r.get("to"))
        hit, how = None, "none"
        if by_e.get(r["entity_id"]):
            hit, how = _pick(by_e[r["entity_id"]], delisted), "entity"
        else:
            base = norm(r["symbol"][:-len("-DELISTED")] if r["symbol"].upper().endswith("-DELISTED") else r["symbol"])
            if by_t.get(base):
                hit, how = _pick(by_t[base], delisted), "symbol"
        out.append({"mpq_symbol": r["symbol"], "asset": r.get("asset") or "", "entity_id": r["entity_id"],
                    "name": hit["name"] if hit else "", "type": hit["type"] if hit else "",
                    "primary_exchange": hit["primary_exchange"] if hit else "", "delisted": hit["delisted"] if hit else "",
                    "match": how, "table_version": version})
    out.sort(key=lambda x: (x["mpq_symbol"], x["asset"]))
    return out


def plan(files, read, active_rows, inactive_rows, identity_version=None):
    """回 (adds, 摘要)。純計算，不寫東西。"""
    id_ver = identity_version or published(files, ID_DIR)[-1]
    if id_ver not in published(files, ID_DIR):
        raise SystemExit("⛔ 找不到 %s/v%d" % (ID_DIR, id_ver))
    id_rows = list(csv.DictReader(io.StringIO(read("%s/v%d/identity.csv" % (ID_DIR, id_ver)))))
    pub = published(files, SYM_DIR)
    ver = (pub[-1] + 1) if pub else 1
    tk = ticker_rows(active_rows, inactive_rows, ver)
    mq = mpq_rows(id_rows, tk, ver)
    strip = lambda rows: [{k: v for k, v in x.items() if k != "table_version"} for x in rows]
    csha = _sha(to_csv(strip(tk), TICKER_COLS[:-1]) + to_csv(strip(mq), MPQ_COLS[:-1]))
    if pub:
        last = json.loads(read("%s/v%d/manifest.json" % (SYM_DIR, pub[-1])))
        if last.get("content_sha256") == csha and last.get("identity_version") == id_ver:
            return [], "沿用 v%d（內容未變）" % pub[-1]
    f = {"tickers.csv": to_csv(tk, TICKER_COLS), "mpq.csv": to_csv(mq, MPQ_COLS),
         "schema.json": json.dumps(SCHEMA, ensure_ascii=False, indent=1)}
    import datetime as dt
    st = defaultdict(int)
    for x in mq:
        st["match_" + x["match"]] += 1
    man = {"version": ver, "created": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "source": "polygon /v3/reference/tickers（現役＋已下市）＋%s/v%d" % (ID_DIR, id_ver),
           "files": {k: ({"sha256": _sha(v), "rows": max(v.count("\n") - 1, 0)} if k.endswith(".csv") else {"sha256": _sha(v)})
                     for k, v in f.items()},
           "identity_version": id_ver, "content_sha256": csha,
           "polygon_rows": {"active": len(active_rows), "inactive": len(inactive_rows)}, "stats": dict(st)}
    base = "%s/v%d" % (SYM_DIR, ver)
    adds = [("%s/%s" % (base, k), v) for k, v in f.items()] + [(base + "/manifest.json", json.dumps(man, ensure_ascii=False, indent=1))]
    return adds, "發布 v%d：代號 %d 筆｜MPQ %d 段（同實體 %d、同代號 %d、查無 %d）｜身份表 v%d" % (
        ver, len(tk), len(mq), st["match_entity"], st["match_symbol"], st["match_none"], id_ver)
