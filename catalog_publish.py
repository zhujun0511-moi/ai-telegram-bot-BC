# -*- coding: utf-8 -*-
"""
catalog_publish.py — 把身份表、板塊表發布成倉庫清單的規則表正本（2026-10-18 Jun 拍板）：
  swing_ai/identity/{版}/        → _catalog/identity/v<N>/（長表：公司 → 各來源代號；v1 只有 Polygon）
  swing_ai/sector_history/{版}/  → _catalog/sector/v<N>/  （按公司，不按代號）
規矩（MongoDB_Standard §六-D 10-18 晚登記）：
- 一次提交含數據＋manifest；v<N> 已存在不覆蓋、取下一號；來源版本與上次發布相同就不發新版。
- 易主前那段由「現任公司」行業代碼推算的板塊：保留，confidence=low，note 註明（Jun C）。
- 公開日誌只印數量。
用法：python sector_history.py --step publish [--dry-run]
"""
import csv
import datetime as dt
import hashlib
import io
import json
import re
from collections import defaultdict

import ticker_identity as TI

CAT = "_catalog"
ID_DIR = CAT + "/identity"
SEC_DIR = CAT + "/sector"
SECTOR_SRC = "swing_ai/sector_history"
REUSE_NOTE = "代號易主前：板塊依現任公司行業代碼推定，不一定適用"

ID_COLS = ["entity_id", "source", "symbol", "from", "to", "evidence", "note", "table_version"]
ENT_COLS = ["entity_id", "name", "type", "active"]
SEC_COLS = ["entity_id", "from", "to", "xl", "source", "confidence", "note", "table_version"]
UNM_COLS = ["ticker", "from", "to", "xl", "source", "reason"]

ID_SCHEMA = {
    "schema": "catalog_identity_v1",
    "identity.csv": {
        "entity_id": "公司（股份）編號：FIGI:{share_class_figi} 優先 → 該 CIK 只有一種股份時借用其 FIGI → CIK:{10 碼} → NAME:{名稱}",
        "source": "代號出自哪個來源：polygon（v1 只有這個）；之後 mpq／yahoo／td／mongo",
        "symbol": "該來源在這段時間用的代號（點形，例 BRK.B）",
        "from": "這段起始日 YYYY-MM-DD；空＝最早可得數據起（Polygon 不給上市日）",
        "to": "這段結束日 YYYY-MM-DD（Polygon delisted_utc）；空＝現役。to 當天已不屬於這段",
        "evidence": "證據來源：polygon／sec／mpq_range／manual",
        "note": "說明，例代號易主",
        "table_version": "本表版本號，與 manifest.version 一致"},
    "entities.csv": {"entity_id": "同上", "name": "最近一段的名稱", "type": "Polygon 類型（CS、ETF…）",
                     "active": "1＝現在仍有現役代號"},
    "查詢": "某代號某天屬於哪家公司：取該 symbol 中第一筆 to 晚於該天（或 to 空）的行"}
SEC_SCHEMA = {
    "schema": "catalog_sector_v1",
    "sector.csv": {
        "entity_id": "公司編號，與同版 manifest.identity_version 的身份表一致",
        "from": "起始月 YYYY-MM（含）", "to": "結束月 YYYY-MM（含）",
        "月份歸屬": "每個月按當月 1 日屬於哪家公司判斷（易主在月中時，那整個月歸舊公司），與板塊重建同一規則",
        "xl": "最主要的一個 XL 板塊（R18）",
        "source": "gics＝當時 S&P 成分股 GICS｜sic_map＝SEC 行業代碼對照｜rule＝行業代碼＋改制規則",
        "confidence": "high／medium／low／rule／rule_unverified；易主前推算段一律 low",
        "note": "說明", "table_version": "本表版本號"},
    "unmapped.csv": "轉不到公司的段（例：該月這個代號查無公司），不進 sector.csv"}


def _sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def to_csv(rows, cols):
    b = io.StringIO()
    w = csv.DictWriter(b, fieldnames=cols, lineterminator="\n", extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return b.getvalue()


def src_key(name):
    """來源版本排序鍵：2026-10-18 < 2026-10-18_v2 < … < 2026-10-18_v10（字串排序會把 _v10 排在 _v2 前）。"""
    m = re.match(r"^(\d{4}-\d{2}-\d{2})(?:_v(\d+))?$", name)
    return (m.group(1), int(m.group(2) or 1)) if m else ("", 0)


def latest_src(files, prefix, must):
    vs = {p.split("/")[2] for p in files if p.startswith(prefix + "/") and p.endswith("/" + must)}
    vs = [v for v in vs if src_key(v)[0]]
    return max(vs, key=src_key) if vs else None


def published(files, d):
    """已發布的版本號清單（由小到大）。"""
    out = set()
    for p in files:
        m = re.match(r"^%s/v(\d+)/manifest\.json$" % re.escape(d), p)
        if m:
            out.add(int(m.group(1)))
    return sorted(out)


def months_range(a, b):
    y, m = int(a[:4]), int(a[5:7])
    out = []
    while "%04d-%02d" % (y, m) <= b:
        out.append("%04d-%02d" % (y, m))
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


# ── 身份表 ──
def identity_rows(ident, reuse, version):
    notes = defaultdict(list)          # (entity, ticker) → 說明
    for r in reuse:
        t = TI.norm(r["ticker"])
        notes[(r["entity_before"], t)].append("此代號 %s 起易主給 %s" % (r["boundary"], r.get("name_after") or r["entity_after"]))
        notes[(r["entity_after"], t)].append("此代號 %s 前屬 %s" % (r["boundary"], r.get("name_before") or r["entity_before"]))
    rows = []
    for x in ident:
        t = TI.norm(x["ticker"])
        rows.append({"entity_id": x["entity"], "source": "polygon", "symbol": t, "from": x.get("start") or "",
                     "to": x.get("end") or "", "evidence": x.get("source") or "polygon",
                     "note": "；".join(notes.get((x["entity"], t), [])), "table_version": version})
    rows.sort(key=lambda r: (r["symbol"], r["to"] or "9999"))
    return rows


def entity_rows(ident):
    by = defaultdict(list)
    for x in ident:
        by[x["entity"]].append(x)
    out = []
    for e, xs in by.items():
        xs = sorted(xs, key=lambda x: x.get("end") or "9999")
        last = xs[-1]
        act = any(str(x.get("active")).lower() in ("true", "1") and not x.get("end") for x in xs)
        out.append({"entity_id": e, "name": last.get("name") or "", "type": last.get("type") or "",
                    "active": 1 if act else 0})
    out.sort(key=lambda r: r["entity_id"])
    return out


# ── 板塊表 ──
def sector_rows(segs, resolver, today_month, version):
    """按代號的段 → 按公司的段。回 (rows, unmapped, n_empty)。"""
    rows, unm, n_empty = [], [], 0
    for s in segs:
        if not s.get("xl"):
            n_empty += 1
            continue
        t = TI.norm(s["ticker"])
        cur_e = resolver.entity(t, today_month)
        run = None
        pieces = []
        for m in months_range(s["start"], s["end"]):
            e = resolver.entity(t, m)
            if e is None:
                key = ("UNMAPPED",)
            else:
                low = s.get("source") in ("sic_map", "rule") and e != cur_e
                key = (e, "low" if low else (s.get("confidence") or ""), REUSE_NOTE if low else "")
            if run and run["key"] == key:
                run["to"] = m
            else:
                run = {"key": key, "from": m, "to": m}
                pieces.append(run)
        for p in pieces:
            if p["key"] == ("UNMAPPED",):
                unm.append({"ticker": t, "from": p["from"], "to": p["to"], "xl": s["xl"], "source": s.get("source") or "",
                            "reason": "該月這個代號查無公司"})
            else:
                e, conf, note = p["key"]
                rows.append({"entity_id": e, "from": p["from"], "to": p["to"], "xl": s["xl"],
                             "source": s.get("source") or "", "confidence": conf, "note": note, "table_version": version})
    rows.sort(key=lambda r: (r["entity_id"], r["from"]))
    return rows, unm, n_empty


def _manifest(version, source, files, extra=None):
    man = {"version": version, "created": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "source": source, "files": {k: {"sha256": _sha(v), "rows": max(v.count("\n") - 1, 0)}
                                       for k, v in files.items() if k.endswith(".csv")}}
    man["files"].update({k: {"sha256": _sha(v)} for k, v in files.items() if not k.endswith(".csv")})
    man.update(extra or {})
    return man


def plan(files, read, today_month):
    """決定要發布什麼。回 (adds: [(path, text)], summary dict)。不寫任何東西（純計算，方便測試）。"""
    adds, summ = [], {}
    id_src = latest_src(files, TI.OUT_PREFIX, "identity.csv")
    if not id_src:
        raise SystemExit("⛔ 找不到身份表來源")
    id_pub = published(files, ID_DIR)
    last_id_man = json.loads(read("%s/v%d/manifest.json" % (ID_DIR, id_pub[-1]))) if id_pub else None
    ident = list(csv.DictReader(io.StringIO(read("%s/%s/identity.csv" % (TI.OUT_PREFIX, id_src)))))
    if last_id_man and last_id_man.get("mpq_symbols"):
        # 2026-10-28：最新版已含 MPQ（v2 起）→ 這裡不再單獨發「只有 Polygon」的版（會冒出不含 MPQ 的中間版）。
        # Polygon 來源有新版時改用 catalog_rules.yml step identity_refresh（Polygon＋MPQ 一次發版）。
        poly_src = last_id_man.get("polygon_source")
        if not poly_src and last_id_man.get("polygon_base_version"):
            poly_src = json.loads(read("%s/v%d/manifest.json" % (ID_DIR, int(last_id_man["polygon_base_version"])))).get("source")
        if poly_src != "%s/%s" % (TI.OUT_PREFIX, id_src):
            raise SystemExit("⛔ 身份表 v%d 已含 MPQ，Polygon 來源 %s 是新的：請改跑 catalog_rules.yml step identity_refresh"
                             "（一次發版），這裡不單獨發 Polygon 版" % (id_pub[-1], id_src))
        id_ver = id_pub[-1]
        summ["identity"] = "沿用 v%d（已含 MPQ、Polygon 來源未變）" % id_ver
    elif last_id_man and last_id_man.get("source") == "%s/%s" % (TI.OUT_PREFIX, id_src):
        id_ver = id_pub[-1]
        summ["identity"] = "沿用 v%d（來源未變）" % id_ver
    else:
        id_ver = (id_pub[-1] + 1) if id_pub else 1
        reuse = list(csv.DictReader(io.StringIO(read("%s/%s/reuse.csv" % (TI.OUT_PREFIX, id_src)) or "")))
        f = {"identity.csv": to_csv(identity_rows(ident, reuse, id_ver), ID_COLS),
             "entities.csv": to_csv(entity_rows(ident), ENT_COLS)}
        for name in ("renames.csv", "reuse.csv", "problems.csv"):
            txt = read("%s/%s/%s" % (TI.OUT_PREFIX, id_src, name))
            if txt is not None:
                f[name] = re.sub(r"^entity(,|$)", r"entity_id\1", txt, count=1)
        f["schema.json"] = json.dumps(ID_SCHEMA, ensure_ascii=False, indent=1)
        man = _manifest(id_ver, "%s/%s" % (TI.OUT_PREFIX, id_src), f,
                        {"note": "v1 只有 source=polygon；MPQ 代號等 MPQ 數據到了另出新版"})
        base = "%s/v%d" % (ID_DIR, id_ver)
        adds += [("%s/%s" % (base, k), v) for k, v in f.items()]
        adds.append((base + "/manifest.json", json.dumps(man, ensure_ascii=False, indent=1)))   # manifest 最後
        summ["identity"] = "發布 v%d：%d 段、%d 家公司" % (id_ver, len(ident), len({x["entity"] for x in ident}))

    sec_src = latest_src(files, SECTOR_SRC, "sector_history.csv")
    sec_man = json.loads(read("%s/%s/manifest.json" % (SECTOR_SRC, sec_src)) or "{}") if sec_src else {}
    if not sec_src:
        summ["sector"] = "找不到板塊來源，跳過"
    elif sec_man.get("identity_version") != id_src:
        summ["sector"] = "⛔ 板塊最新版 %s 用的身份表是 %s，與 %s 不一致，跳過（先重建板塊）" % (
            sec_src, sec_man.get("identity_version"), id_src)
    else:
        sec_pub = published(files, SEC_DIR)
        last = json.loads(read("%s/v%d/manifest.json" % (SEC_DIR, sec_pub[-1]))) if sec_pub else None
        if last and last.get("source") == "%s/%s" % (SECTOR_SRC, sec_src) and last.get("identity_version") == id_ver:
            summ["sector"] = "沿用 v%d（來源未變）" % sec_pub[-1]
        else:
            sv = (sec_pub[-1] + 1) if sec_pub else 1
            segs = list(csv.DictReader(io.StringIO(read("%s/%s/sector_history.csv" % (SECTOR_SRC, sec_src)))))
            rows, unm, n_empty = sector_rows(segs, TI.Resolver(ident), today_month, sv)
            f = {"sector.csv": to_csv(rows, SEC_COLS), "unmapped.csv": to_csv(unm, UNM_COLS)}
            txt = read("%s/%s/problems.csv" % (SECTOR_SRC, sec_src))
            if txt is not None:
                f["problems.csv"] = txt
            f["schema.json"] = json.dumps(SEC_SCHEMA, ensure_ascii=False, indent=1)
            n_low = sum(1 for r in rows if r["note"] == REUSE_NOTE)
            man = _manifest(sv, "%s/%s" % (SECTOR_SRC, sec_src), f,
                            {"identity_version": id_ver, "skipped_empty_segments": n_empty, "low_reuse_segments": n_low})
            base = "%s/v%d" % (SEC_DIR, sv)
            adds += [("%s/%s" % (base, k), v) for k, v in f.items()]
            adds.append((base + "/manifest.json", json.dumps(man, ensure_ascii=False, indent=1)))
            summ["sector"] = "發布 v%d：%d 段（易主前低信心 %d）｜轉不到公司 %d｜無板塊略過 %d" % (
                sv, len(rows), n_low, len(unm), n_empty)
    return adds, summ


def main_publish(a, hf):
    files = hf.list_files()
    adds, summ = plan(files, hf.read, dt.date.today().strftime("%Y-%m"))
    for p, t in adds:
        if p in files:
            raise SystemExit("⛔ %s 已存在，不覆蓋" % p)
        hf.add(p, t)
    n = hf.flush(a.out, "catalog publish")
    print("身份表：%s" % summ.get("identity"), flush=True)
    print("板塊表：%s" % summ.get("sector"), flush=True)
    print("寫出 %d 個檔（%s）" % (n, "本機試跑 " + a.out if a.dry_run else "私有 dataset"), flush=True)
