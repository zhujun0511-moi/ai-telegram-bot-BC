# -*- coding: utf-8 -*-
"""
identity_mpq.py — 身份表 v2：把 MPQ 代號掛到公司（2026-10-25 Jun 拍板範圍＝規則缺口 A1／A2／A3／A6／A7）

輸入：_catalog/identity 最新版（v1＝Polygon 段）＋ _catalog/_src/mpq_symbols 最新版（本地端交付，只有代號與日期）
輸出：_catalog/identity/v<N>/  — 原 Polygon 列原樣保留（只改 table_version），新增 source=mpq 列；多一欄 asset
規則：
- A1 每段 MPQ 代號按「代號＋日期重疊」對 Polygon 身份段；一段 MPQ 跨 Polygon 易主邊界就切成多段（A3）。
- A2 `X-DELISTED` 去掉後綴用 X 對 Polygon；Polygon 沒有、但 X 只是緊貼 `X-DELISTED` 的殘渣（改名時留下）→ 併入同一家。
- A6 每段帶 asset（stock／etf）；同代號同類別、相隔 ≤31 天的段先併起來（股票與 ETN 同代號並存時 ETF 被切碎，例 FIEE）。
- A7 to 只在 Polygon 有結束日時填（MPQ 末根比 Polygon 結束日晚 ≤10 天算同一段、to 延到末根隔天）；
  其他情況不自動判結束（ADR-053 補注 2）。
- 對不上 Polygon 的段：自成一家 MPQ:{代號}，列進 mpq_problems.csv，不猜。
- X-DELISTED 只對 Polygon「已結束」的段；只剩現役段（例 FB-DELISTED＝Meta 舊史、Polygon 的 FB 是新 ETF）→ 不掛、自成一家＋列問題。
- v1 有「Polygon 標已下市但沒有結束日」的段（to＝0000-00-00）：該代號只有這一段＝照掛、to 空、note 註明；
  該代號還有別段＝邊界不明，自成一家＋polygon_ambiguous。
- 兩個 MPQ 代號同時掛同一家公司（A4 同行情或代號重用，例 FEED）：只列 mpq_problems.csv，不處理（A4 不在本次範圍）。
- 2026-10-30 代號重用切分（ADR-060）：catalog_src/identity_cuts.csv 列的代號，hole_after 以前的 MPQ 段不對 Polygon、自成一家
  MPQ:{代號}:pre-{hole_after}。Polygon 沒有舊公司的記錄，不切的話前後兩家會併成現任公司一條序列。
  2026-10-31 加 mode=whole：X-DELISTED 舊段整段不是現任公司，自成一家 MPQ:X-DELISTED（不用同代號切出前後兩段）。
登記：MongoDB_Standard_v3 §六-D 10-25 條。寫入方＝catalog_rules.py --step identity_mpq。日誌只印筆數。Python 3.9 相容。
"""
import csv
import datetime as dt
import io
import json
import os
import re
from collections import Counter, defaultdict

from catalog_publish import CAT, ENT_COLS, ID_SCHEMA, _sha, published, to_csv

ID_DIR = CAT + "/identity"
SRC_DIR = CAT + "/_src/mpq_symbols"
SUFFIX = "-DELISTED"
TOL = 10            # 天：MPQ 與 Polygon 邊界差這麼多以內算同一段
MERGE_GAP = 31      # 天：同代號同類別的段相隔這麼多以內先併
FAR = "9999-12-31"
NODATE = "0000-00-00"   # 身份表 v1：Polygon 標已下市但沒有 delisted_utc（v1 problems.csv 有列）
ID_COLS_V2 = ["entity_id", "source", "symbol", "from", "to", "evidence", "note", "table_version", "asset"]
PROB_COLS = ["symbol", "asset", "from", "to", "kind", "detail"]
CUTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "catalog_src", "identity_cuts.csv")
CUT_COLS = ["symbol", "asset", "mode", "hole_before", "hole_after", "first_trade", "source", "evidence", "note"]
CUT_SOURCES = ("yahoo", "sec", "company", "exchange", "polygon")

SCHEMA_V2 = dict(ID_SCHEMA)
SCHEMA_V2["schema"] = "catalog_identity_v2"
SCHEMA_V2["identity.csv"] = dict(ID_SCHEMA["identity.csv"])
SCHEMA_V2["identity.csv"].update({
    "source": "polygon｜mpq（v2 起）",
    "symbol": "polygon 列＝Polygon 代號（點換成橫線，例 BRK-B）；mpq 列＝MPQ 原樣寫法（含 -DELISTED、點號，例 BRK.B）",
    "from": "polygon 列同 v1；mpq 列＝該段第一根 bar 的日子（被易主邊界切開時＝邊界日）",
    "to": "結束日（當天已不屬於這段）；mpq 列只在 Polygon 有結束日時填，空＝未判結束（不等於確定現役）",
    "evidence": "polygon｜mpq_overlap＝代號＋日期對上 Polygon 段｜mpq_residue＝改名殘渣併入 X-DELISTED｜mpq_only＝對不上，自成一家｜"
                "mpq_cut＝代號重用，洞前段切出、自成一家（MPQ:{代號}:pre-{洞後首日}，v4 起）",
    "asset": "mpq 列：stock／etf（A6，按段）；polygon 列空"})
SCHEMA_V2["entities.csv"] = dict(ID_SCHEMA["entities.csv"])
SCHEMA_V2["entities.csv"]["type"] = "Polygon 類型（CS、ETF…）；MPQ:{代號} 自成一家者＝mpq_stock／mpq_etf"
SCHEMA_V2["mpq_problems.csv"] = {
    "kind": "no_polygon＝Polygon 身份表沒有可對的段｜polygon_ended＝MPQ 在 Polygon 結束日後還有 >10 天數據（那段自成一家）｜"
            "delisted_polygon_active＝MPQ 標 -DELISTED、Polygon 該代號只剩現役段（不掛，自成一家）｜split_by_polygon＝一條 MPQ 序列被 Polygon 易主邊界切成多家（照切，供人工看）｜polygon_ambiguous＝Polygon 該代號多段且有段沒結束日，分不出邊界（自成一家）｜same_entity_overlap＝兩個 MPQ 代號同時掛同一家｜cut_reuse＝人工清單切出的洞前段（代號重用）",
    "detail": "說明"}
SCHEMA_V2["查詢"] = ("polygon：某代號某天＝該 symbol 中第一筆 to 晚於該天（或 to 空）的行；"
                   "mpq：用 (symbol, asset) 查，同一代號 stock 與 etf 的段可時間重疊")


def _d(s):
    return dt.date.fromisoformat(s)


def _days(a, b):
    """b − a 的天數（字串日期）。"""
    return (_d(b) - _d(a)).days


def _next_day(s):
    return (_d(s) + dt.timedelta(days=1)).isoformat()


def _prev_day(s):
    return (_d(s) - dt.timedelta(days=1)).isoformat()


def norm(t):
    return (t or "").strip().upper().replace(".", "-")


def base_symbol(sym):
    return sym[:-len(SUFFIX)] if sym.upper().endswith(SUFFIX) else sym


def mpq_segments(src_rows):
    """本地端每列 → 段 {symbol, asset, start, end}；同代號同類別相隔 ≤MERGE_GAP 天先併。回 (段, 併掉的段數)。"""
    by = defaultdict(list)
    for r in src_rows:
        ds = [x for x in (r.get("d_first"), r.get("m5_first")) if x]
        de = [x for x in (r.get("d_last"), r.get("m5_last")) if x]
        if not ds or not de:
            continue
        by[(r["mpq_symbol"], r["asset"])].append({"symbol": r["mpq_symbol"], "asset": r["asset"],
                                                  "start": min(ds), "end": max(de)})
    out, merged = [], 0
    for _, xs in sorted(by.items()):
        xs.sort(key=lambda x: x["start"])
        cur = dict(xs[0])
        for x in xs[1:]:
            if _days(cur["end"], x["start"]) <= MERGE_GAP:
                cur["end"] = max(cur["end"], x["end"])
                merged += 1
            else:
                out.append(cur)
                cur = dict(x)
        out.append(cur)
    return out, merged


def parse_cuts(text):
    """代號重用切分清單 → {(symbol, asset): 列}。任何不對就整次失敗。"""
    rd = csv.DictReader(io.StringIO((text or "").lstrip("﻿")))
    if rd.fieldnames != CUT_COLS:
        raise SystemExit("⛔ identity_cuts 欄位不對：%s" % rd.fieldnames)
    out = {}
    for r in rd:
        k = (r["symbol"], r["asset"])
        if r["mode"] not in ("hole", "whole"):
            raise SystemExit("⛔ identity_cuts %s mode 不對：%s" % (k, r["mode"]))
        for f in ("hole_before", "hole_after", "first_trade") if r["mode"] == "hole" else ("first_trade",):
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", r[f] or ""):
                raise SystemExit("⛔ identity_cuts %s %s 日期不對" % (k, f))
        if r["asset"] not in ("stock", "etf") or (r["mode"] == "hole" and r["hole_before"] >= r["hole_after"]):
            raise SystemExit("⛔ identity_cuts %s 類別或洞的日期不對" % (k,))
        if r["mode"] == "whole" and (r["hole_before"] or r["hole_after"]):
            raise SystemExit("⛔ identity_cuts %s 整段切出不該有洞的日期" % (k,))
        if not r["source"] or any(x not in CUT_SOURCES for x in r["source"].split("+")) or not (r["evidence"] or "").strip():
            raise SystemExit("⛔ identity_cuts %s 缺來源或證據" % (k,))
        if k in out:
            raise SystemExit("⛔ identity_cuts 重複：%s" % (k,))
        out[k] = r
    return out


def apply_cuts(segs, cuts):
    """把段切在 hole_after：之前的標 cut（自成一家）。每列清單都要切得出前段和後段，否則整次失敗。回 (段, 切出的段數)。"""
    out, n, pre, post = [], 0, set(), set()
    for s in segs:
        k = (s["symbol"], s["asset"])
        c = cuts.get(k)
        if c is None:
            out.append(s)
        elif c["mode"] == "whole":
            out.append(dict(s, cut=c))
            pre.add(k)
            post.add(k)            # 整段切出：舊公司只有這些段，不要求有後段
            n += 1
        elif s["end"] < c["hole_after"]:
            out.append(dict(s, cut=c))
            pre.add(k)
            n += 1
        elif s["start"] >= c["hole_after"]:
            out.append(s)
            post.add(k)
        else:
            out.append(dict(s, end=_prev_day(c["hole_after"]), cut=c))
            out.append(dict(s, start=c["hole_after"]))
            pre.add(k)
            post.add(k)
            n += 1
    bad = sorted(k for k in cuts if k not in pre or k not in post)
    if bad:
        raise SystemExit("⛔ identity_cuts %d 列切不出前後兩段：%s" % (len(bad), bad[:10]))
    return out, n


def load_cuts():
    """讀清單：回 (內容, 解析結果)；沒有檔＝(None, {})。"""
    if not os.path.exists(CUTS_FILE):
        return None, {}
    with open(CUTS_FILE, encoding="utf-8") as fh:
        txt = fh.read()
    return txt, parse_cuts(txt)


class PolyIndex:
    """Polygon 段：代號 → [{entity, from, to}]，from 空＝最早、to 空＝至今。"""

    def __init__(self, id_rows):
        self.by = defaultdict(list)
        for r in id_rows:
            if (r.get("source") or "polygon") == "polygon":
                to, fr = r.get("to") or "", r.get("from") or ""
                self.by[norm(r["symbol"])].append({"entity": r["entity_id"],
                                                   "from": "0000-01-01" if fr in ("", NODATE) else fr,
                                                   "to": "" if to == NODATE else to, "nodate": to == NODATE})
        for v in self.by.values():
            v.sort(key=lambda s: s["to"] or FAR)

    def pieces(self, sym, start, end):
        """[start, end]（含）按 Polygon 段切開 → [{from, last, seg}]，seg=None＝沒有段蓋到。
        該代號有「已下市但沒有結束日」的段、又不只一段 → 邊界不明，整段回 ambiguous（不猜）。"""
        segs = self.by.get(norm(sym), [])
        if len(segs) > 1 and any(s["nodate"] for s in segs):
            return [{"from": start, "last": end, "seg": None, "ambiguous": True}]
        out, cur = [], start
        for s in segs:
            s_last = _prev_day(s["to"]) if s["to"] else FAR
            lo, hi = max(cur, s["from"]), min(end, s_last)
            if lo > hi:
                continue
            if lo > cur:
                out.append({"from": cur, "last": _prev_day(lo), "seg": None})
            out.append({"from": lo, "last": hi, "seg": s})
            cur = _next_day(hi) if hi < FAR else FAR
            if cur > end:
                break
        if cur <= end:
            out.append({"from": cur, "last": end, "seg": None})
        return out


def _absorb(ps):
    """邊界容差：沒對上的頭尾 ≤TOL 天、或被切出 ≤TOL 天的小段，併進相鄰的段（前一段優先）。"""
    changed = True
    while changed and len(ps) > 1:
        changed = False
        for i, p in enumerate(ps):
            if _days(p["from"], p["last"]) + 1 > TOL:
                continue
            if i > 0:
                ps[i - 1]["last"] = p["last"]
            else:
                ps[1]["from"] = p["from"]
            del ps[i]
            changed = True
            break
    return ps


def build(id_rows, src_rows, version, cal_end=None, cuts=None):
    """純函數：Polygon 身份段＋MPQ 代號摘要 → (mpq 身份列, 新增 entities, problems, stats)。"""
    idx = PolyIndex(id_rows)
    segs, merged = mpq_segments(src_rows)
    cal_end = cal_end or max(s["end"] for s in segs)
    rows, probs = [], []
    stats = Counter(merged_segments=merged, mpq_segments=len(segs))
    pending_residue = []
    new_ents = {}
    if cuts:
        segs, stats["cut_segments"] = apply_cuts(segs, cuts)
    for s in segs:
        sym, asset = s["symbol"], s["asset"]
        if s.get("cut"):                   # 代號重用：洞前是別家，不對 Polygon（Polygon 沒有舊公司的記錄，對了就併進現任）
            c = s["cut"]
            eid = "MPQ:" + sym if c["mode"] == "whole" else "MPQ:%s:pre-%s" % (sym, c["hole_after"])
            rows.append({"entity_id": eid, "source": "mpq", "symbol": sym, "from": s["start"], "to": c["hole_after"] or "",
                         "evidence": "mpq_cut", "note": c["evidence"], "table_version": version, "asset": asset,
                         "_last": s["end"]})
            probs.append({"symbol": sym, "asset": asset, "from": s["start"], "to": c["hole_after"] or "", "kind": "cut_reuse",
                          "detail": c["evidence"] + ("；" + c["note"] if c["note"] else "")})
            stats["cut_reuse"] += 1
            new_ents[eid] = {"entity_id": eid, "name": "", "type": "mpq_" + asset, "active": 0}
            continue
        delisted = sym.upper().endswith(SUFFIX)
        ps = _absorb(idx.pieces(base_symbol(sym), s["start"], s["end"]))
        for k, p in enumerate(ps):
            last = k == len(ps) - 1
            r = {"entity_id": "", "source": "mpq", "symbol": sym, "from": p["from"], "to": "", "evidence": "",
                 "note": "", "table_version": version, "asset": asset, "_last": p["last"]}
            if p["seg"] is not None and delisted and not p["seg"]["to"] and not p["seg"]["nodate"]:
                # X-DELISTED 只對已結束的段：Polygon 只剩現役段＝多半是代號被新公司接手、舊公司的段沒留下
                # （例 FB-DELISTED＝Meta 2012～2022，Polygon 的 FB 是 2025 起的新 ETF）→ 不掛，自成一家＋列問題
                r["evidence"] = "mpq_only"
                pending_residue.append((r, "delisted_polygon_active", p["seg"]))
            elif p["seg"] is not None:
                r["entity_id"], r["evidence"] = p["seg"]["entity"], "mpq_overlap"
                if p["seg"]["to"] and p["last"] >= p["seg"]["to"]:      # 容差內併進來的尾巴
                    r["to"] = _next_day(p["last"])
                    r["note"] = "MPQ 末根比 Polygon 結束日 %s 晚 %d 天" % (p["seg"]["to"], _days(p["seg"]["to"], p["last"]))
                elif p["seg"]["to"]:
                    r["to"] = p["seg"]["to"]
                elif p["seg"]["nodate"]:
                    r["note"] = "Polygon 標已下市但沒有結束日"
                    stats["polygon_nodate"] += 1
                stats["overlap"] += 1
            else:
                r["evidence"] = "mpq_only"
                prev = ps[k - 1]["seg"] if k > 0 else None
                kind = ("polygon_ambiguous" if p.get("ambiguous") else
                        "polygon_ended" if prev is not None else "no_polygon")
                pending_residue.append((r, kind, prev))
            if not last:
                r["to"] = ps[k + 1]["from"]
            rows.append(r)
        ents = [p["seg"]["entity"] for p in ps if p["seg"] is not None]
        if len(set(ents)) > 1:                 # 一條連續的 MPQ 序列被 Polygon 易主邊界切開：照切，列出來給人看
            probs.append({"symbol": sym, "asset": asset, "from": s["start"], "to": s["end"], "kind": "split_by_polygon",
                          "detail": "按 Polygon 邊界切成 %d 段：%s" % (len(ps), "、".join(
                              "%s 起 %s" % (p["from"], p["seg"]["entity"] if p["seg"] else "無") for p in ps))})
            stats["split_by_polygon"] += 1
    # A2 殘渣：X 對不上 Polygon、X-DELISTED 存在且 X 緊貼（或落在）X-DELISTED 期間 → 併入 X-DELISTED 那家
    by_sym = defaultdict(list)
    for r in rows:
        by_sym[(r["symbol"].upper(), r["asset"])].append(r)
    pending_residue.sort(key=lambda x: not x[0]["symbol"].upper().endswith(SUFFIX))   # X-DELISTED 先自成一家，殘渣才併得上
    for r, kind, prev in pending_residue:
        xd = None
        if not r["symbol"].upper().endswith(SUFFIX):
            cand = by_sym.get(((r["symbol"] + SUFFIX).upper(), r["asset"]), [])
            for c in cand:
                if c["entity_id"] and _days(r["_last"], c["from"]) <= TOL and r["from"] <= c["_last"]:
                    xd = c
                    break
        if xd is not None:
            r["entity_id"], r["evidence"] = xd["entity_id"], "mpq_residue"
            r["note"] = "改名殘渣：併入 %s" % xd["symbol"]
            stats["residue"] += 1
            continue
        stats[kind] += 1
        r["entity_id"] = "MPQ:" + r["symbol"]
        probs.append({"symbol": r["symbol"], "asset": r["asset"], "from": r["from"], "to": r["to"], "kind": kind,
                      "detail": {"no_polygon": "Polygon 身份表沒有可對的段",
                                 "polygon_ambiguous": "Polygon 該代號有多段、其中有已下市但沒有結束日的段，分不出邊界",
                                 "delisted_polygon_active": "MPQ 標下市（末根 %s），Polygon 該代號只剩現役段 %s：多半是代號已被新公司接手、"
                                                            "舊公司的段沒留下（或 Polygon 落後），不掛" % (
                                     r["_last"], prev["entity"] if prev else "")}.get(kind)
                      or "Polygon 結束日 %s 後 MPQ 還有數據到 %s" % (prev["to"], r["_last"])})
        if r["entity_id"] not in new_ents:
            act = 0 if r["symbol"].upper().endswith(SUFFIX) or _days(r["_last"], cal_end) > TOL else 1
            new_ents[r["entity_id"]] = {"entity_id": r["entity_id"], "name": "", "type": "mpq_" + r["asset"],
                                        "active": act}
    # 兩個 MPQ 代號同時掛同一家（A4／代號重用，只報告）
    by_e = defaultdict(list)
    for r in rows:
        by_e[r["entity_id"]].append(r)
    seen = set()
    for e, xs in by_e.items():
        xs = sorted(xs, key=lambda r: r["from"])
        for i, a in enumerate(xs):
            for b in xs[i + 1:]:
                if b["from"] > a["_last"]:
                    break
                if a["symbol"] == b["symbol"]:
                    continue
                key = tuple(sorted((a["symbol"], b["symbol"])))
                if key in seen:
                    continue
                seen.add(key)
                lo, hi = b["from"], min(a["_last"], b["_last"])
                probs.append({"symbol": key[0], "asset": a["asset"], "from": lo, "to": hi, "kind": "same_entity_overlap",
                              "detail": "與 %s 同時掛 %s（%d 天重疊）" % (key[1], e, _days(lo, hi) + 1)})
                stats["same_entity_overlap"] += 1
    stats["delisted_polygon_active"] = sum(1 for p in probs if p["kind"] == "delisted_polygon_active")
    for r in rows:
        r.pop("_last", None)
    rows.sort(key=lambda r: (r["symbol"], r["asset"], r["from"]))
    probs.sort(key=lambda p: (p["kind"], p["symbol"], p["from"]))
    stats["mpq_rows"] = len(rows)
    stats["mpq_symbols"] = len({r["symbol"] for r in rows})
    stats["mpq_only_entities"] = len(new_ents)
    return rows, sorted(new_ents.values(), key=lambda e: e["entity_id"]), probs, dict(stats)


def _latest(files, read, d):
    pub = published(files, d)
    if not pub:
        return 0, None
    return pub[-1], json.loads(read("%s/v%d/manifest.json" % (d, pub[-1])))


def _mpq_input(files, read):
    """MPQ 代號摘要最新版：回 (版本, manifest, 內容)，並核 sha256。"""
    src_v, src_man = _latest(files, read, SRC_DIR)
    if not src_v:
        raise SystemExit("⛔ 找不到 %s 輸入" % SRC_DIR)
    src_txt = read("%s/v%d/mpq_symbols.csv" % (SRC_DIR, src_v))
    if _sha(src_txt) != src_man["files"]["mpq_symbols.csv"]["sha256"]:
        raise SystemExit("⛔ mpq_symbols v%d sha256 與 manifest 不一致" % src_v)
    return src_v, src_man, src_txt


def plan(files, read):
    """決定要發布什麼：回 (adds, 摘要)。純計算，不寫東西。"""
    src_v, src_man, src_txt = _mpq_input(files, read)
    cuts_txt, cuts = load_cuts()
    id_v, id_man = _latest(files, read, ID_DIR)
    if not id_v:
        raise SystemExit("⛔ 找不到 _catalog/identity 已發布版本")
    if id_man.get("mpq_symbols"):            # 最新版已含 MPQ：基底取它所根據的 Polygon 版
        if id_man["mpq_symbols"] == "%s/v%d" % (SRC_DIR, src_v) and id_man.get("cuts_sha256") == (_sha(cuts_txt) if cuts_txt else None):
            return [], "沿用 v%d（輸入未變）" % id_v
        base_v = int(id_man["polygon_base_version"])
    else:
        base_v = id_v
    base = "%s/v%d" % (ID_DIR, base_v)
    id_rows = list(csv.DictReader(io.StringIO(read(base + "/identity.csv"))))
    poly = [r for r in id_rows if (r.get("source") or "polygon") == "polygon"]
    ver = id_v + 1
    mrows, ments, probs, stats = build(poly, list(csv.DictReader(io.StringIO(src_txt, newline=""))), ver,
                                       (src_man.get("calendar") or [None, None])[1], cuts)
    for r in poly:
        r["table_version"] = ver
        r.setdefault("asset", "")
    # 基底若是 identity_refresh 發的版（本身已含 MPQ），只取 Polygon 的公司，MPQ:{代號} 由本次 build 重新產生
    ents = [e for e in csv.DictReader(io.StringIO(read(base + "/entities.csv")))
            if not (e.get("entity_id") or "").startswith("MPQ:")]
    f = {"identity.csv": to_csv(poly + mrows, ID_COLS_V2),
         "entities.csv": to_csv(ents + ments, ENT_COLS)}
    for name in ("renames.csv", "reuse.csv", "problems.csv"):
        txt = read("%s/%s" % (base, name))
        if txt is not None:
            f[name] = txt
    f["mpq_problems.csv"] = to_csv(probs, PROB_COLS)
    f["schema.json"] = json.dumps(SCHEMA_V2, ensure_ascii=False, indent=1)
    man = {"version": ver, "created": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "source": "%s ＋ %s/v%d" % (base, SRC_DIR, src_v),
           "files": {k: ({"sha256": _sha(v), "rows": max(v.count("\n") - 1, 0)} if k.endswith(".csv")
                         else {"sha256": _sha(v)}) for k, v in f.items()},
           "polygon_base_version": base_v, "mpq_symbols": "%s/v%d" % (SRC_DIR, src_v),
           "mpq_symbols_sha256": src_man["files"]["mpq_symbols.csv"]["sha256"],
           "rules": {"tol_days": TOL, "merge_gap_days": MERGE_GAP}, "stats": stats,
           "note": "v2 起含 source=mpq（A1／A2／A3／A6／A7）；A4 去重、A5 改名拼接不在本版"}
    if cuts_txt:
        man.update(cuts_sha256=_sha(cuts_txt), cuts_rows=len(cuts))
    d = "%s/v%d" % (ID_DIR, ver)
    adds = [("%s/%s" % (d, k), v) for k, v in f.items()]
    adds.append((d + "/manifest.json", json.dumps(man, ensure_ascii=False, indent=1)))   # manifest 最後
    summ = ("發布 v%d：MPQ %d 代號 → %d 段（對上 Polygon %d、改名殘渣 %d、自成一家 %d 家）｜問題：無 Polygon %d、"
            "Polygon 已結束 %d、邊界不明 %d、標下市但 Polygon 只剩現役 %d、被 Polygon 切開 %d、同家重疊 %d｜先併段 %d｜基底 Polygon v%d") % (
        ver, stats["mpq_symbols"], stats["mpq_rows"], stats.get("overlap", 0), stats.get("residue", 0),
        stats["mpq_only_entities"], stats.get("no_polygon", 0), stats.get("polygon_ended", 0),
        stats.get("polygon_ambiguous", 0), stats.get("delisted_polygon_active", 0), stats.get("split_by_polygon", 0), stats.get("same_entity_overlap", 0), stats["merged_segments"], base_v)
    if cuts_txt:
        summ += "｜代號重用切分 %d 支、切出 %d 段" % (len(cuts), stats.get("cut_reuse", 0))
    return adds, summ


# ── identity_refresh（2026-10-28 工單通道）：Polygon 重建＋併 MPQ 一次發一版 ──
# 起因：工單回覆「改名／下市」必須附已發布的身份表版本，新發生的改名要先重建 Polygon 段（ticker_identity.py）。
# 舊流程是 catalog_publish（只發 Polygon 版）→ identity_mpq（再併 MPQ），中間會冒出一版不含 MPQ 的身份表、
# 本地端讀到會錯。這裡直接用最新的 swing_ai/identity 來源＋最新 MPQ 摘要，一次產出含兩者的新版。

def poly_source_of(read, id_man):
    """某已發布身份表版本的 Polygon 段來自哪份 swing_ai/identity 來源。"""
    if not id_man:
        return None
    if id_man.get("polygon_source"):
        return id_man["polygon_source"]
    if id_man.get("polygon_base_version"):
        base = json.loads(read("%s/v%d/manifest.json" % (ID_DIR, int(id_man["polygon_base_version"]))))
        return base.get("source")
    return id_man.get("source")


def _poly_keys(rows):
    return {(r.get("entity_id"), r.get("symbol"), r.get("from") or "", r.get("to") or "")
            for r in rows if (r.get("source") or "polygon") == "polygon"}


def plan_refresh(files, read):
    """決定要發布什麼：回 (adds, 摘要)。純計算，不寫東西。"""
    import ticker_identity as TI
    from catalog_publish import entity_rows, identity_rows, latest_src
    id_src = latest_src(files, TI.OUT_PREFIX, "identity.csv")
    if not id_src:
        raise SystemExit("⛔ 找不到 %s 來源（先跑 ticker_identity.py）" % TI.OUT_PREFIX)
    poly_source = "%s/%s" % (TI.OUT_PREFIX, id_src)
    src_v, src_man, src_txt = _mpq_input(files, read)
    cuts_txt, cuts = load_cuts()
    id_v, id_man = _latest(files, read, ID_DIR)
    if (id_man and poly_source_of(read, id_man) == poly_source and id_man.get("mpq_symbols") == "%s/v%d" % (SRC_DIR, src_v)
            and id_man.get("cuts_sha256") == (_sha(cuts_txt) if cuts_txt else None)):
        return [], "沿用 v%d（Polygon 來源 %s 與 MPQ 輸入都未變）" % (id_v, id_src)
    ver = id_v + 1
    ident = list(csv.DictReader(io.StringIO(read("%s/identity.csv" % poly_source))))
    reuse = list(csv.DictReader(io.StringIO(read("%s/reuse.csv" % poly_source) or "")))
    poly = identity_rows(ident, reuse, ver)
    for r in poly:
        r["asset"] = ""
    mrows, ments, probs, stats = build(poly, list(csv.DictReader(io.StringIO(src_txt, newline=""))), ver,
                                       (src_man.get("calendar") or [None, None])[1], cuts)
    f = {"identity.csv": to_csv(poly + mrows, ID_COLS_V2),
         "entities.csv": to_csv(entity_rows(ident) + ments, ENT_COLS)}
    for name in ("renames.csv", "reuse.csv", "problems.csv"):
        txt = read("%s/%s" % (poly_source, name))
        if txt is not None:
            f[name] = re.sub(r"^entity(,|$)", r"entity_id\1", txt, count=1)
    f["mpq_problems.csv"] = to_csv(probs, PROB_COLS)
    f["schema.json"] = json.dumps(SCHEMA_V2, ensure_ascii=False, indent=1)
    diff = {}
    if id_v:
        prev = list(csv.DictReader(io.StringIO(read("%s/v%d/identity.csv" % (ID_DIR, id_v)))))
        a, b = _poly_keys(prev), _poly_keys(poly)
        diff = {"vs_version": id_v, "polygon_rows_added": len(b - a), "polygon_rows_removed": len(a - b),
                "mpq_rows_prev": sum(1 for r in prev if r.get("source") == "mpq"), "mpq_rows_now": len(mrows)}
    man = {"version": ver, "created": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "source": "%s ＋ %s/v%d" % (poly_source, SRC_DIR, src_v),
           "files": {k: ({"sha256": _sha(v), "rows": max(v.count("\n") - 1, 0)} if k.endswith(".csv")
                         else {"sha256": _sha(v)}) for k, v in f.items()},
           "polygon_source": poly_source, "polygon_base_version": ver,
           "mpq_symbols": "%s/v%d" % (SRC_DIR, src_v),
           "mpq_symbols_sha256": src_man["files"]["mpq_symbols.csv"]["sha256"],
           "rules": {"tol_days": TOL, "merge_gap_days": MERGE_GAP}, "stats": stats, "diff_vs_prev": diff,
           "note": "identity_refresh（2026-10-28）：Polygon 重建＋併 MPQ 一次發版，不產生不含 MPQ 的中間版；規則同 v2"}
    if cuts_txt:
        man.update(cuts_sha256=_sha(cuts_txt), cuts_rows=len(cuts))
    d = "%s/v%d" % (ID_DIR, ver)
    adds = [("%s/%s" % (d, k), v) for k, v in f.items()]
    adds.append((d + "/manifest.json", json.dumps(man, ensure_ascii=False, indent=1)))   # manifest 最後
    summ = "發布 v%d：Polygon 來源 %s（%d 段）＋MPQ v%d（%d 段）｜與 v%s 比 Polygon 段 +%s／-%s、MPQ 段 %s→%d" % (
        ver, id_src, len(poly), src_v, len(mrows), diff.get("vs_version", "-"), diff.get("polygon_rows_added", "-"),
        diff.get("polygon_rows_removed", "-"), diff.get("mpq_rows_prev", "-"), len(mrows))
    return adds, summ
