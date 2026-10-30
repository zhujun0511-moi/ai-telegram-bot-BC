# -*- coding: utf-8 -*-
"""
catalog_rules.py — 發布拆股事件表、交易日曆兩種規則表正本（2026-10-19 Jun 確認範圍；回應本地端規則缺口 A8／A9／C1）：
  Polygon /v3/reference/splits（2000 起全市場）＋ _catalog/identity 最新版 → _catalog/splits/v<N>/
  本地端 NYSE 日曆 CSV（2000～2026）＋ 規則推算（2027 起）      → _catalog/calendar/v<N>/
規矩（MongoDB_Standard §六-D 10-19 登記）：
- 一次提交含數據＋manifest（manifest 最後）；v<N> 已存在不覆蓋、取下一號；內容與上一版相同就不發新版。
- 拆股按「代號＋日期」對到公司（防代號重用，例 SUNW 2019 那筆屬 Sunworks 不屬昇陽）；對不上的進 unmapped.csv。
- 比例不是常見拆法（例 1.272）＝疑似分拆，標 suspect_spinoff、低信心，預設不用於價格還原（A9）。
- 日曆規則推算 2000～2026 與 CSV 交叉核對，差異只寫 crosscheck.csv，不改 CSV 內容。
- 公開日誌只印數量。
2026-10-26 加 --step series_candidates：A4／A5 序列正本表候選（邏輯在 series_mpq.py）。
2026-10-26 拆股表 v3 起來源守衛：和最早那版逐筆比，Polygon 刪除或改比例的保留舊值、列 changes.csv，新增照收。
2026-10-25 拆股表 v2：身份表含 mpq 列時，拆股先按「該日 MPQ 數據屬哪家」對公司（A8 重掛），多一欄 mpq_symbols。
2026-10-25 加 --step identity_mpq：身份表 v2 含 MPQ 代號（邏輯在 identity_mpq.py；輸入 _catalog/_src/mpq_symbols，不連 Polygon）。
2026-10-28 加 --step identity_refresh：身份表重建（先跑 ticker_identity.py；邏輯在 identity_mpq.plan_refresh；工單通道用）。
2026-10-29 拆股表 v4：合併人工查證清單 catalog_src/split_events.csv（add／override／reject，每列附公開來源；Jun 10-29 拍板三點，
  回應本地端暫扣序列需求 ADR-055）；新類型 spinoff／special_dividend／stock_dividend、confidence 加 rejected。
2026-10-29 日曆 v2 格式（Jun 拍板方案 A，回應本地端首輪體檢 ADR-058）：本地端 CSV 不改；合併 catalog_src/calendar_fixes.csv
  （只補 CSV 沒有的日子，source=fix）＋發布 catalog_src/halts.csv（停盤／延後開盤／已知數據缺口）。
用法：python catalog_rules.py --step splits|calendar|identity_mpq|series_candidates|identity_refresh [--dry-run] [--out _catalog_out] [--rule-years 2027]
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
from collections import defaultdict
from fractions import Fraction

from courier import keys

from catalog_publish import CAT, _sha, to_csv, published

SPLIT_DIR = CAT + "/splits"
CAL_DIR = CAT + "/calendar"
ID_DIR = CAT + "/identity"
CAL_SRC_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "catalog_src",
                            "nyse_holidays_and_special_schedules_2000_2026.csv")
SPLIT_EVENTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "catalog_src", "split_events.csv")
CAL_FIXES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "catalog_src", "calendar_fixes.csv")
CAL_HALTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "catalog_src", "halts.csv")
HIGH_FROM = "2003-01-01"          # Polygon 拆股 2003 起完整；2000～2002 明顯不全（10-17 實測）
FAR = "9999-12-31"
REPO = keys.env_strip("HF_REPO_ID") or "zhujun0511-AI/ai-telegram-bot-dataset"   # 10-28 門禁修正：環境變數經門讀（同 raw_writer）

SPLIT_COLS = ["entity_id", "symbol", "ex_date", "type", "ratio", "split_from", "split_to", "source",
              "evidence", "confidence", "note", "table_version"]
SPLIT_UNM_COLS = ["symbol", "ex_date", "split_from", "split_to", "reason"]
CAL_COLS = ["date", "type", "open", "close", "reason", "source", "table_version"]
XC_COLS = ["date", "csv_type", "rule_type", "note"]
FIX_COLS = ["date", "type", "open", "close", "reason", "source", "evidence"]
HALT_SRC_COLS = ["date", "start", "end", "scope", "kind", "source", "note"]
HALT_COLS = HALT_SRC_COLS + ["table_version"]
FIX_SOURCES = ("exchange", "sec", "press")
HALT_SOURCES = FIX_SOURCES + ("local_data",)
HALT_SCOPES = ("all", "nyse_listed", "nasdaq_listed")
HALT_KINDS = ("halt", "delayed_open", "feed_gap")

SPLIT_SCHEMA = {
    "schema": "catalog_splits_v1",
    "splits.csv": {
        "entity_id": "公司（股份）編號，與 manifest.identity_version 那版身份表一致",
        "symbol": "Polygon 記錄上的代號（點形，例 BRK.B）",
        "ex_date": "除權日 YYYY-MM-DD（Polygon execution_date）",
        "type": "split＝拆股（ratio>1）｜reverse_split＝反向合股（ratio<1）｜suspect_spinoff＝比例不是常見拆法，疑似分拆或資料錯",
        "ratio": "split_to÷split_from＝每 1 舊股變幾股；還原實際價：除權日前的調整價 × ratio",
        "split_from": "Polygon 原值", "split_to": "Polygon 原值",
        "source": "polygon", "evidence": "Polygon 記錄 id",
        "confidence": "high＝2003 起且常見拆法｜low＝2000～2002（Polygon 不全）或 suspect_spinoff",
        "note": "說明", "table_version": "本表版本號"},
    "unmapped.csv": "代號＋日期在身份表找不到公司的記錄（不進 splits.csv）",
    "限制": ["2000～2002 Polygon 記錄明顯不全（2000＝5、2001＝8、2002＝78 筆），缺的拆股本表沒有",
           "Polygon 不單獨標分拆；suspect_spinoff 只是按比例推測，預設不用於價格還原",
           "同一代號同一天同比例的重複記錄只留一筆"]}
SPLIT_SCHEMA_V2 = json.loads(json.dumps(SPLIT_SCHEMA))
SPLIT_SCHEMA_V2["schema"] = "catalog_splits_v2"
SPLIT_SCHEMA_V2["splits.csv"]["entity_id"] = ("公司（股份）編號，與 manifest.identity_version 那版身份表一致；"
                                              "先看該日 MPQ 數據屬哪家（X 或 X-DELISTED），MPQ 沒有才用 Polygon 段")
SPLIT_SCHEMA_V2["splits.csv"]["mpq_symbols"] = "該日該公司有數據的 MPQ 代號（「|」分隔）；空＝MPQ 該日沒有這家的數據"
SPLIT_SCHEMA_V2["splits.csv"]["confidence"] += "｜與 Polygon 段歸屬不同（照 MPQ）或 MPQ 同代號多家＝low"
SPLIT_SCHEMA_V2["限制"] = SPLIT_SCHEMA["限制"] + [
    "v2 起按身份表 v2 重掛；ratio 是否解釋得了 MPQ 調整價，由本地端照「倍數核對規格」核，核不過進 hold",
    "v3 起來源守衛：和各（代號, 日）最早出現那版比，Polygon 刪除或改比例的不自動接受（保留舊值、note 註明、列 changes.csv）"]
SPLIT_SCHEMA_V2["changes.csv"] = {
    "kind": "deleted＝Polygon 這次沒有｜ratio_changed＝同代號同日比例不同",
    "kept_*": "保留下來的舊值與它最早出現的版本", "new_*": "Polygon 這次的值",
    "status": "kept_previous＝已保留舊值，待實際價仲裁（仲裁後的覆寫方式另議）"}
CAL_SCHEMA = {
    "schema": "catalog_calendar_v1",
    "calendar.csv": {
        "date": "YYYY-MM-DD", "type": "full_closure＝全日休市｜early_close＝提早收盤｜delayed_open＝延遲開盤",
        "open": "開盤 HH:MM 美東、不帶時區（休市日空）", "close": "收盤 HH:MM 美東（休市日空）",
        "reason": "原因", "source": "local_csv＝本地端日曆 CSV｜rule＝規則推算", "table_version": "本表版本號"},
    "未列的日子": "週一至週五未列＝09:30–16:00 正常交易日；週六日一律休市（不列）",
    "crosscheck.csv": "2000～2026 規則推算與 CSV 的差異（特殊休市、規則演變等），只報告不改"}
CAL_SCHEMA_V2 = json.loads(json.dumps(CAL_SCHEMA))
CAL_SCHEMA_V2["schema"] = "catalog_calendar_v2"
CAL_SCHEMA_V2["calendar.csv"]["source"] += "｜fix＝數據中心補正（BC catalog_src/calendar_fixes.csv，每列附公開來源；只補 CSV 沒有的日子）"
CAL_SCHEMA_V2["crosscheck.csv"] += "（不含 fix 列）"
CAL_SCHEMA_V2["halts.csv"] = {
    "date": "YYYY-MM-DD", "start": "開始 HH:MM 美東、不帶時區", "end": "結束 HH:MM 美東（start < end）",
    "scope": "all＝全市場｜nyse_listed＝CTA 揭示（NYSE、NYSE Arca、NYSE American 上市）｜nasdaq_listed＝UTP 揭示（Nasdaq 上市）",
    "kind": "halt＝停盤／熔斷｜delayed_open＝延後開盤｜feed_gap＝市場有交易但行情源或 MPQ 缺數據（補不回）",
    "source": "exchange｜sec｜press｜local_data（「+」併列；local_data＝只有本地缺根佐證、查無公開記錄）",
    "note": "說明與證據", "table_version": "本表版本號",
    "用法": "與時段重疊的 15 分鐘缺根＝容忍，不判不合格；feed_gap＝已知缺口；時段＝報導時間或本地實際缺的時段"}


# ── 共用 ──
def norm(t):
    return (t or "").strip().upper().replace(".", "-")


def content_sha(rows, cols):
    """不含 table_version 的內容指紋：用來判斷「內容和上一版相同就不發新版」。"""
    return _sha(to_csv(rows, [c for c in cols if c != "table_version"]))


def manifest(version, source, files, extra=None):
    man = {"version": version, "created": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "source": source,
           "files": {k: ({"sha256": _sha(v), "rows": max(v.count("\n") - 1, 0)} if k.endswith(".csv")
                         else {"sha256": _sha(v)}) for k, v in files.items()}}
    man.update(extra or {})
    return man


def last_manifest(files, read, d):
    pub = published(files, d)
    if not pub:
        return 0, None
    return pub[-1], json.loads(read("%s/v%d/manifest.json" % (d, pub[-1])))


def emit(d, ver, f, man):
    base = "%s/v%d" % (d, ver)
    adds = [("%s/%s" % (base, k), v) for k, v in f.items()]
    adds.append((base + "/manifest.json", json.dumps(man, ensure_ascii=False, indent=1)))   # manifest 最後
    return adds


# ── 拆股表 ──
def classify(split_from, split_to, ex_date):
    """回 (type, ratio 字串, confidence, note)。常見拆法＝兩邊都是整數、約分後小的那邊 ≤10（2:1、3:2、1:50…）。"""
    try:
        a, b = Fraction(str(split_from)), Fraction(str(split_to))
    except (ValueError, ZeroDivisionError):
        return "suspect_spinoff", "", "low", "比例無法解析"
    if a <= 0 or b <= 0:
        return "suspect_spinoff", "", "low", "比例非正數"
    q = b / a
    ratio = ("%.10f" % float(q)).rstrip("0").rstrip(".")
    clean = a.denominator == 1 and b.denominator == 1 and min(q.numerator, q.denominator) <= 10 and q != 1
    if not clean:
        return "suspect_spinoff", ratio, "low", "比例不是常見拆法，疑似分拆或資料錯；預設不用於價格還原"
    typ = "split" if q > 1 else "reverse_split"
    if ex_date < HIGH_FROM:
        return typ, ratio, "low", "2000～2002 Polygon 記錄不全"
    return typ, ratio, "high", ""


class IdIndex:
    """_catalog/identity 長表：（代號, 日期）→ entity_id。取該代號第一段「to 晚於該天」；早於該段 from＝查不到。"""

    def __init__(self, id_rows):
        self.by = defaultdict(list)
        for r in id_rows:
            if (r.get("source") or "polygon") == "polygon":      # v2 起有 mpq 列，查法另見 MpqIndex
                self.by[norm(r["symbol"])].append(r)
        for v in self.by.values():
            v.sort(key=lambda r: r.get("to") or FAR)

    def entity(self, symbol, date):
        segs = self.by.get(norm(symbol))
        if not segs:
            return None, "身份表沒有這個代號"
        for r in segs:
            if (r.get("to") or FAR) > date:
                if r.get("from") and date < r["from"]:
                    return None, "早於該代號在身份表的起始日"
                return r["entity_id"], ""
        return None, "晚於該代號在身份表的最後一段"


MPQ_SUFFIX = "-DELISTED"


def mpq_spans(src_rows):
    """_src/mpq_symbols 列 → {(代號, asset): [(段首, 段末)]}（日線或 5 分任一有 bar）。"""
    out = defaultdict(list)
    for r in src_rows:
        ds = [x for x in (r.get("d_first"), r.get("m5_first")) if x]
        de = [x for x in (r.get("d_last"), r.get("m5_last")) if x]
        if ds and de:
            out[(r["mpq_symbol"], r["asset"])].append((min(ds), max(de)))
    return out


class MpqIndex:
    """身份表 v2 的 mpq 列：（Polygon 代號, 日期）→ 該日 MPQ 有數據的公司。
    同時看 X 與 X-DELISTED；列的有效期＝[from, to) 且不晚於該代號該類別最後一根 bar（to 空不代表之後還有數據）。"""

    def __init__(self, id_rows, src_rows):
        spans = mpq_spans(src_rows)
        self.by = defaultdict(list)
        for r in id_rows:
            if r.get("source") != "mpq":
                continue
            sym = r["symbol"]
            ends = [e for b, e in spans.get((sym, r.get("asset") or ""), [])
                    if b < (r.get("to") or FAR) and e >= r["from"]]
            if not ends:
                continue
            last = max(ends)
            if r.get("to"):
                last = min(last, (dt.date.fromisoformat(r["to"]) - dt.timedelta(days=1)).isoformat())
            base = sym[:-len(MPQ_SUFFIX)] if sym.upper().endswith(MPQ_SUFFIX) else sym
            self.by[norm(base)].append((r["from"], last, r["entity_id"], sym))

    def entities(self, symbol, date):
        """回 {entity: [MPQ 代號…]}。"""
        out = defaultdict(set)
        for f, last, e, sym in self.by.get(norm(symbol), []):
            if f <= date <= last:
                out[e].add(sym)
        return {e: sorted(v) for e, v in out.items()}


def _pick(idx, midx, sym, d, stats):
    """回 (entity, why, mpq 代號, 額外 note, 是否降為低信心)。"""
    eid, why = idx.entity(sym, d)
    if midx is None:
        return eid, why, "", "", False
    m = midx.entities(sym, d)
    if len(m) == 1:
        me, ms = next(iter(m.items()))
        if eid is None:
            stats["rehung"] += 1
            return me, "", "|".join(ms), "經 MPQ 代號對到公司（Polygon：%s）" % why, False
        if eid != me:
            stats["conflict"] += 1
            return me, "", "|".join(ms), "Polygon 段歸 %s，MPQ 該日數據屬 %s；照 MPQ" % (eid, me), True
        return eid, "", "|".join(ms), "", False
    if len(m) > 1:
        stats["mpq_ambiguous"] += 1
        if eid is not None:
            return eid, "", "|".join(m.get(eid, [])), "MPQ 該日有 %d 家用此代號，照 Polygon" % len(m), True
        return None, "MPQ 該日有 %d 家用此代號、Polygon 對不上" % len(m), "", "", False
    return eid, why, "", "", False


def split_rows(raw, id_rows, version, src_rows=None, keep_notes=None):
    """raw＝Polygon splits 記錄；src_rows＝_src/mpq_symbols（身份表含 mpq 列時給，v2 起）；
    keep_notes＝{(代號, 日): 說明}，來源守衛保留舊值的鍵（v3 起）。回 (rows, unmapped, stats)。純函數。"""
    idx = IdIndex(id_rows)
    midx = MpqIndex(id_rows, src_rows) if src_rows is not None else None
    rows, unm, seen = [], [], set()
    stats = defaultdict(int)
    for r in sorted(raw, key=lambda x: (x.get("execution_date") or "", norm(x.get("ticker")), str(x.get("id") or ""))):
        sym, d = (r.get("ticker") or "").strip(), (r.get("execution_date") or "").strip()
        sf, st = r.get("split_from"), r.get("split_to")
        if not sym or not re.match(r"^\d{4}-\d{2}-\d{2}$", d):
            stats["bad"] += 1
            continue
        key = (norm(sym), d, _rkey(sf, st))      # 10-30：比約分後比例，20:21 與 1:1.05 算同一筆（否則累乘兩次）
        if key in seen:
            stats["dup"] += 1
            continue
        seen.add(key)
        eid, why, msyms, mnote, low = _pick(idx, midx, sym, d, stats)
        if not eid:
            unm.append({"symbol": sym, "ex_date": d, "split_from": sf, "split_to": st, "reason": why})
            continue
        typ, ratio, conf, note = classify(sf, st, d)
        if low:
            conf = "low"
        knote = (keep_notes or {}).get((norm(sym), d), "")
        note = "；".join(x for x in (note, mnote, knote) if x)
        stats[typ] += 1
        stats["conf_" + conf] += 1
        rows.append({"entity_id": eid, "symbol": sym, "ex_date": d, "type": typ, "ratio": ratio,
                     "split_from": sf, "split_to": st, "source": "polygon", "evidence": r.get("id") or "",
                     "confidence": conf, "note": note, "table_version": version, "mpq_symbols": msyms})
    rows.sort(key=lambda x: (x["entity_id"], x["ex_date"]))
    stats["unmapped"] = len(unm)
    return rows, unm, dict(stats)


def latest_identity(files, read):
    ver, man = last_manifest(files, read, ID_DIR)
    if not ver:
        raise SystemExit("⛔ 找不到 _catalog/identity 已發布版本")
    return ver, list(csv.DictReader(io.StringIO(read("%s/v%d/identity.csv" % (ID_DIR, ver))))), man


def mpq_src_rows(read, id_man):
    """身份表 manifest 有 mpq_symbols（v2 起）→ 讀那份輸入並核 sha256；沒有＝None（照 v1 只用 Polygon）。"""
    src = id_man.get("mpq_symbols") if id_man else None
    if not src:
        return None
    txt = read(src + "/mpq_symbols.csv")
    if txt is None or _sha(txt) != id_man.get("mpq_symbols_sha256"):
        raise SystemExit("⛔ %s 讀不到或 sha256 與身份表 manifest 不一致" % src)
    return list(csv.DictReader(io.StringIO(txt, newline="")))


# ── 來源守衛（2026-10-26 Jun：Polygon 一週內改刪 8 筆，BALL 2004 消失、SLM 2003 比例變）──
CHANGE_COLS = ["symbol", "ex_date", "kind", "kept_from", "kept_to", "kept_id", "kept_version", "new_from", "new_to",
               "new_id", "status"]


def _rkey(sf, st):
    """比例的比較鍵：1:2 與 2:4 視為相同；解析不了就用原字串。"""
    try:
        return str(Fraction(str(st)) / Fraction(str(sf)))
    except (ValueError, ZeroDivisionError):
        return "%s:%s" % (sf, st)


def baseline(files, read):
    """各（代號, 除權日）最早出現那一版的 Polygon 原值（splits.csv＋unmapped.csv）。
    只收 Polygon 來源列：v4 起表裡有人工清單新增的列（source＝yahoo 等），它們不是 Polygon 記錄，算進來會被守衛補回、與清單撞鍵。
    回 {鍵: {"version": v, "recs": [{ticker, execution_date, split_from, split_to, id}]}}。"""
    base = {}
    for v in published(files, SPLIT_DIR):
        seen = defaultdict(list)
        for name, idcol in (("splits.csv", "evidence"), ("unmapped.csv", None)):
            txt = read("%s/v%d/%s" % (SPLIT_DIR, v, name))
            for r in csv.DictReader(io.StringIO(txt or "")):
                if (r.get("source") or "polygon").split("+")[0] != "polygon":
                    continue
                seen[(norm(r["symbol"]), r["ex_date"])].append(
                    {"ticker": r["symbol"], "execution_date": r["ex_date"], "split_from": r["split_from"],
                     "split_to": r["split_to"], "id": r.get(idcol) or "" if idcol else ""})
        for k, recs in seen.items():
            if k not in base:
                base[k] = {"version": v, "recs": recs}
    return base


def guard(raw, base):
    """本次 Polygon 與基準比：比例相同照常；刪除或改比例＝保留基準值；新鍵照收。回 (守衛後 raw, keep_notes, changes, stats)。"""
    cur = defaultdict(list)
    for r in raw:
        cur[(norm(r.get("ticker")), (r.get("execution_date") or "").strip())].append(r)
    out, notes, changes = [], {}, []
    st = defaultdict(int)
    for k, rs in cur.items():
        b = base.get(k)
        if b is None:
            st["added"] += 1
            out += rs
        elif {_rkey(r["split_from"], r["split_to"]) for r in rs} == {_rkey(r["split_from"], r["split_to"]) for r in b["recs"]}:
            out += rs
        else:
            st["ratio_changed"] += 1
            out += b["recs"]
            notes[k] = "保留 v%d 值：Polygon 這次改成 %s" % (b["version"], "、".join(
                "%s:%s" % (r["split_from"], r["split_to"]) for r in rs))
            for o in b["recs"]:
                for n in rs:
                    changes.append({"symbol": o["ticker"], "ex_date": k[1], "kind": "ratio_changed",
                                    "kept_from": o["split_from"], "kept_to": o["split_to"], "kept_id": o["id"],
                                    "kept_version": b["version"], "new_from": n.get("split_from"),
                                    "new_to": n.get("split_to"), "new_id": n.get("id") or "", "status": "kept_previous"})
    for k, b in base.items():
        if k in cur:
            continue
        st["deleted"] += 1
        out += b["recs"]
        notes[k] = "保留 v%d 值：Polygon 這次沒有這筆" % b["version"]
        for o in b["recs"]:
            changes.append({"symbol": o["ticker"], "ex_date": k[1], "kind": "deleted", "kept_from": o["split_from"],
                            "kept_to": o["split_to"], "kept_id": o["id"], "kept_version": b["version"], "new_from": "",
                            "new_to": "", "new_id": "", "status": "kept_previous"})
    changes.sort(key=lambda c: (c["kind"], c["symbol"], c["ex_date"]))
    return out, notes, changes, dict(st)


def plan_splits(files, read, raw, events_text=None):
    """events_text＝catalog_src/split_events.csv 內容（v4 起；None＝不合併，照 v3）。"""
    id_ver, id_rows, id_man = latest_identity(files, read)
    src_rows = mpq_src_rows(read, id_man)
    last_v, last = last_manifest(files, read, SPLIT_DIR)
    ver = last_v + 1
    base = baseline(files, read)
    gst = None
    if base:
        raw, notes, changes, gst = guard(raw, base)
    else:
        notes, changes = {}, []
    rows, unm, stats = split_rows(raw, id_rows, ver, src_rows, notes)
    est = None
    if events_text is not None:
        midx = MpqIndex(id_rows, src_rows) if src_rows is not None else None
        ms = (lambda sym, d, e: "|".join(midx.entities(sym, d).get(e, []))) if midx is not None else None
        rows, est = apply_events(rows, parse_events(events_text), ver, ms)
    cols = SPLIT_COLS + (["mpq_symbols"] if src_rows is not None else [])
    csha = content_sha(rows, cols)
    if last and last.get("content_sha256") == csha and last.get("identity_version") == id_ver:
        return [], "沿用 v%d（內容未變）" % last_v
    schema = SPLIT_SCHEMA if src_rows is None else SPLIT_SCHEMA_V2
    if est is not None:
        schema = SPLIT_SCHEMA_V3
    f = {"splits.csv": to_csv(rows, cols), "unmapped.csv": to_csv(unm, SPLIT_UNM_COLS)}
    if gst is not None:
        f["changes.csv"] = to_csv(changes, CHANGE_COLS)
    f["schema.json"] = json.dumps(schema, ensure_ascii=False, indent=1)
    extra = {"identity_version": id_ver, "content_sha256": csha, "polygon_rows": len(raw), "stats": stats}
    if src_rows is not None:
        extra["mpq_symbols"] = id_man["mpq_symbols"]
    if gst is not None:
        extra["guard"] = {"baseline_versions": published(files, SPLIT_DIR), "deleted": gst.get("deleted", 0),
                          "ratio_changed": gst.get("ratio_changed", 0), "added": gst.get("added", 0)}
    if est is not None:
        extra.update(events_sha256=_sha(events_text), events_rows=sum(est.values()), events=est)
    man = manifest(ver, "polygon /v3/reference/splits execution_date>=2000-01-01"
                   + ("＋catalog_src/split_events.csv" if est is not None else ""), f, extra)
    summ = "發布 v%d：%d 筆（拆股 %d、合股 %d、疑似分拆 %d；低信心 %d）｜對不上公司 %d｜重複略過 %d｜身份表 v%d" % (
        ver, len(rows), stats.get("split", 0), stats.get("reverse_split", 0), stats.get("suspect_spinoff", 0),
        stats.get("conf_low", 0), len(unm), stats.get("dup", 0), id_ver)
    if src_rows is not None:
        summ += "｜經 MPQ 補掛 %d、與 Polygon 歸屬不同照 MPQ %d、MPQ 同代號多家 %d" % (
            stats.get("rehung", 0), stats.get("conflict", 0), stats.get("mpq_ambiguous", 0))
    if gst is not None:
        summ += "｜來源守衛：Polygon 刪除 %d、改比例 %d 照舊保留，新增 %d" % (
            gst.get("deleted", 0), gst.get("ratio_changed", 0), gst.get("added", 0))
    if est is not None:
        summ += "｜人工查證清單：新增 %d、更正 %d、駁回 %d" % (
            sum(v for k, v in est.items() if k.startswith("add_")),
            sum(v for k, v in est.items() if k.startswith("override_")), est.get("reject", 0))
    return emit(SPLIT_DIR, ver, f, man), summ


# ── 人工查證清單（2026-10-29 Jun 拍板：①新類型沿用 splits.csv ②每筆要公開來源、跳變只當線索 ③錯的不刪標 rejected）──
EVENT_COLS = ["action", "entity_id", "symbol", "ex_date", "type", "ratio", "split_from", "split_to", "source", "evidence", "note"]
EVENT_ACTIONS = ("add", "override", "reject")
EVENT_TYPES = ("split", "reverse_split", "spinoff", "special_dividend", "stock_dividend")
EVENT_SOURCES = ("polygon", "polygon_dividend", "sec", "company", "exchange", "yahoo")
SPLIT_SCHEMA_V3 = json.loads(json.dumps(SPLIT_SCHEMA_V2))
SPLIT_SCHEMA_V3["schema"] = "catalog_splits_v3"
SPLIT_SCHEMA_V3["splits.csv"]["type"] += ("｜spinoff＝分拆（含合併分拆的重組調整）｜special_dividend＝特別股息｜stock_dividend＝股票股利"
                                          "（後三種只來自人工查證清單，ratio＝還原倍數，split_from／split_to 可空）")
SPLIT_SCHEMA_V3["splits.csv"]["source"] = ("polygon｜人工查證清單的來源（yahoo／sec／company／exchange／polygon_dividend）；"
                                           "更正過的 Polygon 列＝polygon+<來源>")
SPLIT_SCHEMA_V3["splits.csv"]["evidence"] = "Polygon 記錄 id｜清單附的公開記錄與實測跳變（「；」分隔）"
SPLIT_SCHEMA_V3["splits.csv"]["confidence"] += "｜人工查證清單新增或更正＝high｜rejected＝查證為錯，不用於價格還原（note 寫原因）"
SPLIT_SCHEMA_V3["限制"] = SPLIT_SCHEMA_V2["限制"] + [
    "v4 起合併 BC catalog_src/split_events.csv：add＝表裡沒有的事件；override＝更正既有列 type／ratio；reject＝既有列標 rejected（不刪）",
    "分拆的 ratio＝本地端實測跳變（與公開值差 ≤5% 才收）；拆股／股票股利用公開比例"]


def _ratio_ok(v):
    try:
        return float(v) > 0
    except (TypeError, ValueError):
        return False


def parse_events(text):
    """清單 CSV → 列表；任何一列不合格就整次停（fail-closed）。純函數。"""
    rd = csv.DictReader(io.StringIO(text))
    if rd.fieldnames != EVENT_COLS:
        raise SystemExit("⛔ split_events.csv 欄位不符：%s" % rd.fieldnames)
    out, seen = [], set()
    for i, e in enumerate(rd, 2):
        bad = []
        if e["action"] not in EVENT_ACTIONS:
            bad.append("action")
        if not (e["entity_id"] and e["symbol"] and re.match(r"^\d{4}-\d{2}-\d{2}$", e["ex_date"] or "")):
            bad.append("entity_id/symbol/ex_date")
        if not (e["evidence"] or "").strip():
            bad.append("evidence 必填")
        if e["action"] in ("add", "override"):
            if e["type"] not in EVENT_TYPES:
                bad.append("type")
            if not _ratio_ok(e["ratio"]):
                bad.append("ratio")
            if not all(x in EVENT_SOURCES for x in (e["source"] or "-").split("+")):
                bad.append("source")
        elif not (e["note"] or "").strip():
            bad.append("reject 要寫 note")
        k = (e["entity_id"], norm(e["symbol"]), e["ex_date"])
        if k in seen:
            bad.append("同鍵重複")
        seen.add(k)
        if bad:
            raise SystemExit("⛔ split_events.csv 第 %d 列不合格：%s" % (i, "、".join(bad)))
        out.append(e)
    return out


def apply_events(rows, events, version, msyms=None):
    """把清單套到拆股表。override／reject 必須恰好對到一列既有列；add 不得與既有列同鍵；違反就整次停。
    msyms(symbol, ex_date, entity_id)→該日 MPQ 代號字串（新增列用；None＝沒有 mpq_symbols 欄）。回 (rows, stats)。純函數。"""
    st = defaultdict(int)
    out = [dict(r) for r in rows]
    idx = defaultdict(list)
    for r in out:
        idx[(r["entity_id"], norm(r["symbol"]), r["ex_date"])].append(r)
    for e in events:
        k = (e["entity_id"], norm(e["symbol"]), e["ex_date"])
        hit = idx.get(k, [])
        if e["action"] == "add":
            if hit:
                raise SystemExit("⛔ split_events add 與既有列同鍵：%s %s %s" % k)
            new = {"entity_id": e["entity_id"], "symbol": e["symbol"], "ex_date": e["ex_date"], "type": e["type"],
                   "ratio": e["ratio"], "split_from": e["split_from"], "split_to": e["split_to"], "source": e["source"],
                   "evidence": e["evidence"], "confidence": "high", "note": e["note"], "table_version": version}
            if msyms is not None:
                new["mpq_symbols"] = msyms(e["symbol"], e["ex_date"], e["entity_id"]) or ""
            out.append(new)
            idx[k].append(new)
            st["add_" + e["type"]] += 1
            continue
        if len(hit) != 1:
            raise SystemExit("⛔ split_events %s 對到 %d 列（要恰好 1 列）：%s %s %s" % ((e["action"], len(hit)) + k))
        r = hit[0]
        ev_note = e["note"] if e["action"] == "override" else "查證為錯：" + e["note"]
        r.update(evidence="；".join(x for x in (r["evidence"], e["evidence"]) if x),
                 note="；".join(x for x in (r["note"], ev_note) if x))
        if e["action"] == "override":
            src = r["source"].split("+")
            r.update(type=e["type"], ratio=e["ratio"], confidence="high",
                     source="+".join(src + [x for x in e["source"].split("+") if x not in src]))
            st["override_" + e["type"]] += 1
        else:
            r["confidence"] = "rejected"
            st["reject"] += 1
    out.sort(key=lambda x: (x["entity_id"], x["ex_date"]))
    return out, dict(st)


# ── 交易日曆 ──
def _nth_weekday(year, month, weekday, n):
    d = dt.date(year, month, 1)
    return d + dt.timedelta(days=(weekday - d.weekday()) % 7 + 7 * (n - 1))


def _last_weekday(year, month, weekday):
    d = dt.date(year, 12, 31) if month == 12 else dt.date(year, month + 1, 1) - dt.timedelta(days=1)
    return d - dt.timedelta(days=(d.weekday() - weekday) % 7)


def _easter(year):
    a = year % 19; b = year // 100; c = year % 100
    d = b // 4; e = b % 4; f = (b + 8) // 25; g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30; i = c // 4; k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7; m = (a + 11 * h + 22 * l) // 451
    return dt.date(year, (h + l - 7 * m + 114) // 31, ((h + l - 7 * m + 114) % 31) + 1)


def _observed(d):
    if d.weekday() == 5:
        return d - dt.timedelta(days=1)
    if d.weekday() == 6:
        return d + dt.timedelta(days=1)
    return d


def nyse_year(year):
    """規則推算某年：回 [{date, type, open, close, reason}]。
    沿用 update_market_calendar_year.nyse_year，補兩條：六月節 2022 起才休市；元旦落週六不補休（NYSE 規則）。"""
    easter = _easter(year)
    hol = [("New Year's Day", dt.date(year, 1, 1) if dt.date(year, 1, 1).weekday() == 5 else _observed(dt.date(year, 1, 1))),
           ("Martin Luther King, Jr. Day", _nth_weekday(year, 1, 0, 3)),
           ("Washington's Birthday", _nth_weekday(year, 2, 0, 3)),
           ("Good Friday", easter - dt.timedelta(days=2)),
           ("Memorial Day", _last_weekday(year, 5, 0)),
           ("Juneteenth", _observed(dt.date(year, 6, 19)) if year >= 2022 else None),
           ("Independence Day", _observed(dt.date(year, 7, 4))),
           ("Labor Day", _nth_weekday(year, 9, 0, 1)),
           ("Thanksgiving Day", _nth_weekday(year, 11, 3, 4)),
           ("Christmas Day", _observed(dt.date(year, 12, 25)))]
    hol = [(n, d) for n, d in hol if d and d.weekday() < 5]
    days = {d for _, d in hol}
    out = [{"date": d.isoformat(), "type": "full_closure", "open": "", "close": "", "reason": n} for n, d in hol]
    half = [(_nth_weekday(year, 11, 3, 4) + dt.timedelta(days=1), "Day after Thanksgiving")]
    for d, n in ((dt.date(year, 7, 3), "Independence Day Eve"), (dt.date(year, 12, 24), "Christmas Eve")):
        if d.weekday() < 5 and d not in days:
            half.append((d, n))
    out += [{"date": d.isoformat(), "type": "early_close", "open": "09:30", "close": "13:00", "reason": n}
            for d, n in half]
    return sorted(out, key=lambda r: r["date"])


_TYPES = {"full closure": "full_closure", "early close": "early_close", "unscheduled early close": "early_close",
          "delayed open": "delayed_open"}


def parse_calendar_csv(text):
    """本地端 CSV（Date,DayOfWeek,Type,TradingHours,Reason）→ 標準列。認不得的 Type 直接報錯（不猜）。"""
    out = []
    for r in csv.DictReader(io.StringIO(text.lstrip("﻿"))):
        d = (r.get("Date") or "").strip()
        typ = _TYPES.get((r.get("Type") or "").strip().lower())
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", d) or not typ:
            raise SystemExit("⛔ 日曆 CSV 有認不得的列：%s / %s" % (d, r.get("Type")))
        op = cl = ""
        if typ != "full_closure":
            m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})", r.get("TradingHours") or "")
            if not m:
                raise SystemExit("⛔ 日曆 CSV 時段認不得：%s" % d)
            op, cl = "%02d:%s" % (int(m.group(1)), m.group(2)), "%02d:%s" % (int(m.group(3)), m.group(4))
        reason = (r.get("Reason") or "").strip()
        if (r.get("Type") or "").strip().lower() == "unscheduled early close":
            reason = ("Unscheduled: " + reason).strip()
        out.append({"date": d, "type": typ, "open": op, "close": cl, "reason": reason, "source": "local_csv"})
    dates = [x["date"] for x in out]
    if len(dates) != len(set(dates)):
        raise SystemExit("⛔ 日曆 CSV 有重複日期")
    return out


def crosscheck(csv_rows, years):
    by_csv = {r["date"]: r for r in csv_rows}
    by_rule = {r["date"]: r for y in years for r in nyse_year(y)}
    out = []
    for d in sorted(set(by_csv) | set(by_rule)):
        if int(d[:4]) not in years:
            continue
        c, r = by_csv.get(d), by_rule.get(d)
        ct, rt = (c or {}).get("type", ""), (r or {}).get("type", "")
        if ct == rt and (not c or c["close"] == r["close"]):
            continue
        note = ("只在 CSV：" + c["reason"]) if c and not r else ("只在規則：" + r["reason"]) if r and not c else "類型或時段不同"
        out.append({"date": d, "csv_type": ct, "rule_type": rt, "note": note})
    return out


_HHMM = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")


def _src_dict(text, cols, what):
    rd = csv.DictReader(io.StringIO(text.lstrip("﻿")))
    if rd.fieldnames != cols:
        raise SystemExit("⛔ %s 欄位不對：%s（應為 %s）" % (what, rd.fieldnames, ",".join(cols)))
    return [{k: (v or "").strip() for k, v in r.items()} for r in rd]


def _weekday(d, what):
    try:
        ok = dt.date.fromisoformat(d).weekday() < 5 and re.match(r"^\d{4}-\d{2}-\d{2}$", d)
    except ValueError:
        ok = False
    if not ok:
        raise SystemExit("⛔ %s 日期不對或不是平日：%s" % (what, d))


def _sources(s, allowed, what, d):
    if not s or any(x not in allowed for x in s.split("+")):
        raise SystemExit("⛔ %s %s 來源不對：%s（允許 %s，「+」併列）" % (what, d, s, "｜".join(allowed)))


def parse_calendar_fixes(text, csv_dates):
    """數據中心日曆補正清單（10-29 方案 A）→ calendar.csv 列（source=fix）。只准補 CSV 沒有的日子；任何不對就整次失敗。"""
    out = []
    for r in _src_dict(text, FIX_COLS, "日曆補正"):
        d = r["date"]
        _weekday(d, "日曆補正")
        if d in csv_dates:
            raise SystemExit("⛔ 日曆補正 %s 與本地端 CSV 同日期，不准覆蓋 CSV" % d)
        if r["type"] not in ("full_closure", "early_close", "delayed_open"):
            raise SystemExit("⛔ 日曆補正 %s 類型不對：%s" % (d, r["type"]))
        if r["type"] == "full_closure":
            if r["open"] or r["close"]:
                raise SystemExit("⛔ 日曆補正 %s 全日休市不該有時段" % d)
        elif not (_HHMM.match(r["open"]) and _HHMM.match(r["close"]) and r["open"] < r["close"]):
            raise SystemExit("⛔ 日曆補正 %s 時段不對：%s–%s" % (d, r["open"], r["close"]))
        _sources(r["source"], FIX_SOURCES, "日曆補正", d)
        if not r["reason"] or not r["evidence"]:
            raise SystemExit("⛔ 日曆補正 %s 缺原因或證據" % d)
        out.append({"date": d, "type": r["type"], "open": r["open"], "close": r["close"],
                    "reason": r["reason"], "source": "fix"})
    dates = [x["date"] for x in out]
    if len(dates) != len(set(dates)):
        raise SystemExit("⛔ 日曆補正有重複日期")
    return out


def parse_halts(text):
    """停盤／已知缺口清單（10-29 方案 A）→ halts.csv 列。任何不對就整次失敗。"""
    out = []
    for r in _src_dict(text, HALT_SRC_COLS, "停盤清單"):
        d = r["date"]
        _weekday(d, "停盤清單")
        if not (_HHMM.match(r["start"]) and _HHMM.match(r["end"]) and r["start"] < r["end"]):
            raise SystemExit("⛔ 停盤清單 %s 時段不對：%s–%s" % (d, r["start"], r["end"]))
        if r["scope"] not in HALT_SCOPES or r["kind"] not in HALT_KINDS:
            raise SystemExit("⛔ 停盤清單 %s 範圍或類型不對：%s／%s" % (d, r["scope"], r["kind"]))
        _sources(r["source"], HALT_SOURCES, "停盤清單", d)
        if not r["note"]:
            raise SystemExit("⛔ 停盤清單 %s 缺說明" % d)
        out.append(r)
    keys = [(x["date"], x["start"], x["scope"]) for x in out]
    if len(keys) != len(set(keys)):
        raise SystemExit("⛔ 停盤清單有重複（日期、開始、範圍）")
    return sorted(out, key=lambda x: (x["date"], x["start"], x["scope"]))


def calendar_rows(csv_text, rule_years, version, fixes_text=None, halts_text=None):
    """回傳 (calendar 列, crosscheck 列, (CSV 首年, 末年), halts 列或 None, fix 列數)。"""
    csv_rows = parse_calendar_csv(csv_text)
    csv_years = sorted({int(r["date"][:4]) for r in csv_rows})
    fixes = parse_calendar_fixes(fixes_text, {r["date"] for r in csv_rows}) if fixes_text is not None else []
    rows = list(csv_rows) + fixes
    for y in rule_years:
        if y in csv_years:
            raise SystemExit("⛔ %d 年 CSV 已有，不用規則覆蓋" % y)
        rows += [dict(r, source="rule") for r in nyse_year(y)]
    dates = [r["date"] for r in rows]
    if len(dates) != len(set(dates)):
        raise SystemExit("⛔ 日曆補正與規則推算撞日期")
    halts = None
    if halts_text is not None:
        closed = {r["date"] for r in rows if r["type"] == "full_closure"}
        halts = parse_halts(halts_text)
        bad = sorted({h["date"] for h in halts} & closed)
        if bad:
            raise SystemExit("⛔ 停盤清單的日子是全日休市：%s" % ",".join(bad))
        for h in halts:
            h["table_version"] = version
    for r in rows:
        r["table_version"] = version
    rows.sort(key=lambda r: r["date"])
    xc = crosscheck(csv_rows, list(range(csv_years[0], csv_years[-1] + 1)))
    return rows, xc, (csv_years[0], csv_years[-1]), halts, len(fixes)


def plan_calendar(files, read, csv_text, rule_years, fixes_text=None, halts_text=None):
    last_v, last = last_manifest(files, read, CAL_DIR)
    ver = last_v + 1
    rows, xc, (y0, y1), halts, n_fix = calendar_rows(csv_text, rule_years, ver, fixes_text, halts_text)
    v2 = fixes_text is not None or halts_text is not None
    csha = content_sha(rows, CAL_COLS)
    if halts is not None:      # v2：只改停盤清單也要發新版
        csha = _sha(csha + content_sha(halts, HALT_COLS))
    if last and last.get("content_sha256") == csha:
        return [], "沿用 v%d（內容未變）" % last_v
    f = {"calendar.csv": to_csv(rows, CAL_COLS), "crosscheck.csv": to_csv(xc, XC_COLS),
         "schema.json": json.dumps(CAL_SCHEMA_V2 if v2 else CAL_SCHEMA, ensure_ascii=False, indent=1)}
    if halts is not None:
        f["halts.csv"] = to_csv(halts, HALT_COLS)
    n_rule = sum(1 for r in rows if r["source"] == "rule")
    extra = {"content_sha256": csha, "csv_years": [y0, y1], "rule_years": rule_years}
    if fixes_text is not None:
        extra.update(fixes_sha256=_sha(fixes_text), fixes_rows=n_fix)
    if halts is not None:
        extra.update(halts_sha256=_sha(halts_text), halts_rows=len(halts))
    man = manifest(ver, "local_csv %d～%d（sha256 %s）＋rule %s%s" % (
        y0, y1, _sha(csv_text)[:12], ",".join(map(str, rule_years)) or "無",
        "＋fix %d" % n_fix if fixes_text is not None else ""), f, extra)
    summ = "發布 v%d：%d 列（CSV %d～%d 共 %d、補正 %d、規則推算 %d）｜交叉核對差異 %d" % (
        ver, len(rows), y0, y1, len(rows) - n_rule - n_fix, n_fix, n_rule, len(xc))
    if halts is not None:
        summ += "｜停盤清單 %d（%s）" % (len(halts), "、".join(
            "%s %d" % (k, sum(1 for h in halts if h["kind"] == k)) for k in HALT_KINDS))
    return emit(CAL_DIR, ver, f, man), summ


# ── 執行 ──
def fetch_polygon_splits():
    from courier.polygon import RawPolygon
    rows = RawPolygon(lambda m: print(m, flush=True)).all_pages(
        "/v3/reference/splits", {"execution_date.gte": "2000-01-01", "limit": 1000,
                                 "sort": "execution_date", "order": "asc"})
    if rows is None:
        raise SystemExit("⛔ Polygon 拆股查詢失敗")
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step", required=True, choices=("splits", "calendar", "identity_mpq", "series_candidates", "identity_refresh"),
                    help="identity_mpq＝身份表 v2 含 MPQ 代號（2026-10-25，identity_mpq.py）｜"
                         "series_candidates＝序列正本表候選（2026-10-26，series_mpq.py）｜"
                         "identity_refresh＝身份表重建：最新 Polygon 來源＋併 MPQ 一次發版（2026-10-28 工單通道）")
    ap.add_argument("--dry-run", action="store_true", help="不寫 dataset，結果寫本機 --out")
    ap.add_argument("--out", default="_catalog_out")
    ap.add_argument("--rule-years", default="2027", help="規則推算的年份（逗號分隔）")
    a = ap.parse_args()
    from courier.dataset import RawHF
    hf = RawHF(write=not a.dry_run, repo=REPO, written_by="catalog_rules")
    files = hf.list_files()
    if a.step == "splits":
        ev = None
        if os.path.exists(SPLIT_EVENTS_FILE):
            with open(SPLIT_EVENTS_FILE, encoding="utf-8") as fh:
                ev = fh.read()
        adds, summ = plan_splits(files, hf.read, fetch_polygon_splits(), ev)
    elif a.step == "identity_mpq":
        import identity_mpq
        adds, summ = identity_mpq.plan(files, hf.read)
    elif a.step == "identity_refresh":
        import identity_mpq
        adds, summ = identity_mpq.plan_refresh(files, hf.read)
    elif a.step == "series_candidates":
        import series_mpq
        adds, summ = series_mpq.plan(files, hf.read)
    else:
        if not os.path.exists(CAL_SRC_FILE):
            raise SystemExit("⛔ 缺日曆來源 %s" % CAL_SRC_FILE)
        with open(CAL_SRC_FILE, encoding="utf-8") as fh:
            src = fh.read()
        years = [int(y) for y in a.rule_years.split(",") if y.strip()]
        extra = []
        for p in (CAL_FIXES_FILE, CAL_HALTS_FILE):
            if os.path.exists(p):
                with open(p, encoding="utf-8") as fh:
                    extra.append(fh.read())
            else:
                extra.append(None)
        adds, summ = plan_calendar(files, hf.read, src, years, *extra)
    for p, _ in adds:
        if p in files:
            raise SystemExit("⛔ %s 已存在，不覆蓋" % p)
    for p, t in adds:
        hf.add(p, t)
    n = hf.flush(a.out, "catalog %s" % a.step)
    print("%s：%s" % ({"splits": "拆股表", "calendar": "交易日曆", "identity_mpq": "身份表", "series_candidates": "序列正本表候選", "identity_refresh": "身份表重建"}[a.step], summ), flush=True)
    print("寫出 %d 個檔（%s）" % (n, "本機試跑 " + a.out if a.dry_run else "私有 dataset"), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
