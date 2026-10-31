# -*- coding: utf-8 -*-
"""
series_mpq.py — 序列正本表的候選（2026-10-26 Jun 同意範圍＝A4 去重＋A5 改名拼接；兩步走的第一步）

同一家公司（entity_id）×同類別（stock／etf）在 MPQ 有 ≥2 個代號時，列出它們的關係與建議：
- overlap    代號日期重疊（MPQ 把改名前歷史複製到新代號、或同發行人不同證券被掛同一家）→ A4：
             建議留覆蓋最長者（同長取較晚起的＝新代號），其餘記別名；**要本地端核重疊段收盤是否相同**才算數
- sequential 前後接、空檔 ≤10 天 → A5：建議拼接；**要本地端核接縫價格接得上**才算數
- gap        前後接、空檔 >10 天 → hold（Jun 人工看）
輸入：_catalog/identity 最新版（需含 source=mpq）＋ 它所根據的 _catalog/_src/mpq_symbols（取各段末根日）
輸出：_catalog/series/candidates/v<N>/{groups.csv, pairs.csv, schema.json, manifest.json}（草稿，不是正本）
2026-10-30 加正本發布 plan_publish：本地端核對結果（_src/series_check）＋候選表 → _catalog/series/v<N>/。
登記：MongoDB_Standard_v3 §六-D 10-26 條。寫入方＝catalog_rules.py --step series_candidates。日誌只印筆數。Python 3.9 相容。
"""
import csv
import datetime as dt
import io
import json
from collections import Counter, defaultdict

from catalog_publish import CAT, _sha, published, to_csv

ID_DIR = CAT + "/identity"
CAND_DIR = CAT + "/series/candidates"
SERIES_DIR = CAT + "/series"
CHECK_DIR = CAT + "/_src/series_check"
GAP_DAYS = 10       # 天（日曆日）：前後接的空檔在這以內算改名接續（與身份表 v2 的 TOL 一致）
FAR = "9999-12-31"
GROUP_COLS = ["entity_id", "asset", "kind", "n_symbols", "symbols", "proposed_primary", "note"]
PAIR_COLS = ["pair_id", "entity_id", "asset", "sym_a", "sym_b", "check", "from", "to", "a_last", "b_first",
             "gap_days", "proposed_action"]
SCHEMA = {
    "schema": "catalog_series_candidates_v1",
    "說明": "草稿，不是正本；本地端照價格核對規格核 pairs.csv 後，數據中心才發布 _catalog/series/v<N>/",
    "groups.csv": {
        "kind": "overlap＝代號日期重疊（A4）｜sequential＝前後接、空檔 ≤10 天（A5）｜gap＝空檔 >10 天（hold）",
        "symbols": "這家公司這個類別的 MPQ 代號，格式 代號@首日~末日，按首日排，「|」分隔",
        "proposed_primary": "覆蓋天數最長的代號（同長取較晚起的＝新代號）；只是建議",
        "note": "說明"},
    "pairs.csv": {
        "check": "overlap_identity＝核重疊段（from～to）兩代號收盤是否相同｜splice_continuity＝核 a 末日收盤與 b 首日收盤接不接得上",
        "a_last／b_first": "sym_a 最後一根、sym_b 第一根 bar 的日子（sequential／gap 用）",
        "gap_days": "b_first − a_last 的日曆天數；重疊時為負",
        "proposed_action": "dedupe_keep_a／dedupe_keep_b＝重複、留 a／b｜splice＝拼接｜hold＝不自動處理"},
    "限制": ["entity_id 來自身份表，同發行人的不同證券（優先股、權證、單位）可能被掛成同一家；價格核對會把它們判成不同、進 hold",
           "日期是 MPQ 有 bar 的首末日（日線或 5 分任一），不是上市／下市日"]}


def _days(a, b):
    return (dt.date.fromisoformat(b) - dt.date.fromisoformat(a)).days


def intervals(id_rows, src_rows):
    """(entity, asset) → [{symbol, first, last}]：身份表 mpq 列 ∩ 該代號該類別實際有 bar 的期間。自成一家（MPQ:）不列。"""
    spans = defaultdict(list)
    for r in src_rows:
        ds = [x for x in (r.get("d_first"), r.get("m5_first")) if x]
        de = [x for x in (r.get("d_last"), r.get("m5_last")) if x]
        if ds and de:
            spans[(r["mpq_symbol"], r["asset"])].append((min(ds), max(de)))
    acc = defaultdict(lambda: [FAR, ""])
    for r in id_rows:
        if r.get("source") != "mpq" or r["entity_id"].startswith("MPQ:"):
            continue
        to_last = (dt.date.fromisoformat(r["to"]) - dt.timedelta(days=1)).isoformat() if r.get("to") else FAR
        for b, e in spans.get((r["symbol"], r.get("asset") or ""), []):
            lo, hi = max(b, r["from"]), min(e, to_last)
            if lo <= hi:
                k = (r["entity_id"], r["asset"], r["symbol"])
                acc[k][0], acc[k][1] = min(acc[k][0], lo), max(acc[k][1], hi)
    out = defaultdict(list)
    for (e, a, s), (f, l) in acc.items():
        out[(e, a)].append({"symbol": s, "first": f, "last": l})
    for v in out.values():
        v.sort(key=lambda x: (x["first"], x["symbol"]))
    return out


def _primary(iv):
    return max(iv, key=lambda x: (_days(x["first"], x["last"]), x["first"], x["symbol"]))["symbol"]


def build(id_rows, src_rows):
    """純函數：回 (groups, pairs, stats)。"""
    groups, pairs = [], []
    st = Counter()
    for (e, a), iv in sorted(intervals(id_rows, src_rows).items()):
        if len(iv) < 2:
            st["single"] += 1
            continue
        prim = _primary(iv)
        kinds, notes = set(), []
        for i, x in enumerate(iv):
            for y in iv[i + 1:]:
                if y["first"] <= x["last"]:                     # 重疊
                    kinds.add("overlap")
                    if prim in (x["symbol"], y["symbol"]):           # 有正本候選就留它，否則留這兩個裡較長的
                        keep = "a" if prim == x["symbol"] else "b"
                    else:
                        keep = "a" if _primary([x, y]) == x["symbol"] else "b"
                    pairs.append({"entity_id": e, "asset": a, "sym_a": x["symbol"], "sym_b": y["symbol"],
                                  "check": "overlap_identity", "from": y["first"], "to": min(x["last"], y["last"]),
                                  "a_last": x["last"], "b_first": y["first"], "gap_days": _days(x["last"], y["first"]),
                                  "proposed_action": "dedupe_keep_" + keep})
        seq = sorted(iv, key=lambda x: (x["last"], x["first"]))
        for x, y in zip(seq, seq[1:]):                          # 依時間相鄰、不重疊的接縫
            if y["first"] <= x["last"]:
                continue
            gap = _days(x["last"], y["first"])
            k = "sequential" if gap <= GAP_DAYS else "gap"
            kinds.add(k)
            pairs.append({"entity_id": e, "asset": a, "sym_a": x["symbol"], "sym_b": y["symbol"],
                          "check": "splice_continuity", "from": "", "to": "", "a_last": x["last"], "b_first": y["first"],
                          "gap_days": gap, "proposed_action": "splice" if k == "sequential" else "hold"})
        kind = "overlap" if "overlap" in kinds else ("gap" if "gap" in kinds else "sequential")
        if len(kinds) > 1:
            notes.append("同時有 " + "、".join(sorted(kinds)))
        st[kind] += 1
        groups.append({"entity_id": e, "asset": a, "kind": kind, "n_symbols": len(iv),
                       "symbols": "|".join("%s@%s~%s" % (x["symbol"], x["first"], x["last"]) for x in iv),
                       "proposed_primary": prim, "note": "；".join(notes)})
    for i, p in enumerate(pairs, 1):
        p["pair_id"] = "P%05d" % i
    st["pairs"] = len(pairs)
    st.update({"pair_" + k: v for k, v in Counter(p["check"] for p in pairs).items()})
    return groups, pairs, dict(st)


def plan(files, read):
    """回 (adds, 摘要)。純計算。"""
    pub = published(files, ID_DIR)
    if not pub:
        raise SystemExit("⛔ 找不到 _catalog/identity 已發布版本")
    id_ver = pub[-1]
    id_man = json.loads(read("%s/v%d/manifest.json" % (ID_DIR, id_ver)))
    src = id_man.get("mpq_symbols")
    if not src:
        raise SystemExit("⛔ 身份表 v%d 沒有 MPQ 代號（需 v2 起）" % id_ver)
    src_txt = read(src + "/mpq_symbols.csv")
    if src_txt is None or _sha(src_txt) != id_man.get("mpq_symbols_sha256"):
        raise SystemExit("⛔ %s 讀不到或 sha256 與身份表 manifest 不一致" % src)
    id_rows = list(csv.DictReader(io.StringIO(read("%s/v%d/identity.csv" % (ID_DIR, id_ver)))))
    groups, pairs, st = build(id_rows, list(csv.DictReader(io.StringIO(src_txt, newline=""))))
    cpub = published(files, CAND_DIR)
    if cpub:
        last = json.loads(read("%s/v%d/manifest.json" % (CAND_DIR, cpub[-1])))
        if last.get("identity_version") == id_ver and last.get("mpq_symbols") == src:
            return [], "沿用候選 v%d（輸入未變）" % cpub[-1]
    ver = (cpub[-1] + 1) if cpub else 1
    f = {"groups.csv": to_csv(groups, GROUP_COLS), "pairs.csv": to_csv(pairs, PAIR_COLS),
         "schema.json": json.dumps(SCHEMA, ensure_ascii=False, indent=1)}
    man = {"version": ver, "created": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "source": "%s/v%d ＋ %s" % (ID_DIR, id_ver, src),
           "files": {k: ({"sha256": _sha(v), "rows": max(v.count("\n") - 1, 0)} if k.endswith(".csv")
                         else {"sha256": _sha(v)}) for k, v in f.items()},
           "identity_version": id_ver, "mpq_symbols": src, "rules": {"gap_days": GAP_DAYS}, "stats": st,
           "note": "草稿：本地端價格核對後才發布 _catalog/series/v<N>/ 正本"}
    d = "%s/v%d" % (CAND_DIR, ver)
    adds = [("%s/%s" % (d, k), v) for k, v in f.items()]
    adds.append((d + "/manifest.json", json.dumps(man, ensure_ascii=False, indent=1)))   # manifest 最後
    summ = "發布候選 v%d：多代號 %d 家（重疊 %d、前後接 %d、空檔過長 %d）｜待核 %d 對（重疊段 %d、接縫 %d）｜身份表 v%d" % (
        ver, len(groups), st.get("overlap", 0), st.get("sequential", 0), st.get("gap", 0), len(pairs),
        st.get("pair_overlap_identity", 0), st.get("pair_splice_continuity", 0), id_ver)
    return adds, summ


# ── 正本發布（2026-10-30 Jun 拍板）：本地端核對結果＋候選表 → _catalog/series/v<N>/ ──
MAX_DUP_OFF = 0.005     # duplicate 的中位比值偏離超過這個就不併（AHP／BHR 差約 1%：更像少一個係數，不是四捨五入）
SERIES_COLS = ["entity_id", "asset", "primary_symbol", "n_members", "members", "first", "last", "table_version"]
ALIAS_COLS = ["entity_id", "asset", "symbol", "primary_symbol", "role", "from", "to", "pair_ids", "price_basis", "table_version"]
HOLD_COLS = ["pair_id", "entity_id", "asset", "sym_a", "sym_b", "check", "verdict", "price_basis", "reason", "median_ratio",
             "junction_ratio", "table_version"]
SERIES_SCHEMA = {
    "schema": "catalog_series_v1",
    "說明": "同一家公司同類別的 MPQ 代號怎麼合併：只有本地端價格核對過的才併，其餘在 hold.csv",
    "series.csv": {"primary_symbol": "這組的主代號：沒被去重掉的成員中末日最晚者（同日取覆蓋天數長者）",
                   "members": "組內代號，「|」分隔", "first／last": "組內最早首日、最晚末日"},
    "aliases.csv": {"role": "primary＝主代號｜duplicate＝同一份數據，用主代號，這條不用｜splice＝接在同一條序列上",
                    "from／to": "該代號 MPQ 有 bar 的首末日", "pair_ids": "依據哪些核對對", "price_basis": "actual｜mpq_adjusted，有混用時用 actual|mpq_adjusted"},
    "hold.csv": {"reason": "verdict 原值，或 ratio_off＝duplicate 但比值偏離、not_actual＝接縫只有調整價、candidate_hold＝候選表本就 hold"},
    "限制": ["沒列在 aliases.csv 的代號維持自己一條序列", "只處理候選表裡的對；hold 的對不代表錯，只是還沒核准"]}


def adopt(pair, chk):
    """這一對採不採用：回 (去向, 原因)，去向＝dedupe／splice／None。純函數。"""
    v = chk["verdict"]
    act = pair["proposed_action"]
    if chk["check"] == "overlap_identity":
        if v != "duplicate":
            return None, v
        if abs(float(chk["median_ratio_b_over_a"]) - 1) > MAX_DUP_OFF:
            return None, "ratio_off"
        return ("dedupe", "") if act.startswith("dedupe") else (None, "candidate_hold")
    if v != "continuous":
        return None, v
    if act != "splice":
        return None, "candidate_hold"
    return ("splice", "") if chk["price_basis"] == "actual" else (None, "not_actual")


def _spans(groups):
    """groups.csv 的 代號@首日~末日 → {(entity, asset, symbol): (首, 末)}。"""
    out = {}
    for g in groups:
        for x in g["symbols"].split("|"):
            sym, d = x.rsplit("@", 1)
            out[(g["entity_id"], g["asset"], sym)] = tuple(d.split("~"))
    return out


def build_series(groups, pairs, checks, version):
    """純函數：候選表＋核對結果 → (series 列, aliases 列, hold 列, 統計)。對不上或同組矛盾就整次失敗。"""
    by_id = {c["pair_id"]: c for c in checks}
    if len(by_id) != len(checks) or set(by_id) != {p["pair_id"] for p in pairs}:
        raise SystemExit("⛔ 核對結果與候選表的對不一致")
    for p in pairs:
        c = by_id[p["pair_id"]]
        if (c["sym_a"], c["sym_b"], c["asset"], c["check"], c["entity_id"]) != (p["sym_a"], p["sym_b"], p["asset"], p["check"], p["entity_id"]):
            raise SystemExit("⛔ %s 核對結果與候選表內容不同" % p["pair_id"])
    spans = _spans(groups)
    par = {}

    def find(x):
        par.setdefault(x, x)
        while par[x] != x:
            par[x] = par[par[x]]
            x = par[x]
        return x

    used, hold, dropped = [], [], set()
    st = Counter()
    for p in pairs:
        c = by_id[p["pair_id"]]
        how, why = adopt(p, c)
        a, b = (p["entity_id"], p["asset"], p["sym_a"]), (p["entity_id"], p["asset"], p["sym_b"])
        if how is None:
            hold.append({"pair_id": p["pair_id"], "entity_id": p["entity_id"], "asset": p["asset"], "sym_a": p["sym_a"],
                         "sym_b": p["sym_b"], "check": p["check"], "verdict": c["verdict"], "price_basis": c["price_basis"],
                         "reason": why, "median_ratio": c.get("median_ratio_b_over_a") or "",
                         "junction_ratio": c.get("junction_ratio") or "", "table_version": version})
            st["hold_" + why] += 1
            continue
        par[find(a)] = find(b)
        used.append((p, c, how, a, b))
        st[how] += 1
        if how == "dedupe":
            dropped.add(b if p["proposed_action"] == "dedupe_keep_a" else a)
    for h in hold:
        a, b = (h["entity_id"], h["asset"], h["sym_a"]), (h["entity_id"], h["asset"], h["sym_b"])
        if a in par and b in par and find(a) == find(b) and h["reason"] in ("different", "scaled", "jump"):
            raise SystemExit("⛔ 同組矛盾：%s %s／%s 判 %s，卻被其他對併成一組" % (h["pair_id"], h["sym_a"], h["sym_b"], h["reason"]))
    comp = defaultdict(list)
    for k in list(par):
        comp[find(k)].append(k)
    series, aliases = [], []
    links = defaultdict(list)
    for p, c, how, a, b in used:
        links[a].append((p["pair_id"], c["price_basis"], how))
        links[b].append((p["pair_id"], c["price_basis"], how))
    for ks in comp.values():
        for k in ks:
            if k not in spans:
                raise SystemExit("⛔ %s 不在候選表 groups.csv" % (k,))
        pool = [k for k in ks if k not in dropped] or ks
        prim = max(pool, key=lambda k: (spans[k][1], _days(*spans[k]), k[2]))
        e, asset = prim[0], prim[1]
        members = sorted(k[2] for k in ks)
        series.append({"entity_id": e, "asset": asset, "primary_symbol": prim[2], "n_members": len(ks), "members": "|".join(members),
                       "first": min(spans[k][0] for k in ks), "last": max(spans[k][1] for k in ks), "table_version": version})
        for k in sorted(ks, key=lambda x: x[2]):
            ls = links[k]
            role = "primary" if k == prim else "duplicate" if k in dropped else "splice"
            aliases.append({"entity_id": e, "asset": asset, "symbol": k[2], "primary_symbol": prim[2], "role": role,
                            "from": spans[k][0], "to": spans[k][1], "pair_ids": "|".join(sorted({x[0] for x in ls})),
                            "price_basis": "|".join(sorted({x[1] for x in ls})), "table_version": version})
    series.sort(key=lambda r: (r["entity_id"], r["asset"], r["primary_symbol"]))
    aliases.sort(key=lambda r: (r["entity_id"], r["asset"], r["symbol"]))
    st.update(series=len(series), aliases=len(aliases), hold=len(hold))
    return series, aliases, hold, dict(st)


def plan_publish(files, read):
    """回 (adds, 摘要)。純計算，不寫東西。"""
    cv = published(files, CHECK_DIR)
    if not cv:
        raise SystemExit("⛔ 找不到 %s（先跑 tools/upload_series_check.py）" % CHECK_DIR)
    chk_v = cv[-1]
    chk_man = json.loads(read("%s/v%d/manifest.json" % (CHECK_DIR, chk_v)))
    chk_txt = read("%s/v%d/check.csv" % (CHECK_DIR, chk_v))
    if chk_txt is None or _sha(chk_txt) != chk_man["files"]["check.csv"]["sha256"]:
        raise SystemExit("⛔ check.csv 讀不到或 sha256 與 manifest 不一致")
    cand_v = int(chk_man["candidates_version"])
    d = "%s/v%d" % (CAND_DIR, cand_v)
    cand_man = json.loads(read(d + "/manifest.json"))
    id_ver = published(files, ID_DIR)[-1]
    if cand_man["identity_version"] != id_ver:
        raise SystemExit("⛔ 候選表 v%d 用身份表 v%s，最新是 v%d" % (cand_v, cand_man["identity_version"], id_ver))
    pub = published(files, SERIES_DIR)
    if pub:
        last = json.loads(read("%s/v%d/manifest.json" % (SERIES_DIR, pub[-1])))
        if last.get("check_sha256") == _sha(chk_txt) and last.get("identity_version") == id_ver:
            return [], "沿用 v%d（輸入未變）" % pub[-1]
    ver = (pub[-1] + 1) if pub else 1
    groups = list(csv.DictReader(io.StringIO(read(d + "/groups.csv"))))
    pairs = list(csv.DictReader(io.StringIO(read(d + "/pairs.csv"))))
    checks = list(csv.DictReader(io.StringIO(chk_txt)))
    series, aliases, hold, st = build_series(groups, pairs, checks, ver)
    f = {"series.csv": to_csv(series, SERIES_COLS), "aliases.csv": to_csv(aliases, ALIAS_COLS), "hold.csv": to_csv(hold, HOLD_COLS),
         "schema.json": json.dumps(SERIES_SCHEMA, ensure_ascii=False, indent=1)}
    man = {"version": ver, "created": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "source": "%s ＋ %s/v%d" % (d, CHECK_DIR, chk_v),
           "files": {k: ({"sha256": _sha(v), "rows": max(v.count("\n") - 1, 0)} if k.endswith(".csv")
                         else {"sha256": _sha(v)}) for k, v in f.items()},
           "candidates_version": cand_v, "identity_version": id_ver, "check_version": chk_v, "check_sha256": _sha(chk_txt),
           "rules": {"max_dup_off": MAX_DUP_OFF}, "counts": st}
    out = "%s/v%d" % (SERIES_DIR, ver)
    adds = [("%s/%s" % (out, k), v) for k, v in f.items()]
    adds.append((out + "/manifest.json", json.dumps(man, ensure_ascii=False, indent=1)))   # manifest 最後
    return adds, "發布 v%d：%d 組、%d 個代號（去重 %d 對、拼接 %d 對）｜hold %d 對｜候選 v%d、身份表 v%d" % (
        ver, st["series"], st["aliases"], st.get("dedupe", 0), st.get("splice", 0), st["hold"], cand_v, id_ver)
