# -*- coding: utf-8 -*-
"""
test_series_mpq.py — 序列正本表候選（series_mpq.py）離線測試：假的身份表 v2 與假的 MPQ 代號摘要，不連網。
用法：python tests/test_series_mpq.py
"""
import csv
import hashlib
import io
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import series_mpq as SM   # noqa: E402

PASS, FAIL = [], []


def check(name, ok, info=""):
    (PASS if ok else FAIL).append(name)
    print(("✅ " if ok else "❌ ") + name + ("" if ok else "  " + str(info)))


def rows(text):
    return list(csv.DictReader(io.StringIO(text)))


H = "entity_id,source,symbol,from,to,evidence,note,table_version,asset\n"
IDENT = H + "\n".join([
    "FIGI:META,polygon,META,,,polygon,,2,",
    "FIGI:META,mpq,FB-DELISTED,2012-05-18,2022-06-09,mpq_overlap,,2,stock",     # 重疊：MPQ 把舊史複製到 META
    "FIGI:META,mpq,META,2012-05-18,,mpq_overlap,,2,stock",
    "FIGI:TT,mpq,TTAC,2016-09-28,,mpq_overlap,,2,stock",                        # 前後接（改名）
    "FIGI:TT,mpq,ABFL,2024-12-13,,mpq_overlap,,2,stock",
    "FIGI:GAP,mpq,OLDG-DELISTED,2000-01-03,,mpq_overlap,,2,stock",               # 空檔過長
    "FIGI:GAP,mpq,NEWG,2013-05-01,,mpq_overlap,,2,stock",
    "FIGI:ONE,mpq,ONE,2000-01-03,,mpq_overlap,,2,stock",                         # 只有一個代號：不列
    "MPQ:SOLO,mpq,SOLO,2000-01-03,,mpq_only,,2,stock",                           # 自成一家：不列
    "FIGI:J,mpq,JAVA,2007-08-27,,mpq_overlap,,2,stock",                          # 同代號不同類別：各自一家一條
    "FIGI:J,mpq,JAVA,2021-10-05,,mpq_overlap,,2,etf",
    "FIGI:AI,mpq,AAIC-DELISTED,2000-01-03,,mpq_overlap,,2,stock",                # 三個代號：一個包住兩個
    "FIGI:AI,mpq,AIW-DELISTED,2013-05-06,,mpq_overlap,,2,stock",
    "FIGI:AI,mpq,AIC-DELISTED,2015-03-23,,mpq_overlap,,2,stock",
    "FIGI:CUT,mpq,CUT-DELISTED,2000-01-03,2010-01-04,mpq_overlap,,2,stock",      # 身份段 to 截斷 bar 期間
]) + "\n"
SRC = "mpq_symbol,asset,seg,d_first,d_last,d_days,m5_first,m5_last,m5_days\n" + "\n".join([
    "FB-DELISTED,stock,1,2012-05-18,2022-06-08,2531,,,",
    "META,stock,1,2012-05-21,2026-10-23,3628,2012-05-18,2026-10-23,3629",
    "TTAC,stock,1,2016-09-28,2024-12-12,2000,,,",
    "ABFL,stock,1,2024-12-13,2026-10-23,460,,,",
    "OLDG-DELISTED,stock,1,2000-01-03,2010-11-09,2700,,,",
    "NEWG,stock,1,2013-05-01,2026-10-23,3400,,,",
    "ONE,stock,1,2000-01-03,2026-10-23,6700,,,",
    "SOLO,stock,1,2000-01-03,2026-10-23,6700,,,",
    "JAVA,stock,1,2007-08-27,2010-01-26,600,,,",
    "JAVA,etf,2,2021-10-05,2025-07-03,900,,,",
    "AAIC-DELISTED,stock,1,2000-01-03,2023-12-13,6000,,,",
    "AIW-DELISTED,stock,1,2013-05-06,2021-08-05,2000,,,",
    "AIC-DELISTED,stock,1,2015-03-23,2024-01-05,2200,,,",
    "CUT-DELISTED,stock,1,2000-01-03,2015-06-30,3800,,,",
]) + "\n"


def make_store(src=SRC):
    return {"_catalog/identity/v2/identity.csv": IDENT,
            "_catalog/identity/v2/manifest.json": json.dumps({
                "version": 2, "mpq_symbols": "_catalog/_src/mpq_symbols/v1",
                "mpq_symbols_sha256": hashlib.sha256(src.encode("utf-8")).hexdigest()}),
            "_catalog/_src/mpq_symbols/v1/mpq_symbols.csv": src}


store = make_store()
adds, summ = SM.plan(set(store), store.get)
out = dict(adds)
check("發布到候選 v1、manifest 最後", adds[-1][0] == "_catalog/series/candidates/v1/manifest.json"
      and all(p.startswith("_catalog/series/candidates/v1/") for p in out))
g = {r["entity_id"]: r for r in rows(out["_catalog/series/candidates/v1/groups.csv"])}
p = rows(out["_catalog/series/candidates/v1/pairs.csv"])


def pr(e):
    return [x for x in p if x["entity_id"] == e]


check("只列多代號的公司（ONE、SOLO、JAVA 不列）", set(g) == {"FIGI:META", "FIGI:TT", "FIGI:GAP", "FIGI:AI"}, set(g))
check("META：重疊、正本建議 META（覆蓋最長）", g["FIGI:META"]["kind"] == "overlap" and g["FIGI:META"]["proposed_primary"] == "META")
check("META：一對重疊核對、留 META", [(x["sym_a"], x["sym_b"], x["check"], x["proposed_action"]) for x in pr("FIGI:META")]
      == [("FB-DELISTED", "META", "overlap_identity", "dedupe_keep_b")], pr("FIGI:META"))
check("META：重疊段＝2012-05-18～2022-06-08", (pr("FIGI:META")[0]["from"], pr("FIGI:META")[0]["to"]) == ("2012-05-18", "2022-06-08"))
check("TTAC→ABFL：前後接、建議拼接、空檔 1 天",
      g["FIGI:TT"]["kind"] == "sequential" and [(x["sym_a"], x["sym_b"], x["check"], x["proposed_action"], x["gap_days"])
                                                for x in pr("FIGI:TT")] == [("TTAC", "ABFL", "splice_continuity", "splice", "1")])
check("空檔過長 → gap、hold", g["FIGI:GAP"]["kind"] == "gap" and pr("FIGI:GAP")[0]["proposed_action"] == "hold"
      and int(pr("FIGI:GAP")[0]["gap_days"]) > 10)
ai = pr("FIGI:AI")
check("三個代號：兩兩重疊各一對；含正本 AAIC-DELISTED 的留它，另一對留較長的 AIC-DELISTED",
      g["FIGI:AI"]["proposed_primary"] == "AAIC-DELISTED" and g["FIGI:AI"]["n_symbols"] == "3"
      and sorted((x["sym_a"], x["sym_b"], x["proposed_action"]) for x in ai if x["check"] == "overlap_identity")
      == [("AAIC-DELISTED", "AIC-DELISTED", "dedupe_keep_a"), ("AAIC-DELISTED", "AIW-DELISTED", "dedupe_keep_a"),
          ("AIW-DELISTED", "AIC-DELISTED", "dedupe_keep_b")], ai)
check("symbols 欄帶首末日", "FB-DELISTED@2012-05-18~2022-06-08" in g["FIGI:META"]["symbols"])
iv = SM.intervals(rows(IDENT), rows(SRC))
check("身份段 to 會截斷 bar 期間", iv[("FIGI:CUT", "stock")][0]["last"] == "2010-01-03", iv[("FIGI:CUT", "stock")])
check("pair_id 連號", [x["pair_id"] for x in p] == ["P%05d" % i for i in range(1, len(p) + 1)])
man = json.loads(out["_catalog/series/candidates/v1/manifest.json"])
check("manifest 記身份表版本與統計", man["identity_version"] == 2 and man["stats"]["overlap"] == 2
      and man["stats"]["sequential"] == 1 and man["stats"]["gap"] == 1, man["stats"])
s2 = dict(store)
s2.update(out)
adds2, summ2 = SM.plan(set(s2), s2.get)
check("輸入未變不發新版", adds2 == [] and "沿用候選 v1" in summ2, summ2)
bad = make_store()
bad["_catalog/_src/mpq_symbols/v1/mpq_symbols.csv"] += "X,stock,1,2020-01-02,2020-01-03,2,,,\n"
try:
    SM.plan(set(bad), bad.get)
    check("輸入 sha256 不符要停", False)
except SystemExit:
    check("輸入 sha256 不符要停", True)

# ── 正本發布（2026-10-30）──
def _g(e, asset, *items):
    return {"entity_id": e, "asset": asset, "symbols": "|".join(items)}


def _p(pid, e, a, b, check, act, asset="stock"):
    return {"pair_id": pid, "entity_id": e, "asset": asset, "sym_a": a, "sym_b": b, "check": check, "proposed_action": act}


def _c(p, verdict, basis="actual", med="1.0", jr=""):
    return dict({k: p[k] for k in ("pair_id", "entity_id", "asset", "sym_a", "sym_b", "check")}, verdict=verdict,
                price_basis=basis, median_ratio_b_over_a=med, junction_ratio=jr)


OV, SP = "overlap_identity", "splice_continuity"
G = [_g("E1", "stock", "OLD@2000-01-03~2010-01-01", "NEW@2005-01-03~2026-10-01"),
     _g("E2", "stock", "AA@2000-01-03~2010-01-01", "BB@2010-01-04~2026-10-01"),
     _g("E3", "stock", "XA@2000-01-03~2026-10-01", "XB@2001-01-03~2026-10-01"),
     _g("E4", "stock", "CH1@2000-01-03~2020-01-01", "CH2@2001-01-03~2020-01-01", "CH3@2002-01-03~2026-10-01")]
P = [_p("P1", "E1", "OLD", "NEW", OV, "dedupe_keep_b"), _p("P2", "E2", "AA", "BB", SP, "splice"),
     _p("P3", "E3", "XA", "XB", OV, "dedupe_keep_a"), _p("P4", "E4", "CH1", "CH2", OV, "dedupe_keep_b"),
     _p("P5", "E4", "CH2", "CH3", OV, "dedupe_keep_b")]
C = [_c(P[0], "duplicate", "mpq_adjusted"), _c(P[1], "continuous", jr="1.02"), _c(P[2], "duplicate", med="0.990099"),
     _c(P[3], "duplicate"), _c(P[4], "duplicate")]
s_rows, a_rows, h_rows, s_st = SM.build_series(G, P, C, 1)
s_by = {r["entity_id"]: r for r in s_rows}
a_by = {r["symbol"]: r for r in a_rows}
check("正本：去重留被保留的代號", s_by["E1"]["primary_symbol"] == "NEW" and a_by["OLD"]["role"] == "duplicate")
check("正本：調整價的 duplicate 照收", a_by["OLD"]["price_basis"] == "mpq_adjusted")
check("正本：拼接主代號取末日最晚", s_by["E2"]["primary_symbol"] == "BB" and a_by["AA"]["role"] == "splice")
check("正本：比值偏離 1% 不併、進 hold", "E3" not in s_by and h_rows[0]["pair_id"] == "P3" and h_rows[0]["reason"] == "ratio_off", h_rows)
check("正本：鏈式去重只留一個主代號", s_by["E4"]["primary_symbol"] == "CH3" and s_by["E4"]["n_members"] == 3
      and a_by["CH1"]["primary_symbol"] == "CH3", s_by["E4"])
check("正本：組的首末日", s_by["E4"]["first"] == "2000-01-03" and s_by["E4"]["last"] == "2026-10-01")
check("正本：統計", s_st["dedupe"] == 3 and s_st["splice"] == 1 and s_st["hold"] == 1, s_st)
h2 = SM.build_series(G, P, [_c(P[0], "duplicate"), _c(P[1], "continuous", "mpq_adjusted"), C[2], C[3], C[4]], 1)[2]
check("正本：接縫只有調整價進 hold", {r["pair_id"]: r["reason"] for r in h2}.get("P2") == "not_actual", h2)
P_h = [dict(P[0]), dict(P[1], proposed_action="hold"), P[2], P[3], P[4]]
check("正本：候選表本就 hold 不拼", {r["pair_id"]: r["reason"] for r in SM.build_series(G, P_h, C, 1)[2]}.get("P2") == "candidate_hold")


def s_fails(name, fn):
    try:
        fn()
        check(name, False)
    except SystemExit:
        check(name, True)


h3 = SM.build_series(G, P, C[:-1], 1)
check("正本：沒核過的對進 hold", {r["pair_id"]: r["reason"] for r in h3[2]}.get("P5") == "unchecked", h3[2])
check("正本：候選表沒有的核對結果不用、有記數", SM.build_series(G, P[:-1], C, 1)[3]["check_unused"] == 1)
P_id = [dict(p, pair_id="N" + p["pair_id"]) for p in P]
check("正本：pair_id 變了仍按代號對上", SM.build_series(G, P_id, C, 1)[3]["dedupe"] == 3)
OV_T = "sym_a,sym_b,asset,source,evidence,note\nAA,BB,stock,sec,公告,\n"
O = SM.parse_overrides(OV_T)
P_o = [dict(P[0]), dict(P[1], proposed_action="hold"), P[2], P[3], P[4]]
o_rows = SM.build_series(G, P_o, C, 1, O)
check("正本：放行清單可拼候選 hold 的對", o_rows[3]["splice"] == 1, o_rows[3])
s_fails("正本：放行但只有調整價要停", lambda: SM.build_series(G, P_o, [C[0], _c(P[1], "continuous", "mpq_adjusted"), C[2], C[3], C[4]], 1, O))
s_fails("正本：放行但不是 continuous 要停", lambda: SM.build_series(G, P_o, [C[0], _c(P[1], "jump"), C[2], C[3], C[4]], 1, O))
s_fails("正本：放行的對不在候選表要停", lambda: SM.build_series(G, P_o, C, 1, SM.parse_overrides(OV_T.replace("AA,BB", "ZZ,YY"))))
s_fails("正本：放行缺證據要停", lambda: SM.parse_overrides(OV_T.replace("公告", "")))
s_fails("正本：放行重複要停", lambda: SM.parse_overrides(OV_T + "AA,BB,stock,sec,公告,\n"))
G2 = G + [_g("E5", "stock", "M1@2000-01-03~2020-01-01", "M2@2001-01-03~2020-01-01", "M3@2002-01-03~2020-01-01")]
P2 = P + [_p("P6", "E5", "M1", "M2", OV, "dedupe_keep_a"), _p("P7", "E5", "M2", "M3", OV, "dedupe_keep_a"),
          _p("P8", "E5", "M1", "M3", OV, "dedupe_keep_a")]
s_fails("正本：同組矛盾要停", lambda: SM.build_series(G2, P2, C + [_c(P2[5], "duplicate"), _c(P2[6], "duplicate"), _c(P2[7], "different")], 1))
check("正本：不矛盾的 hold 可在同組", SM.build_series(G2, P2, C + [_c(P2[5], "duplicate"), _c(P2[6], "duplicate"), _c(P2[7], "partial")], 1)[3]["hold"] == 2)

print("\n%d 通過、%d 失敗" % (len(PASS), len(FAIL)))
sys.exit(1 if FAIL else 0)
