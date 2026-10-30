# -*- coding: utf-8 -*-
"""
test_identity_mpq.py — 身份表 v2（identity_mpq.py）離線測試：假的身份表 v1 與假的 MPQ 代號摘要（記憶體裡），不連網。
用法：python tests/test_identity_mpq.py
"""
import csv
import hashlib
import io
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import identity_mpq as IM   # noqa: E402

IM.CUTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_no_such_cuts.csv")   # 假資料不套真清單
PASS, FAIL = [], []


def check(name, ok, info=""):
    (PASS if ok else FAIL).append(name)
    print(("✅ " if ok else "❌ ") + name + ("" if ok else "  " + str(info)))


def rows(text):
    return list(csv.DictReader(io.StringIO(text)))


# ── 假身份表 v1（Polygon）──
# SUNW：昇陽 2010-01-27 結束、之後 Sunworks；AVB：2026-08-18 結束；BKYI：現役；BRK-B：點換橫線；
# DEAD：2015-06-01 結束；FEED：2026-09-16 結束（兩個 MPQ 代號都會掛上去）
IDENT = ("entity_id,source,symbol,from,to,evidence,note,table_version\n"
         "FIGI:SUNMICRO,polygon,SUNW,,2010-01-27,polygon,,1\n"
         "FIGI:SUNWORKS,polygon,SUNW,2010-01-27,,polygon,代號易主,1\n"
         "FIGI:AVB,polygon,AVB,,2026-08-18,polygon,,1\n"
         "FIGI:BKYI,polygon,BKYI,,,polygon,,1\n"
         "FIGI:BRKB,polygon,BRK-B,,,polygon,,1\n"
         "FIGI:DEAD,polygon,DEAD,,2015-06-01,polygon,,1\n"
         "FIGI:FEED,polygon,FEED,,2026-09-16,polygon,,1\n"
         "FIGI:FBETF,polygon,FB,,,polygon,,1\n"                      # 2025 起的新 ETF；Meta 用 FB 那段 Polygon 沒留
         "FIGI:OLDX,polygon,NODT,,0000-00-00,polygon,,1\n"          # Polygon 標已下市但沒有結束日
         "FIGI:AMB1,polygon,AMBG,,0000-00-00,polygon,,1\n"
         "FIGI:AMB2,polygon,AMBG,0000-00-00,,polygon,代號易主,1\n")
ENTS = ("entity_id,name,type,active\n"
        "FIGI:SUNMICRO,SUN MICRO,CS,0\nFIGI:SUNWORKS,SUNWORKS,CS,1\nFIGI:AVB,AVALONBAY,CS,0\n"
        "FIGI:BKYI,BIO-KEY,CS,1\nFIGI:BRKB,BERKSHIRE B,CS,1\nFIGI:DEAD,DEAD CO,CS,0\nFIGI:FEED,FEED CO,CS,0\n"
        "FIGI:FBETF,FB ETF,ETF,1\n")
COLS = "mpq_symbol,asset,seg,d_first,d_last,d_days,m5_first,m5_last,m5_days\n"
SRC = COLS + "\n".join([
    "AVB,stock,1,,,,2000-01-03,2000-01-03,1",                                      # 改名殘渣（Polygon 也有）
    "AVB-DELISTED,stock,1,2000-01-04,2026-08-14,6700,2000-01-04,2026-08-14,6700",  # A2
    "SUNW-DELISTED,stock,1,2000-01-03,2010-01-26,2500,2000-01-03,2010-01-26,2500",
    "SUNW,stock,1,2000-01-03,2024-02-01,6000,2000-01-03,2024-02-01,6000",          # A3：跨易主邊界要切
    "BKYI-DELISTED,stock,1,2001-01-02,2025-03-01,5000,2001-01-02,2025-03-01,5000",  # 標下市、Polygon 現役
    "BRK.B,stock,1,2003-09-10,2026-10-23,5800,2000-01-03,2026-10-23,6700",
    "DEAD-DELISTED,stock,1,2000-01-03,2015-06-05,3800,2000-01-03,2015-06-05,3800",  # 末根晚 4 天：容差內
    "LATE-DELISTED,stock,1,2000-01-03,2019-01-01,4000,,,",                          # Polygon 沒有
    "LATE,stock,1,,,,1999-12-31,1999-12-31,1",                                      # 殘渣：併入 LATE-DELISTED
    "WART,stock,1,2010-01-04,2010-03-01,40,,,",                                     # 權證類：Polygon 沒有
    "FEED,stock,1,,,,2015-01-02,2023-12-29,2175",
    "FEED-DELISTED,stock,1,2000-01-03,2020-12-31,5000,,,",                          # 同家重疊
    "JAVA,stock,1,2007-08-27,2010-01-26,600,,,",                                    # A6：換類別
    "JAVA,etf,2,2021-10-05,2025-07-03,900,,,",
    "FIEE,stock,1,2016-02-18,2026-10-23,2600,,,",
    "FIEE,etf,2,,,,2016-02-29,2016-02-29,1",                                        # A6：ETF 被切碎 → 併
    "FIEE,etf,3,,,,2016-03-15,2016-03-16,2",
    "FIEE,etf,4,,,,2016-03-28,2016-03-30,3",
    "NA,stock,1,2022-07-13,2026-10-23,1000,,,",
    "NODT-DELISTED,stock,1,2001-01-02,2012-05-01,2800,,,",                         # 只有一段、沒結束日
    "AMBG,stock,1,2018-01-02,2026-10-23,2200,,,",
    "FB-DELISTED,stock,1,2012-05-18,2022-06-08,2531,,,",
    "FB,etf,1,2025-06-26,2026-10-23,330,,,",                                   # 多段＋沒結束日 → 不猜
]) + "\n"


def make_store(src=SRC):
    src_man = {"version": 1, "files": {"mpq_symbols.csv": {"sha256": hashlib.sha256(src.encode()).hexdigest(),
                                                           "rows": src.count("\n") - 1}},
               "calendar": ["2000-01-03", "2026-10-23"]}
    return {"_catalog/identity/v1/identity.csv": IDENT, "_catalog/identity/v1/entities.csv": ENTS,
            "_catalog/identity/v1/renames.csv": "entity_id,old_ticker,new_ticker,date,old_name,new_name\n",
            "_catalog/identity/v1/manifest.json": json.dumps({"version": 1}),
            "_catalog/_src/mpq_symbols/v1/mpq_symbols.csv": src,
            "_catalog/_src/mpq_symbols/v1/manifest.json": json.dumps(src_man)}


store = make_store()
adds, summ = IM.plan(set(store), store.get)
out = dict(adds)
check("發布到 v2、manifest 最後", adds[-1][0] == "_catalog/identity/v2/manifest.json"
      and all(p.startswith("_catalog/identity/v2/") for p in out), [p for p, _ in adds])
ident = rows(out["_catalog/identity/v2/identity.csv"])
mpq = [r for r in ident if r["source"] == "mpq"]
poly = [r for r in ident if r["source"] == "polygon"]
probs = rows(out["_catalog/identity/v2/mpq_problems.csv"])


def seg(sym, asset="stock"):
    return [r for r in mpq if r["symbol"] == sym and r["asset"] == asset]


def prob(kind, sym=None):
    return [p for p in probs if p["kind"] == kind and (sym is None or p["symbol"] == sym)]


check("Polygon 列原樣保留、版本改 2", len(poly) == 11 and all(r["table_version"] == "2" and r["asset"] == "" for r in poly)
      and [(r["entity_id"], r["symbol"], r["from"], r["to"]) for r in poly if r["symbol"] == "SUNW"]
      == [("FIGI:SUNMICRO", "SUNW", "", "2010-01-27"), ("FIGI:SUNWORKS", "SUNW", "2010-01-27", "")])
check("A2：AVB-DELISTED 對到 AVB、to＝Polygon 結束日",
      [(r["entity_id"], r["from"], r["to"], r["evidence"]) for r in seg("AVB-DELISTED")]
      == [("FIGI:AVB", "2000-01-04", "2026-08-18", "mpq_overlap")], seg("AVB-DELISTED"))
check("A2：AVB 殘渣同一家", [r["entity_id"] for r in seg("AVB")] == ["FIGI:AVB"])
check("A3：SUNW 跨易主邊界切成兩段", [(r["entity_id"], r["from"], r["to"]) for r in seg("SUNW")]
      == [("FIGI:SUNMICRO", "2000-01-03", "2010-01-27"), ("FIGI:SUNWORKS", "2010-01-27", "")], seg("SUNW"))
check("A3：SUNW-DELISTED 只屬昇陽", [r["entity_id"] for r in seg("SUNW-DELISTED")] == ["FIGI:SUNMICRO"])
check("點號代號 BRK.B 對到 BRK-B、symbol 保留 MPQ 寫法",
      [(r["entity_id"], r["symbol"], r["to"]) for r in seg("BRK.B")] == [("FIGI:BRKB", "BRK.B", "")])
check("A7：容差內末根晚 4 天 → 同一段、to 延到末根隔天",
      [(r["entity_id"], r["to"]) for r in seg("DEAD-DELISTED")] == [("FIGI:DEAD", "2015-06-06")]
      and "晚 4 天" in seg("DEAD-DELISTED")[0]["note"], seg("DEAD-DELISTED"))
check("標下市但 Polygon 只剩現役段 → 不掛、自成一家、列問題",
      [r["entity_id"] for r in seg("BKYI-DELISTED")] == ["MPQ:BKYI-DELISTED"]
      and len(prob("delisted_polygon_active", "BKYI-DELISTED")) == 1)
check("FB-DELISTED 類（Polygon 舊段沒留下、代號給了新公司）不掛到新公司",
      [r["entity_id"] for r in seg("FB-DELISTED")] == ["MPQ:FB-DELISTED"]
      and [r["entity_id"] for r in seg("FB", "etf")] == ["FIGI:FBETF"])
check("一條序列被 Polygon 切開 → 列 split_by_polygon", len(prob("split_by_polygon", "SUNW")) == 1)
check("Polygon 沒有 → 自成一家 MPQ:、to 空、列問題",
      [(r["entity_id"], r["to"], r["evidence"]) for r in seg("LATE-DELISTED")]
      == [("MPQ:LATE-DELISTED", "", "mpq_only")] and len(prob("no_polygon", "LATE-DELISTED")) == 1)
check("Polygon 沒有的殘渣 → 併入 X-DELISTED、不列問題",
      [(r["entity_id"], r["evidence"]) for r in seg("LATE")] == [("MPQ:LATE-DELISTED", "mpq_residue")]
      and not prob("no_polygon", "LATE"))
check("權證類對不上 → 自成一家", [r["entity_id"] for r in seg("WART")] == ["MPQ:WART"])
check("同家重疊只報告（FEED；SUNW 前段與 SUNW-DELISTED 同屬昇陽也算），每對一筆",
      sorted(p["symbol"] for p in prob("same_entity_overlap")) == ["FEED", "SUNW"]
      and {r["entity_id"] for r in seg("FEED") + seg("FEED-DELISTED")} == {"FIGI:FEED"}, probs)
check("A6：JAVA 股票與 ETF 各一段", len(seg("JAVA")) == 1 and len(seg("JAVA", "etf")) == 1)
check("A6：FIEE 碎段（相隔 ≤31 天）併成一段", [(r["from"]) for r in seg("FIEE", "etf")] == ["2016-02-29"]
      and json.loads(out["_catalog/identity/v2/manifest.json"])["stats"]["merged_segments"] == 2)
check("Polygon 無結束日、只有一段 → 照掛、to 空、不算標下市仍現役",
      [(r["entity_id"], r["to"]) for r in seg("NODT-DELISTED")] == [("FIGI:OLDX", "")]
      and "沒有結束日" in seg("NODT-DELISTED")[0]["note"] and not prob("delisted_polygon_active", "NODT-DELISTED"))
check("Polygon 多段＋無結束日 → 自成一家、polygon_ambiguous",
      [r["entity_id"] for r in seg("AMBG")] == ["MPQ:AMBG"] and len(prob("polygon_ambiguous", "AMBG")) == 1)
check("NA 代號沒變成空值", len(seg("NA")) == 1 and seg("NA")[0]["entity_id"] == "MPQ:NA")
ents = rows(out["_catalog/identity/v2/entities.csv"])
check("entities：原 8 家＋MPQ 自成一家",
      len(ents) == 8 + len({r["entity_id"] for r in mpq if r["entity_id"].startswith("MPQ:")})
      and {"entity_id": "MPQ:LATE-DELISTED", "name": "", "type": "mpq_stock", "active": "0"} in ents
      and {"entity_id": "MPQ:NA", "name": "", "type": "mpq_stock", "active": "1"} in ents, ents)
man = json.loads(out["_catalog/identity/v2/manifest.json"])
check("manifest 記輸入與基底", man["polygon_base_version"] == 1 and man["mpq_symbols"] == "_catalog/_src/mpq_symbols/v1"
      and all(hashlib.sha256(out["_catalog/identity/v2/" + k].encode()).hexdigest() == v["sha256"]
              for k, v in man["files"].items()))
check("每個 MPQ 代號都有身份列", {r["symbol"] for r in mpq} == {r["mpq_symbol"] for r in rows(SRC)})
check("同代號同類別的段首尾相接", all(
    a["to"] == b["from"] for s in {(r["symbol"], r["asset"]) for r in mpq}
    for a, b in zip(seg(*s), seg(*s)[1:])))

# 再跑一次：最新版已含同一份輸入 → 不發新版
store2 = dict(store)
store2.update(out)
adds2, summ2 = IM.plan(set(store2), store2.get)
check("輸入未變不發新版", adds2 == [] and "沿用 v2" in summ2, summ2)

# sha256 不符 → 停
bad = make_store()
bad["_catalog/_src/mpq_symbols/v1/mpq_symbols.csv"] += "X,stock,1,2020-01-02,2020-01-03,2,,,\n"
try:
    IM.plan(set(bad), bad.get)
    check("輸入 sha256 不符要停", False)
except SystemExit:
    check("輸入 sha256 不符要停", True)

# 容差邊界：末根晚 11 天 → 切出一段自成一家＋polygon_ended
s3 = COLS + "DEAD-DELISTED,stock,1,2000-01-03,2015-06-12,3800,,,\n"
st3 = make_store(s3)
r3 = rows(dict(IM.plan(set(st3), st3.get)[0])["_catalog/identity/v2/identity.csv"])
p3 = rows(dict(IM.plan(set(st3), st3.get)[0])["_catalog/identity/v2/mpq_problems.csv"])
check("超出容差 → 切段、後段自成一家、列 polygon_ended",
      [(r["entity_id"], r["from"], r["to"]) for r in r3 if r["source"] == "mpq"]
      == [("FIGI:DEAD", "2000-01-03", "2015-06-01"), ("MPQ:DEAD-DELISTED", "2015-06-01", "")]
      and [p["kind"] for p in p3] == ["polygon_ended"], (r3, p3))

# ── 10-30 代號重用切分（ADR-060）──
CUT_H = "symbol,asset,hole_before,hole_after,first_trade,source,evidence,note\n"
CUT_T = CUT_H + "NEWCO,etf,2005-04-21,2020-09-10,2020-09-17,yahoo,Yahoo 上市 2020,\n"
C_ID = [{"entity_id": "FIGI:NEW", "source": "polygon", "symbol": "NEWCO", "from": "", "to": ""}]
C_SRC = [{"mpq_symbol": "NEWCO", "asset": "etf", "d_first": "2005-01-03", "d_last": "2005-04-21", "m5_first": "", "m5_last": ""},
         {"mpq_symbol": "NEWCO", "asset": "etf", "d_first": "2020-09-10", "d_last": "2026-10-01", "m5_first": "", "m5_last": ""}]
c_rows, c_ents, c_probs, c_st = IM.build(C_ID, C_SRC, 4, "2026-10-01", IM.parse_cuts(CUT_T))
c_rows.sort(key=lambda r: r["from"])
check("切分：洞前自成一家", c_rows[0]["entity_id"] == "MPQ:NEWCO:pre-2020-09-10" and c_rows[0]["evidence"] == "mpq_cut"
      and c_rows[0]["to"] == "2020-09-10", c_rows[0])
check("切分：洞後仍掛現任", c_rows[1]["entity_id"] == "FIGI:NEW" and c_rows[1]["from"] == "2020-09-10", c_rows[1])
check("切分：entities 與 problems 有記", c_ents[0]["entity_id"] == "MPQ:NEWCO:pre-2020-09-10" and c_ents[0]["active"] == 0
      and c_probs[0]["kind"] == "cut_reuse" and c_st["cut_reuse"] == 1, (c_ents, c_probs))
c_one = [dict(C_SRC[0], d_last="2026-10-01")]
c_rows2 = sorted(IM.build(C_ID, c_one, 4, "2026-10-01", IM.parse_cuts(CUT_T))[0], key=lambda r: r["from"])
check("切分：連續一段從洞後首日切開", [r["entity_id"] for r in c_rows2] == ["MPQ:NEWCO:pre-2020-09-10", "FIGI:NEW"]
      and c_rows2[1]["from"] == "2020-09-10", c_rows2)


def c_fails(name, fn):
    try:
        fn()
        check(name, False)
    except SystemExit:
        check(name, True)


c_fails("切分：切不出後段要停", lambda: IM.build(C_ID, C_SRC[:1], 4, "2026-10-01", IM.parse_cuts(CUT_T)))
c_fails("切分：切不出前段要停", lambda: IM.build(C_ID, C_SRC[1:], 4, "2026-10-01", IM.parse_cuts(CUT_T)))
c_fails("切分：代號對不到要停", lambda: IM.build(C_ID, C_SRC, 4, "2026-10-01", IM.parse_cuts(CUT_T.replace("NEWCO", "OTHER"))))
c_fails("切分：缺證據要停", lambda: IM.parse_cuts(CUT_T.replace("Yahoo 上市 2020", "")))
c_fails("切分：來源不在名單要停", lambda: IM.parse_cuts(CUT_T.replace("yahoo", "reddit")))
c_fails("切分：洞日期倒置要停", lambda: IM.parse_cuts(CUT_T.replace("2005-04-21", "2021-01-01")))
c_fails("切分：重複要停", lambda: IM.parse_cuts(CUT_T + CUT_T[len(CUT_H):]))
c_fails("切分：欄位不對要停", lambda: IM.parse_cuts("a,b\n"))
check("真清單：格式全過", len(IM.parse_cuts(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "catalog_src",
                                                     "identity_cuts.csv"), encoding="utf-8").read())) == 143)

print("\n%d 通過、%d 失敗" % (len(PASS), len(FAIL)))
sys.exit(1 if FAIL else 0)
