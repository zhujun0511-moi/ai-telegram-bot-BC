# -*- coding: utf-8 -*-
"""
test_catalog_rules.py — 拆股表、交易日曆發布（catalog_rules.py）離線測試：
假的 dataset 與假的 Polygon 記錄（記憶體裡），不連網。用法：python tests/test_catalog_rules.py
"""
import csv
import io
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import catalog_rules as CR   # noqa: E402

PASS, FAIL = [], []


def check(name, ok, info=""):
    (PASS if ok else FAIL).append(name)
    print(("✅ " if ok else "❌ ") + name + ("" if ok else "  " + str(info)))


def rows(text):
    return list(csv.DictReader(io.StringIO(text)))


# ── 假身份表 v1：SUNW 前段昇陽（2010 下市）、後段 Sunworks；FB→META 改名；BRK.B ──
IDENT = ("entity_id,source,symbol,from,to,evidence,note,table_version\n"
         "FIGI:SUNMICRO,polygon,SUNW,,2010-01-27,polygon,,1\n"
         "FIGI:SUNWORKS,polygon,SUNW,2015-01-01,,polygon,代號易主,1\n"
         "FIGI:META,polygon,FB,,2022-06-09,polygon,,1\n"
         "FIGI:META,polygon,META,2022-06-09,,polygon,,1\n"
         "FIGI:BRKB,polygon,BRK.B,,,polygon,,1\n"
         "FIGI:NVDA,polygon,NVDA,,,polygon,,1\n")
store = {"_catalog/identity/v1/identity.csv": IDENT,
         "_catalog/identity/v1/manifest.json": json.dumps({"version": 1})}


def read(p):
    return store.get(p)


RAW = [
    {"id": "a1", "ticker": "SUNW", "execution_date": "2019-08-12", "split_from": 7, "split_to": 1},   # Sunworks 合股
    {"id": "a2", "ticker": "SUNW", "execution_date": "2000-12-05", "split_from": 1, "split_to": 2},   # 昇陽（2000 低信心）
    {"id": "a3", "ticker": "NVDA", "execution_date": "2024-06-10", "split_from": 1, "split_to": 10},
    {"id": "a3b", "ticker": "NVDA", "execution_date": "2024-06-10", "split_from": 1, "split_to": 10},  # 重複
    {"id": "a4", "ticker": "BRK.B", "execution_date": "2010-01-21", "split_from": 1, "split_to": 50},
    {"id": "a5", "ticker": "BDX", "execution_date": "2022-04-01", "split_from": 1, "split_to": 1.272},  # 疑似分拆
    {"id": "a6", "ticker": "FB", "execution_date": "2012-01-01", "split_from": 2, "split_to": 3},      # 3:2
    {"id": "a7", "ticker": "SUNW", "execution_date": "2012-05-01", "split_from": 1, "split_to": 2},   # 兩段之間的空窗
]

# classify
check("classify 1→10 拆股 high", CR.classify(1, 10, "2024-06-10") == ("split", "10", "high", ""))
check("classify 7→1 合股", CR.classify(7, 1, "2019-08-12")[0:3] == ("reverse_split", "0.1428571429", "high"))
check("classify 2000 年低信心", CR.classify(1, 2, "2000-12-05")[2] == "low")
check("classify 1.272 疑似分拆", CR.classify(1, 1.272, "2022-04-01")[0] == "suspect_spinoff")
check("classify 1000→1272 疑似分拆", CR.classify(1000, 1272, "2022-04-01")[0] == "suspect_spinoff")
check("classify 3:2 常見", CR.classify(2, 3, "2012-01-01")[0] == "split")
check("classify 1:200 合股常見", CR.classify(200, 1, "2020-01-01")[0] == "reverse_split")
check("classify 1:1 不算拆股", CR.classify(1, 1, "2020-01-01")[0] == "suspect_spinoff")
check("classify 0 不崩", CR.classify(0, 1, "2020-01-01")[0] == "suspect_spinoff")

ident_rows = rows(IDENT)
r, unm, st = CR.split_rows(RAW, ident_rows, 1)
by = {(x["symbol"], x["ex_date"]): x for x in r}
check("SUNW 2019 歸 Sunworks（防代號重用）", by[("SUNW", "2019-08-12")]["entity_id"] == "FIGI:SUNWORKS")
check("SUNW 2000 歸昇陽", by[("SUNW", "2000-12-05")]["entity_id"] == "FIGI:SUNMICRO")
check("SUNW 兩段之間的空窗進 unmapped", any(u["ex_date"] == "2012-05-01" for u in unm), unm)
check("BRK.B 點形對得上", by[("BRK.B", "2010-01-21")]["entity_id"] == "FIGI:BRKB")
check("FB 改名前歸 META 公司", by[("FB", "2012-01-01")]["entity_id"] == "FIGI:META")
check("BDX 身份表沒有→unmapped", any(u["symbol"] == "BDX" for u in unm))
check("重複只留一筆", st.get("dup") == 1 and sum(1 for x in r if x["symbol"] == "NVDA") == 1, st)
check("evidence＝Polygon id", by[("NVDA", "2024-06-10")]["evidence"] == "a3")

adds, summ = CR.plan_splits(set(store), read, RAW)
paths = [p for p, _ in adds]
check("splits 發布 v1", paths[0].startswith("_catalog/splits/v1/") and paths[-1] == "_catalog/splits/v1/manifest.json", paths)
man = json.loads(adds[-1][1])
check("manifest 記 identity_version", man["identity_version"] == 1)
check("manifest sha256 對得上", all(man["files"][p.split("/")[-1]]["sha256"] == CR._sha(t) for p, t in adds[:-1]))
for p, t in adds:
    store[p] = t
adds2, summ2 = CR.plan_splits(set(store), read, RAW)
check("內容沒變不發新版", adds2 == [] and "沿用 v1" in summ2, summ2)
adds3, _ = CR.plan_splits(set(store), read, RAW + [{"id": "z", "ticker": "NVDA", "execution_date": "2021-07-20",
                                                     "split_from": 1, "split_to": 4}])
check("內容變了發 v2", adds3 and adds3[0][0].startswith("_catalog/splits/v2/"))
check("v2 的 table_version＝2", rows(dict(adds3)["_catalog/splits/v2/splits.csv"])[0]["table_version"] == "2")

# ── 日曆 ──
y2027 = {r["date"]: r for r in CR.nyse_year(2027)}
check("2027 元旦 1/1（週五）", y2027.get("2027-01-01", {}).get("type") == "full_closure")
check("2027 聖誕 12/25 週六→12/24 休市", y2027.get("2027-12-24", {}).get("type") == "full_closure")
check("2027 獨立日 7/4 週日→7/5", "2027-07-05" in y2027 and "2027-07-03" not in y2027)
check("2027 感恩節隔天半日 13:00", y2027.get("2027-11-26", {}).get("close") == "13:00")
check("2027 耶穌受難日 3/26", y2027.get("2027-03-26", {}).get("type") == "full_closure")
check("2021 沒有六月節", not any("Juneteenth" in r["reason"] for r in CR.nyse_year(2021)))
check("2022 元旦落週六不補休", "2021-12-31" not in {r["date"] for r in CR.nyse_year(2022)}
      and not any(r["date"].startswith("2022-01") and "New Year" in r["reason"] for r in CR.nyse_year(2022)))

CSV = ("Date,DayOfWeek,Type,TradingHours,Reason\n"
       "2025-01-01,Wednesday,Full Closure,All Day,New Year's Day\n"
       "2025-01-09,Thursday,Full Closure,All Day,National Day of Mourning - President Jimmy Carter\n"
       "2025-07-03,Thursday,Early Close,09:30 - 13:00 EST,Independence Day Eve\n"
       "2026-01-01,Thursday,Full Closure,All Day,New Year's Day\n"
       "2026-09-11,Friday,Delayed Open,12:00 - 16:00 EST,test delayed\n"
       "2026-06-01,Monday,Unscheduled Early Close,09:30 - 15:56 EST,glitch\n")
cr = CR.parse_calendar_csv(CSV)
d = {x["date"]: x for x in cr}
check("CSV 延遲開盤 12:00", d["2026-09-11"]["type"] == "delayed_open" and d["2026-09-11"]["open"] == "12:00")
check("CSV 臨時提早收盤 15:56", d["2026-06-01"]["type"] == "early_close" and d["2026-06-01"]["close"] == "15:56"
      and d["2026-06-01"]["reason"].startswith("Unscheduled"))
check("CSV 休市日時段空", d["2025-01-09"]["open"] == "" and d["2025-01-09"]["close"] == "")
try:
    CR.parse_calendar_csv("Date,DayOfWeek,Type,TradingHours,Reason\n2025-01-02,Thursday,Weird,All Day,x\n")
    check("認不得的 Type 報錯", False)
except SystemExit:
    check("認不得的 Type 報錯", True)

cal, xc, yrs = CR.calendar_rows(CSV, [2027], 1)
check("CSV 年份 2025～2026", yrs == (2025, 2026))
check("2027 由規則補上", any(r["date"].startswith("2027") and r["source"] == "rule" for r in cal))
check("CSV 內容原樣保留", sum(1 for r in cal if r["source"] == "local_csv") == 6)
xd = {x["date"]: x for x in xc}
check("交叉核對抓到卡特國喪日（只在 CSV）", xd.get("2025-01-09", {}).get("note", "").startswith("只在 CSV"))
check("交叉核對抓到 CSV 缺的節日（只在規則）", "2025-12-25" in xd and xd["2025-12-25"]["csv_type"] == "")
try:
    CR.calendar_rows(CSV, [2026], 1)
    check("規則年份與 CSV 重疊報錯", False)
except SystemExit:
    check("規則年份與 CSV 重疊報錯", True)

cstore = {}
adds, summ = CR.plan_calendar(set(cstore), cstore.get, CSV, [2027])
check("calendar 發布 v1、manifest 最後", adds[-1][0] == "_catalog/calendar/v1/manifest.json", [p for p, _ in adds])
for p, t in adds:
    cstore[p] = t
adds2, summ2 = CR.plan_calendar(set(cstore), cstore.get, CSV, [2027])
check("日曆內容沒變不發新版", adds2 == [], summ2)
adds3, _ = CR.plan_calendar(set(cstore), cstore.get, CSV, [2027, 2028])
check("加 2028 發 v2", adds3 and adds3[0][0].startswith("_catalog/calendar/v2/"))

# ── 拆股表 v2：身份表 v2 有 mpq 列（2026-10-25 A8 重掛）──
import hashlib  # noqa: E402

ID2 = (IDENT.replace("table_version\n", "table_version,asset\n", 1).replace(",1\n", ",2,\n")
       + "FIGI:SUNMICRO,mpq,SUNW-DELISTED,2000-01-03,2010-01-27,mpq_overlap,,2,stock\n"
       + "FIGI:SUNWORKS,mpq,SUNW,2015-03-04,,mpq_overlap,,2,stock\n"
       + "FIGI:BRKB,mpq,BRK.B,2000-01-03,,mpq_overlap,,2,stock\n"
       + "MPQ:OLDCO-DELISTED,mpq,OLDCO-DELISTED,2000-01-03,,mpq_only,,2,stock\n"     # Polygon 沒有的舊公司
       + "FIGI:NEWCO,mpq,FBX,2018-01-02,,mpq_overlap,,2,stock\n"                    # Polygon 段歸 FIGI:NEWCO
       + "MPQ:FBX-DELISTED,mpq,FBX-DELISTED,2005-01-03,,mpq_only,,2,stock\n"         # MPQ 該日數據屬舊公司
       + "FIGI:NEWCO,polygon,FBX,,,polygon,,2,\n"
       + "MPQ:DUPA,mpq,DUP,2010-01-04,,mpq_only,,2,stock\n"
       + "MPQ:DUPB-DELISTED,mpq,DUP-DELISTED,2010-01-04,,mpq_only,,2,stock\n")
SRC2 = ("mpq_symbol,asset,seg,d_first,d_last,d_days,m5_first,m5_last,m5_days\n"
        "SUNW-DELISTED,stock,1,2000-01-03,2010-01-26,2500,,,\n"
        "SUNW,stock,1,2015-03-04,2024-02-14,2000,,,\n"
        "BRK.B,stock,1,2000-01-03,2026-10-23,6700,,,\n"
        "OLDCO-DELISTED,stock,1,2000-01-03,2008-06-30,2100,,,\n"
        "FBX,stock,1,2018-01-02,2026-10-23,2200,,,\n"
        "FBX-DELISTED,stock,1,2005-01-03,2015-12-31,2700,,,\n"
        "DUP,stock,1,2010-01-04,2020-12-31,2700,,,\n"
        "DUP-DELISTED,stock,1,2010-01-04,2020-12-31,2700,,,\n")
RAW2 = RAW + [
    {"id": "b1", "ticker": "OLDCO", "execution_date": "2005-05-02", "split_from": 1, "split_to": 2},   # v1 對不上 → MPQ 補掛
    {"id": "b2", "ticker": "OLDCO", "execution_date": "2012-05-01", "split_from": 1, "split_to": 2},   # 末根後 → 仍對不上
    {"id": "b3", "ticker": "FBX", "execution_date": "2012-06-01", "split_from": 1, "split_to": 3},     # Polygon 與 MPQ 不同家
    {"id": "b4", "ticker": "DUP", "execution_date": "2014-06-02", "split_from": 1, "split_to": 2},     # MPQ 同代號兩家
]
store2 = {"_catalog/identity/v1/identity.csv": IDENT, "_catalog/identity/v1/manifest.json": json.dumps({"version": 1}),
          "_catalog/identity/v2/identity.csv": ID2,
          "_catalog/identity/v2/manifest.json": json.dumps({
              "version": 2, "mpq_symbols": "_catalog/_src/mpq_symbols/v1",
              "mpq_symbols_sha256": hashlib.sha256(SRC2.encode("utf-8")).hexdigest()}),
          "_catalog/_src/mpq_symbols/v1/mpq_symbols.csv": SRC2}
adds_v2, summ_v2 = CR.plan_splits(set(store2), store2.get, RAW2)
o2 = dict(adds_v2)
sp2 = rows(o2["_catalog/splits/v1/splits.csv"])
un2 = rows(o2["_catalog/splits/v1/unmapped.csv"])
b2 = {(x["symbol"], x["ex_date"]): x for x in sp2}
m2 = json.loads(o2["_catalog/splits/v1/manifest.json"])
check("v2：manifest 記身份表 v2 與 mpq 輸入", m2["identity_version"] == 2 and m2["mpq_symbols"] == "_catalog/_src/mpq_symbols/v1")
check("v2：多 mpq_symbols 欄", "mpq_symbols" in sp2[0])
check("v2：Polygon 與 MPQ 一致者照舊、帶 MPQ 代號",
      b2[("SUNW", "2000-12-05")]["entity_id"] == "FIGI:SUNMICRO" and b2[("SUNW", "2000-12-05")]["mpq_symbols"] == "SUNW-DELISTED"
      and b2[("BRK.B", "2010-01-21")]["mpq_symbols"] == "BRK.B")
check("v2：Polygon 對不上、MPQ 有 → 補掛", b2[("OLDCO", "2005-05-02")]["entity_id"] == "MPQ:OLDCO-DELISTED"
      and "經 MPQ" in b2[("OLDCO", "2005-05-02")]["note"], b2.get(("OLDCO", "2005-05-02")))
check("v2：MPQ 末根之後不補掛", any(u["symbol"] == "OLDCO" and u["ex_date"] == "2012-05-01" for u in un2), un2)
check("v2：Polygon 與 MPQ 不同家 → 照 MPQ、低信心",
      b2[("FBX", "2012-06-01")]["entity_id"] == "MPQ:FBX-DELISTED" and b2[("FBX", "2012-06-01")]["confidence"] == "low")
check("v2：MPQ 同代號兩家、Polygon 對不上 → unmapped", any(u["symbol"] == "DUP" for u in un2))
check("v2：SUNW 空窗（2012，兩家都沒數據）仍 unmapped", any(u["symbol"] == "SUNW" and u["ex_date"] == "2012-05-01" for u in un2))
check("v2：Sunworks 2019 合股帶 MPQ 代號 SUNW", b2[("SUNW", "2019-08-12")]["entity_id"] == "FIGI:SUNWORKS"
      and b2[("SUNW", "2019-08-12")]["mpq_symbols"] == "SUNW")
bad2 = dict(store2)
bad2["_catalog/_src/mpq_symbols/v1/mpq_symbols.csv"] = SRC2 + "X,stock,1,2020-01-02,2020-01-03,2,,,\n"
try:
    CR.plan_splits(set(bad2), bad2.get, RAW2)
    check("v2：mpq 輸入 sha256 不符要停", False)
except SystemExit:
    check("v2：mpq 輸入 sha256 不符要停", True)

print("\n%d 通過，%d 失敗" % (len(PASS), len(FAIL)))
sys.exit(1 if FAIL else 0)
