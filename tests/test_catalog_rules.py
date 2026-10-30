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

cal, xc, yrs, _h, _nf = CR.calendar_rows(CSV, [2027], 1)
check("沒給清單＝halts None、補正 0", _h is None and _nf == 0)
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

# ── 日曆 v2：補正清單＋停盤清單（2026-10-29 方案 A）──
FIX = ("date,type,open,close,reason,source,evidence\n"
       "2025-07-07,early_close,09:30,13:00,test early,exchange,NYSE notice x\n")
HALT = ("date,start,end,scope,kind,source,note\n"
        "2025-03-12,09:35,09:50,all,halt,exchange+press,熔斷\n"
        "2025-02-03,10:30,11:30,nyse_listed,feed_gap,local_data,原因未證實\n")


def raises(name, fn):
    try:
        fn()
        check(name, False)
    except SystemExit:
        check(name, True)


cal2, xc2, _, h2, nf2 = CR.calendar_rows(CSV, [2027], 3, FIX, HALT)
fx = [r for r in cal2 if r["source"] == "fix"]
check("補正列 source=fix、時段對", len(fx) == 1 and fx[0]["date"] == "2025-07-07" and fx[0]["close"] == "13:00" and nf2 == 1)
check("CSV 列不變", sum(1 for r in cal2 if r["source"] == "local_csv") == 6)
check("crosscheck 不受補正影響", xc2 == xc)
check("halts 按日期排、帶版本", [h["date"] for h in h2] == ["2025-02-03", "2025-03-12"] and h2[0]["table_version"] == 3)
raises("補正撞 CSV 日期報錯", lambda: CR.calendar_rows(CSV, [2027], 1, FIX.replace("2025-07-07", "2025-07-03"), None))
raises("補正撞規則年份報錯", lambda: CR.calendar_rows(CSV, [2027], 1, FIX.replace("2025-07-07", "2027-01-01")
                                                       .replace("early_close,09:30,13:00", "full_closure,,"), None))
raises("補正週末報錯", lambda: CR.calendar_rows(CSV, [2027], 1, FIX.replace("2025-07-07", "2025-07-05"), None))
raises("補正缺證據報錯", lambda: CR.calendar_rows(CSV, [2027], 1, FIX.replace(",NYSE notice x", ","), None))
raises("補正來源不在名單報錯", lambda: CR.calendar_rows(CSV, [2027], 1, FIX.replace("exchange", "yahoo"), None))
raises("補正時段倒置報錯", lambda: CR.calendar_rows(CSV, [2027], 1, FIX.replace("09:30,13:00", "13:00,09:30"), None))
raises("補正休市日帶時段報錯", lambda: CR.calendar_rows(CSV, [2027], 1, FIX.replace("early_close", "full_closure"), None))
raises("補正欄位不對報錯", lambda: CR.calendar_rows(CSV, [2027], 1, FIX.replace("evidence", "proof"), None))
raises("停盤時段倒置報錯", lambda: CR.calendar_rows(CSV, [2027], 1, None, HALT.replace("09:35,09:50", "09:50,09:35")))
raises("停盤範圍不對報錯", lambda: CR.calendar_rows(CSV, [2027], 1, None, HALT.replace("nyse_listed", "arca")))
raises("停盤類型不對報錯", lambda: CR.calendar_rows(CSV, [2027], 1, None, HALT.replace("feed_gap", "glitch")))
raises("停盤來源不對報錯", lambda: CR.calendar_rows(CSV, [2027], 1, None, HALT.replace("local_data", "yahoo")))
raises("停盤缺說明報錯", lambda: CR.calendar_rows(CSV, [2027], 1, None, HALT.replace(",原因未證實", ",")))
raises("停盤落在休市日報錯", lambda: CR.calendar_rows(CSV, [2027], 1, None, HALT.replace("2025-02-03", "2025-01-09")))
raises("停盤重複報錯", lambda: CR.calendar_rows(CSV, [2027], 1, None, HALT + "2025-03-12,09:35,09:40,all,halt,press,x\n"))

c2 = {}
a1, _ = CR.plan_calendar(set(c2), c2.get, CSV, [2027], FIX, HALT)
f1 = dict(a1)
m1 = json.loads(f1["_catalog/calendar/v1/manifest.json"])
check("v2 格式產出 halts.csv", "_catalog/calendar/v1/halts.csv" in f1 and len(rows(f1["_catalog/calendar/v1/halts.csv"])) == 2)
check("schema 名 catalog_calendar_v2", json.loads(f1["_catalog/calendar/v1/schema.json"])["schema"] == "catalog_calendar_v2")
check("manifest 記補正與停盤 sha／筆數", m1["fixes_sha256"] == CR._sha(FIX) and m1["fixes_rows"] == 1
      and m1["halts_sha256"] == CR._sha(HALT) and m1["halts_rows"] == 2)
check("manifest 檔案 sha 對得上", all(m1["files"][p.split("/")[-1]]["sha256"] == CR._sha(t) for p, t in a1[:-1]))
for p, t in a1:
    c2[p] = t
a2, s2 = CR.plan_calendar(set(c2), c2.get, CSV, [2027], FIX, HALT)
check("v2 內容沒變不發新版", a2 == [], s2)
a3, _ = CR.plan_calendar(set(c2), c2.get, CSV, [2027], FIX, HALT.replace("原因未證實", "原因未證實（補註）"))
check("只改停盤清單也發新版", a3 and a3[0][0].startswith("_catalog/calendar/v2/"))

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

# ── 來源守衛（2026-10-26）：v1 對、v2 被 Polygon 改刪、v3 要保留 v1 值 ──
G_IDENT = IDENT + "FIGI:BALL,polygon,BALL,,,polygon,,1\nFIGI:SLM,polygon,SLM,,,polygon,,1\nFIGI:NEWX,polygon,NEWX,,,polygon,,1\n"
SPC = ",".join(CR.SPLIT_COLS) + "\n"
g_store = {"_catalog/identity/v1/identity.csv": G_IDENT, "_catalog/identity/v1/manifest.json": json.dumps({"version": 1}),
           "_catalog/splits/v1/splits.csv": SPC
           + "FIGI:BALL,BALL,2004-08-24,split,2,1,2,polygon,E-old-ball,high,,1\n"
           + "FIGI:SLM,SLM,2003-06-23,split,3,1,3,polygon,E-old-slm,high,,1\n"
           + "FIGI:NVDA,NVDA,2024-06-10,split,10,1,10,polygon,a3,high,,1\n",
           "_catalog/splits/v1/unmapped.csv": "symbol,ex_date,split_from,split_to,reason\nGONE,2010-01-04,1,4,身份表沒有這個代號\n",
           "_catalog/splits/v1/manifest.json": json.dumps({"version": 1}),
           "_catalog/splits/v2/splits.csv": SPC                                        # v2 未設守衛：BALL 消失、SLM 變 1:2
           + "FIGI:SLM,SLM,2003-06-23,split,2,1,2,polygon,E-new-slm,high,,2\n"
           + "FIGI:NVDA,NVDA,2024-06-10,split,10,1,10,polygon,a3,high,,2\n",
           "_catalog/splits/v2/unmapped.csv": "symbol,ex_date,split_from,split_to,reason\n",
           "_catalog/splits/v2/manifest.json": json.dumps({"version": 2})}
G_RAW = [{"id": "E-new-slm", "ticker": "SLM", "execution_date": "2003-06-23", "split_from": 1, "split_to": 2},
         {"id": "a3", "ticker": "NVDA", "execution_date": "2024-06-10", "split_from": 2, "split_to": 20},     # 同比例不同寫法
         {"id": "n1", "ticker": "NEWX", "execution_date": "2026-01-05", "split_from": 1, "split_to": 2}]      # 新增
g_adds, g_summ = CR.plan_splits(set(g_store), g_store.get, G_RAW)
g = dict(g_adds)
g_sp = {(r["symbol"], r["ex_date"]): r for r in rows(g["_catalog/splits/v3/splits.csv"])}
g_ch = rows(g["_catalog/splits/v3/changes.csv"])
g_man = json.loads(g["_catalog/splits/v3/manifest.json"])
check("守衛：發布到 v3、有 changes.csv", "_catalog/splits/v3/changes.csv" in g and g_adds[-1][0].endswith("v3/manifest.json"))
check("守衛：Polygon 刪掉的 BALL 保留 v1 值", ("BALL", "2004-08-24") in g_sp and g_sp[("BALL", "2004-08-24")]["ratio"] == "2"
      and "保留 v1" in g_sp[("BALL", "2004-08-24")]["note"], g_sp.get(("BALL", "2004-08-24")))
check("守衛：SLM 改比例 → 保留 v1 的 1:3（不是 v2 的 1:2）", g_sp[("SLM", "2003-06-23")]["ratio"] == "3"
      and g_sp[("SLM", "2003-06-23")]["evidence"] == "E-old-slm")
check("守衛：比例同、寫法不同（2:20＝1:10）不算改", g_sp[("NVDA", "2024-06-10")]["note"] == ""
      and not any(c["symbol"] == "NVDA" for c in g_ch))
check("守衛：新增照收", ("NEWX", "2026-01-05") in g_sp)
check("守衛：unmapped 裡的舊記錄也保留", any(c["symbol"] == "GONE" and c["kind"] == "deleted" for c in g_ch)
      and "GONE" in g["_catalog/splits/v3/unmapped.csv"])
check("守衛：changes 記新舊值與版本", sorted((c["symbol"], c["kind"], c["kept_version"], c["new_to"]) for c in g_ch)
      == [("BALL", "deleted", "1", ""), ("GONE", "deleted", "1", ""), ("SLM", "ratio_changed", "1", "2")], g_ch)
check("守衛：manifest 記數", g_man["guard"] == {"baseline_versions": [1, 2], "deleted": 2, "ratio_changed": 1, "added": 1},
      g_man.get("guard"))
g2 = dict(g_store)
g2.update(g)
g_adds2, g_summ2 = CR.plan_splits(set(g2), g2.get, G_RAW)
check("守衛：同樣輸入再跑 → 內容未變不發新版", g_adds2 == [] and "沿用 v3" in g_summ2, g_summ2)

# ── 人工查證清單（2026-10-29 v4）：add／override／reject，fail-closed ──
EVH = ",".join(CR.EVENT_COLS) + "\n"
EV = (EVH
      + "add,FIGI:NVDA,NVDA,2000-06-27,split,2,1,2,yahoo,Yahoo 2000-06-27 1:2；MPQ 跳變 2,Polygon 無此記錄\n"
      + "add,FIGI:META,META,2024-01-02,spinoff,1.2,,,yahoo,Yahoo 1000:1197；MPQ 跳變 1.2,分拆\n"
      + "override,FIGI:BALL,BALL,2004-08-24,stock_dividend,1.05,,,yahoo,Yahoo 100:105,改判股票股利\n"
      + "reject,FIGI:SLM,SLM,2003-06-23,,,,,,Polygon 已刪；Yahoo 無,屬他公司\n")
e_adds, e_summ = CR.plan_splits(set(g2), g2.get, G_RAW, EV)
e = dict(e_adds)
e_sp = {(r["symbol"], r["ex_date"]): r for r in rows(e["_catalog/splits/v4/splits.csv"])}
e_man = json.loads(e["_catalog/splits/v4/manifest.json"])
check("清單：發 v4、schema v3", e_adds[-1][0].endswith("v4/manifest.json")
      and json.loads(e["_catalog/splits/v4/schema.json"])["schema"] == "catalog_splits_v3")
check("清單：add 拆股 high、source=yahoo", e_sp[("NVDA", "2000-06-27")]["confidence"] == "high"
      and e_sp[("NVDA", "2000-06-27")]["source"] == "yahoo" and e_sp[("NVDA", "2000-06-27")]["table_version"] == "4")
check("清單：add 分拆 split_from 空", e_sp[("META", "2024-01-02")]["type"] == "spinoff" and e_sp[("META", "2024-01-02")]["split_from"] == "")
b = e_sp[("BALL", "2004-08-24")]
check("清單：override 改型、保留舊 evidence 並追加", b["type"] == "stock_dividend" and b["ratio"] == "1.05" and b["confidence"] == "high"
      and b["evidence"].startswith("E-old-ball；") and b["source"] == "polygon+yahoo" and "保留 v1" in b["note"], b)
sl = e_sp[("SLM", "2003-06-23")]
check("清單：reject 不刪、標 rejected、note 寫原因", sl["confidence"] == "rejected" and "查證為錯：屬他公司" in sl["note"] and sl["ratio"] == "3", sl)
check("清單：manifest 記 events", e_man["events_sha256"] == CR._sha(EV) and e_man["events_rows"] == 4
      and e_man["events"] == {"add_split": 1, "add_spinoff": 1, "override_stock_dividend": 1, "reject": 1}, e_man.get("events"))
check("清單：摘要有數", "新增 2、更正 1、駁回 1" in e_summ, e_summ)
check("清單：不給清單＝照 v3 行為", CR.plan_splits(set(g2), g2.get, G_RAW)[0] == [])

# ── 回歸（10-30）：表裡有人工新增列之後「再發一版」不得撞鍵（守衛基準只收 Polygon 來源列）──
g3 = dict(g2)
g3.update(e)
r_base = CR.baseline(set(g3), g3.get)
check("再發一版：人工新增列不進基準", ("NVDA", "2000-06-27") not in r_base and ("META", "2024-01-02") not in r_base)
check("再發一版：override 過的 polygon+yahoo 列仍在基準", ("BALL", "2004-08-24") in r_base)
r_adds, r_summ = CR.plan_splits(set(g3), g3.get, G_RAW, EV)
check("再發一版：同輸入不撞鍵、內容未變不發新版", r_adds == [] and "沿用 v4" in r_summ, r_summ)
r_new = G_RAW + [{"id": "n2", "ticker": "NVDA", "execution_date": "2021-07-20", "split_from": 1, "split_to": 4}]
r_adds2, r_summ2 = CR.plan_splits(set(g3), g3.get, r_new, EV)
r = dict(r_adds2)
r_rows = rows(r.get("_catalog/splits/v5/splits.csv", ""))
r_sp = {(x["symbol"], x["ex_date"]): x for x in r_rows}
check("再發一版：有新事件發 v5", ("NVDA", "2021-07-20") in r_sp, r_summ2)
check("再發一版：人工新增列只有一筆、內容不變", sum(1 for x in r_rows if (x["symbol"], x["ex_date"]) == ("NVDA", "2000-06-27")) == 1
      and r_sp[("NVDA", "2000-06-27")]["source"] == "yahoo" and r_sp[("META", "2024-01-02")]["type"] == "spinoff")
check("再發一版：人工新增列不算 Polygon 刪除", not any(c["symbol"] == "META" or c["ex_date"] == "2000-06-27"
                                             for c in rows(r["_catalog/splits/v5/changes.csv"])))
check("再發一版：override 不重複疊加 evidence", r_sp[("BALL", "2004-08-24")]["evidence"].count("Yahoo 100:105") == 1
      and r_sp[("BALL", "2004-08-24")]["type"] == "stock_dividend", r_sp[("BALL", "2004-08-24")])


def fails(text, name):
    try:
        CR.plan_splits(set(g2), g2.get, G_RAW, text)
        check(name, False)
    except SystemExit:
        check(name, True)


fails(EVH + "override,FIGI:X,NOPE,2020-01-02,split,2,1,2,yahoo,ev,\n", "清單：override 對不到要停")
fails(EVH + "add,FIGI:NVDA,NVDA,2024-06-10,split,10,1,10,yahoo,ev,\n", "清單：add 與既有同鍵要停")
fails(EVH + "add,FIGI:NVDA,NVDA,2020-01-02,split,2,1,2,blog,ev,\n", "清單：來源不在白名單要停")
fails(EVH + "add,FIGI:NVDA,NVDA,2020-01-02,split,2,1,2,yahoo,,\n", "清單：沒 evidence 要停")
fails(EVH + "add,FIGI:NVDA,NVDA,2020-01-02,bogus,2,1,2,yahoo,ev,\n", "清單：type 不認得要停")
fails(EVH + "add,FIGI:NVDA,NVDA,2020-01-02,split,0,1,2,yahoo,ev,\n", "清單：ratio 非正要停")
fails(EVH + "reject,FIGI:SLM,SLM,2003-06-23,,,,,,ev,\n", "清單：reject 沒 note 要停")
fails(EVH + "add,FIGI:NVDA,NVDA,2020-01-02,split,2,1,2,yahoo,ev,\n" * 2, "清單：同鍵重複要停")
fails("action,entity_id\n", "清單：欄位不符要停")

print("\n%d 通過，%d 失敗" % (len(PASS), len(FAIL)))
sys.exit(1 if FAIL else 0)
