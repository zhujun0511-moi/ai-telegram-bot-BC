# -*- coding: utf-8 -*-
"""catalog_symbols 離線測試（不連網）。"""
import csv
import io
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import catalog_symbols as CS

PASS, FAIL = [], []


def check(name, cond, info=""):
    (PASS if cond else FAIL).append(name)
    print(("✅ " if cond else "❌ ") + name + ("" if cond else "  " + str(info)[:300]))


ACT = [{"ticker": "NEWCO", "name": "New Co", "type": "CS", "cik": "0000000123", "primary_exchange": "XNYS",
        "currency_name": "usd", "share_class_figi": "BBGNEW", "composite_figi": "BBGNEWC"},
       {"ticker": "NEWCO.WS", "name": "New Co Warrant", "type": "WARRANT", "cik": "123", "primary_exchange": "XNYS"},
       {"ticker": "ETFX", "name": "Some ETF", "type": "ETF", "primary_exchange": "ARCX", "share_class_figi": "BBGETF"}]
INA = [{"ticker": "OLDX", "name": "Old X Inc", "type": "CS", "cik": "0000000999", "delisted_utc": "2015-03-02T05:00:00Z",
        "share_class_figi": "BBGOLD"},
       {"ticker": "NEWCO", "name": "Prior Owner", "type": "CS", "cik": "0000000777", "delisted_utc": "2010-01-05T05:00:00Z",
        "share_class_figi": "BBGPRIOR"}]
tk = CS.ticker_rows(ACT, INA, 1)
by = {(x["symbol"], x["active"]): x for x in tk}
check("權證也進名冊", ("NEWCO-WS", 1) in by and by[("NEWCO-WS", 1)]["type"] == "WARRANT", tk)
check("權證借用同 CIK 唯一 FIGI 當實體", by[("NEWCO-WS", 1)]["entity_id"] == "FIGI:BBGNEW", by[("NEWCO-WS", 1)])
check("下市日取日期", by[("OLDX", 0)]["delisted"] == "2015-03-02" and by[("OLDX", 0)]["active"] == 0)
check("CIK 補成 10 碼", by[("NEWCO", 1)]["cik"] == "0000000123")
ID = [{"entity_id": "FIGI:BBGNEW", "source": "mpq", "symbol": "NEWCO", "asset": "stock", "to": ""},
      {"entity_id": "FIGI:BBGPRIOR", "source": "mpq", "symbol": "NEWCO-DELISTED", "asset": "stock", "to": "2010-01-05"},
      {"entity_id": "MPQ:ZZZ", "source": "mpq", "symbol": "ZZZ", "asset": "etf", "to": ""},
      {"entity_id": "MPQ:OLDX", "source": "mpq", "symbol": "OLDX-DELISTED", "asset": "stock", "to": ""},
      {"entity_id": "FIGI:BBGNEW", "source": "polygon", "symbol": "NEWCO", "asset": ""}]
mq = {x["mpq_symbol"]: x for x in CS.mpq_rows(ID, tk, 1)}
check("只收 mpq 列", len(mq) == 4, mq)
check("同實體對上", mq["NEWCO"]["match"] == "entity" and mq["NEWCO"]["name"] == "New Co")
check("舊公司段對到舊公司", mq["NEWCO-DELISTED"]["name"] == "Prior Owner" and mq["NEWCO-DELISTED"]["delisted"] == "2010-01-05")
check("實體對不上就看代號", mq["OLDX-DELISTED"]["match"] == "symbol" and mq["OLDX-DELISTED"]["type"] == "CS")
check("都沒有＝none", mq["ZZZ"]["match"] == "none" and mq["ZZZ"]["name"] == "")
ID2 = ID + [{"entity_id": "FIGI:BBGPRIOR", "source": "mpq", "symbol": "NEWCO", "asset": "stock", "to": "2010-01-05"}]
mq2 = [x for x in CS.mpq_rows(ID2, tk, 1) if x["mpq_symbol"] == "NEWCO"]
check("同代號先後兩家公司各一列", sorted(x["name"] for x in mq2) == ["New Co", "Prior Owner"], mq2)

idcsv = io.StringIO()
w = csv.DictWriter(idcsv, fieldnames=["entity_id", "source", "symbol", "from", "to", "evidence", "note", "table_version", "asset"])
w.writeheader()
for r in ID:
    w.writerow({k: r.get(k, "") for k in w.fieldnames})
store = {"_catalog/identity/v6/identity.csv": idcsv.getvalue(), "_catalog/identity/v6/manifest.json": "{}",
         "_catalog/identity/v7/identity.csv": idcsv.getvalue(), "_catalog/identity/v7/manifest.json": "{}"}
adds, summ = CS.plan(set(store), store.get, ACT, INA, 6)
got = dict(adds)
man = json.loads(got["_catalog/symbols/v1/manifest.json"])
check("發 v1、manifest 最後", adds[-1][0] == "_catalog/symbols/v1/manifest.json" and man["identity_version"] == 6, summ)
check("manifest sha256 對得上", all(man["files"][p.split("/")[-1]]["sha256"] == CS._sha(t) for p, t in adds[:-1]))
store.update(got)
check("內容沒變沿用", CS.plan(set(store), store.get, ACT, INA, 6)[0] == [])
check("身份表換版要發新版", CS.plan(set(store), store.get, ACT, INA, 7)[0][0][0].startswith("_catalog/symbols/v2/"))
try:
    CS.plan(set(store), store.get, ACT, INA, 99)
    check("指定不存在的身份表版本要停", False)
except SystemExit:
    check("指定不存在的身份表版本要停", True)

check("優先股：Polygon ABRpD 與 MPQ ABR.D 同代號", CS.norm("ABRpD") == CS.norm("ABR.D") == "ABR-D", CS.norm("ABRpD"))
check("結尾單獨 p 只大寫", CS.norm("DCOMp") == "DCOMP")
print("\n%d 通過，%d 失敗" % (len(PASS), len(FAIL)))
sys.exit(1 if FAIL else 0)
