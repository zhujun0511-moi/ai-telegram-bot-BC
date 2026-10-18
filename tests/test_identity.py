# -*- coding: utf-8 -*-
"""
test_identity.py — 身份時間表（ticker_identity.py）離線測試：假的 Polygon 列，不連網。
案例：FB→META（改名）、WM（Washington Mutual → Waste Management，易主）、SUNW→JAVA（改名後下市）、
SPCX（前後不同公司）、只有 FIGI 沒 CIK、Polygon 與 SEC 的 CIK 不一致。用法：python tests/test_identity.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import ticker_identity as TI   # noqa: E402

PASS, FAIL = [], []


def check(name, ok, info=""):
    (PASS if ok else FAIL).append(name)
    print(("✅ " if ok else "❌ ") + name + ("" if ok else "  " + str(info)))


active = [
    {"ticker": "META", "name": "Meta Platforms, Inc.", "type": "CS", "cik": "0001326801"},
    {"ticker": "WM", "name": "Waste Management, Inc.", "type": "CS", "cik": "0000823768"},
    {"ticker": "SPCX", "name": "New SPCX Co", "type": "CS", "cik": "0002000001"},
    {"ticker": "FIGIONLY", "name": "No Cik Corp", "type": "CS", "share_class_figi": "BBG000XYZ"},
    {"ticker": "MISMATCH", "name": "Mismatch Inc", "type": "CS", "cik": "0000000111"},
]
inactive = [
    {"ticker": "FB", "name": "Facebook, Inc.", "type": "CS", "cik": "0001326801", "delisted_utc": "2022-06-09T00:00:00Z"},
    {"ticker": "WM", "name": "Washington Mutual Inc", "type": "CS", "cik": "0000933136",
     "delisted_utc": "2008-09-26T00:00:00Z"},
    {"ticker": "SUNW", "name": "Sun Microsystems", "type": "CS", "cik": "0000709519", "delisted_utc": "2007-08-27T00:00:00Z"},
    {"ticker": "JAVA", "name": "Sun Microsystems", "type": "CS", "cik": "0000709519", "delisted_utc": "2010-01-27T00:00:00Z"},
    {"ticker": "SPCX", "name": "The SPAC and New Issue ETF", "type": "ETF", "cik": "0001771146",
     "delisted_utc": "2024-03-01T00:00:00Z"},
]
ident, ren, reu, prob = TI.build(active, inactive, sec_t2c={"MISMATCH": 999, "META": 1326801})
R = TI.Resolver(ident)

rn = {(r["old_ticker"], r["new_ticker"]): r for r in ren}
check("改名：FB → META，日期 2022-06-09", ("FB", "META") in rn and rn[("FB", "META")]["date"] == "2022-06-09", ren)
check("改名：SUNW → JAVA（同一家 Sun，CIK 相同）", ("SUNW", "JAVA") in rn, ren)
ru = {r["ticker"]: r for r in reu}
check("易主：WM 在 2008-09-26 由 Washington Mutual 換成 Waste Management",
      "WM" in ru and ru["WM"]["boundary"] == "2008-09-26" and "Washington" in ru["WM"]["name_before"]
      and "Waste" in ru["WM"]["name_after"], ru.get("WM"))
check("易主：SPCX 前後是不同公司（ETF → 新公司）", "SPCX" in ru and ru["SPCX"]["entity_before"] != ru["SPCX"]["entity_after"])
check("改名不算易主（FB、META 都不在易主清單）", "FB" not in ru and "META" not in ru)

check("查詢：FB 在 2018-05 屬於 Meta 這家公司", R.entity("FB", "2018-05") == "CIK:0001326801")
check("查詢：Meta 這家公司現在的代號是 META", R.current_ticker(R.entity("FB", "2018-05")) == "META")
check("查詢：WM 在 2005-01 是 Washington Mutual", R.entity("WM", "2005-01") == "CIK:0000933136")
check("查詢：WM 在 2010-01 是 Waste Management", R.entity("WM", "2010-01") == "CIK:0000823768")
check("查詢：易主當天（2008-09-26）已屬新公司", R.entity("WM", "2008-09-26") == "CIK:0000823768")
check("查詢：Washington Mutual 已下市 → 沒有現在的代號（不會被接到現在的 WM＝Waste Management）",
      R.current_ticker("CIK:0000933136") is None and R.entity("WM", "2005-01") != R.entity("WM", "2010-01"))
check("查詢：已下市的 Sun 沒有現在的代號", R.current_ticker("CIK:0000709519") is None)
check("查詢：FB 在 2023 年（改名後）查無公司", R.entity("FB", "2023-01") is None)
check("沒有 CIK 用 FIGI 當公司編號", R.entity("FIGIONLY", "2020-01") == "FIGI:BBG000XYZ")
check("Polygon 與 SEC 的 CIK 不一致 → 列為問題", any("MISMATCH" == p["ticker"] and "不一致" in p["reason"] for p in prob),
      prob)
check("一致的不列問題（META）", not any(p["ticker"] == "META" for p in prob))

print("\n%d 通過，%d 失敗" % (len(PASS), len(FAIL)))
sys.exit(1 if FAIL else 0)
