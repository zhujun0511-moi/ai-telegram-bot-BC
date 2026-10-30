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

# ── 第 2 版：股份 FIGI 主鍵、排除權證單位 ──
act2 = [
    {"ticker": "GOOGL", "name": "Alphabet Inc. Class A", "type": "CS", "cik": "0001652044", "share_class_figi": "BBG009S39JX6"},
    {"ticker": "GOOG", "name": "Alphabet Inc. Class C", "type": "CS", "cik": "0001652044", "share_class_figi": "BBG009S3NB30"},
    {"ticker": "META", "name": "Meta Platforms", "type": "CS", "cik": "0001326801", "share_class_figi": "BBG000MM2P62"},
    {"ticker": "XYZ", "name": "XYZ Merged Co", "type": "CS", "cik": "0009999999", "share_class_figi": "BBG00XYZ0001"},
    {"ticker": "ABCD", "name": "Abcd Acq Corp", "type": "CS", "cik": "0008888888"},
    {"ticker": "ABCDU", "name": "Abcd Acq Corp Units", "type": "UNIT", "cik": "0008888888"},
    {"ticker": "ABCDW", "name": "Abcd Acq Corp Warrants", "type": "WARRANT", "cik": "0008888888"},
]
ina2 = [
    {"ticker": "FB", "name": "Facebook", "type": "CS", "cik": "0001326801", "delisted_utc": "2022-06-09T00:00:00Z"},
    {"ticker": "SPAC1", "name": "Spac One Acq", "type": "CS", "cik": "0009999999", "delisted_utc": "2021-07-01T00:00:00Z"},
    {"ticker": "SPAC1W", "name": "Spac One Warrants", "type": "WARRANT", "cik": "0009999999",
     "delisted_utc": "2021-07-01T00:00:00Z"},
]
i2, r2, u2, p2 = TI.build(act2, ina2)
R2 = TI.Resolver(i2)
pairs = {(r["old_ticker"], r["new_ticker"]) for r in r2}
check("第 2 版：GOOG／GOOGL 兩種股份各自一家，不算改名", not any("GOOG" in x for pr in pairs for x in pr), pairs)
check("第 2 版：GOOGL 在 2018 年對到它自己（Class A）、現在仍是 GOOGL",
      R2.current_ticker(R2.entity("GOOGL", "2018-05")) == "GOOGL" and R2.is_current("GOOGL", R2.entity("GOOGL", "2018-05")))
check("第 2 版：GOOG 與 GOOGL 是不同的股份", R2.entity("GOOG", "2018-05") != R2.entity("GOOGL", "2018-05"))
check("第 2 版：FB（沒有 FIGI）借用 Meta 唯一的股份 FIGI → 仍認得 FB→META 改名", ("FB", "META") in pairs, pairs)
check("第 2 版：SPAC 合併後改名 SPAC1 → XYZ", ("SPAC1", "XYZ") in pairs, pairs)
check("第 2 版：單位、權證不進時間表（ABCDU、ABCDW、SPAC1W 都沒有紀錄）",
      not any(x["ticker"] in ("ABCDU", "ABCDW", "SPAC1W") for x in i2))
check("第 2 版：SPAC 的 ABCD／ABCDU／ABCDW 不算改名", not any("ABCD" in x for pr in pairs for x in pr), pairs)
check("第 2 版：改名總數＝2（FB→META、SPAC1→XYZ）", len(r2) == 2, r2)

# ── 10-30 第 3 版：債券／優先股不進表、不借用普通股 FIGI ──
act3 = [
    {"ticker": "DTE", "name": "DTE Energy Company", "type": "CS", "cik": "0000936340", "share_class_figi": "BBGDTE"},
    {"ticker": "DTG", "name": "DTE Energy Company 2021 Series E 4.375% Junior Subordinated Debentures", "type": "SP",
     "cik": "0000936340"},
    {"ticker": "RILY", "name": "BRC Group Holdings, Inc. Common Stock", "type": "CS", "cik": "0001464790",
     "share_class_figi": "BBGRILY"},
    {"ticker": "RILYG", "name": "BRC Group Holdings, Inc. 5.00% Senior Notes due 2026", "type": "CS",
     "cik": "0001464790", "share_class_figi": "BBGRILYG"},
    {"ticker": "RILYW", "name": "BRC Group W.I.", "type": "", "cik": "0001464790"},
    {"ticker": "GECCM", "name": "Great Elm Capital Corp. 6.75% Notes Due 2025", "type": "ETF", "cik": "0001675033"},
    {"ticker": "SPY", "name": "SPDR S&P 500 ETF Trust", "type": "ETF", "cik": "0000884394", "share_class_figi": "BBGSPY"},
    {"ticker": "BUFR", "name": "FT Vest 10% Buffer ETF", "type": "ETF", "share_class_figi": "BBGBUFR"},
    {"ticker": "PFF", "name": "iShares Preferred and Income Securities ETF", "type": "ETF", "share_class_figi": "BBGPFF"},
    {"ticker": "DUEX", "name": "Due West Holdings Inc", "type": "CS", "share_class_figi": "BBGDUEX"},
    {"ticker": "NOTE", "name": "Noteworthy 5% Growth Corp", "type": "CS", "share_class_figi": "BBGNOTE"},
]
ina3 = [
    {"ticker": "EBAY-L", "name": "EBAY INC 6.0% DUE 02/01/2056 (DE)", "cik": "0001065088",
     "delisted_utc": "2021-01-01T00:00:00Z"},
    {"ticker": "HBHC-P", "name": "HANCOCK HOLDING CO PFD CONV PFD SER", "type": None, "cik": "0000750577",
     "delisted_utc": "2015-01-01T00:00:00Z"},
    {"ticker": "HEPS", "name": "HAWAIIAN ELEC IND 8.36%(TOPRS)", "cik": "0000354707",
     "delisted_utc": "2005-01-01T00:00:00Z"},
    {"ticker": "CDW", "name": "CENDANT CORPORATION COM EX-DISTRIBUTION W.I.", "cik": "0000723612",
     "delisted_utc": "2006-01-01T00:00:00Z"},
]
for t3, want in (("DTG", "sp"), ("RILYG", "name"), ("GECCM", "name")):
    check("跳過 %s" % t3, TI.skip_reason(next(r for r in act3 if r["ticker"] == t3)) == want)
check("空類型債券跳過", TI.skip_reason(ina3[0]) == "name")
check("type None 優先股跳過", TI.skip_reason(ina3[1]) == "name")
check("TOPRS 跳過", TI.skip_reason(ina3[2]) == "name")
check("W.I. 不受影響（不在這次範圍）", TI.skip_reason(ina3[3]) is None)
check("原有 PFD 類型仍跳過", TI.skip_reason({"ticker": "X", "type": "PFD", "name": "X"}) == "type")
for t3 in ("DTE", "SPY", "BUFR", "PFF", "DUEX", "NOTE"):
    check("不誤殺 %s" % t3, TI.skip_reason(next(r for r in act3 if r["ticker"] == t3)) is None)
id3 = TI.build(act3, ina3)[0]
t3s = {x["ticker"]: x for x in id3}
check("債券不進表", not ({"DTG", "RILYG", "GECCM", "EBAY-L", "HBHC-P", "HEPS"} & set(t3s)), sorted(t3s))
check("普通股、ETF 仍在表", {"DTE", "RILY", "SPY", "BUFR", "PFF", "DUEX", "NOTE", "CDW"} <= set(t3s), sorted(t3s))
check("被標成 CS 的債不占借用名額（RILYW 仍借到 RILY）", t3s["RILYW"]["entity"] == "FIGI:BBGRILY", t3s.get("RILYW"))

print("\n%d 通過，%d 失敗" % (len(PASS), len(FAIL)))
sys.exit(1 if FAIL else 0)
