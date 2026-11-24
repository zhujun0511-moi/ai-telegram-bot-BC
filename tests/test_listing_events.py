# -*- coding: utf-8 -*-
"""
test_listing_events.py — 每日上市事件（listing_events.py）離線測試：只測純函數，不連網。
用法：python tests/test_listing_events.py
"""
import datetime as dt
import os
import sys
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import listing_events as L   # noqa: E402

PASS, FAIL = [], []


def check(name, ok, info=""):
    (PASS if ok else FAIL).append(name)
    print(("✅ " if ok else "❌ ") + name + ("" if ok else "  " + str(info)))


def R(t, figi="", cik="", ex="XNAS", typ="CS", name="X"):
    return {"ticker": t, "name": name, "type": typ, "primary_exchange": ex, "cik": cik, "composite_figi": "",
            "share_class_figi": figi, "list_date": ""}


prev = {"INDV": R("INDV", "BBG01ZTXFLX8", "1625297"), "SUPN": R("SUPN", "BBG001SFKFF1", "1356576"),
        "ROKU": R("ROKU", "BBG001ZZPQM2"), "AAA": R("AAA", "F1", ex="XNYS"), "BBB": R("BBB", "F2"),
        "CCC": R("CCC", "", "123")}
cur = {"SUPN": R("SUPN", "BBG01ZTXFLX8", "1625297"),       # 代號易主（舊 Supernus → Indivior）
       "AAA": R("AAA", "F1", ex="XNAS"),                    # 換交易所
       "BBB": R("BBB", "F2"), "ANT": R("ANT", "BBG0118LR3P8"),
       "NEWW": R("NEWW", "F9", typ="WARRANT"), "CCC": R("CCC", "", "0000000123")}
d = L.diff_snapshots(prev, cur)
check("INDV 改名到原屬舊 Supernus 的 SUPN＝改名", d["renamed"] == [("INDV", "SUPN")], d)
check("消失只剩 ROKU（INDV 已配成改名）", d["disappeared"] == ["ROKU"], d)
check("SUPN 同代號換公司＝易主（照報）", d["reused"] == ["SUPN"], d)
check("AAA 換交易所", d["market_changed"] == ["AAA"], d)
check("新出現 ANT、NEWW", d["appeared"] == ["ANT", "NEWW"], d)
check("CIK 補零後相同不算易主", "CCC" not in d["reused"], d)

prev2 = {"OLD": R("OLD", "FX", "1"), "KEEP": R("KEEP", "FK")}
cur2 = {"NEW": R("NEW", "FX", "1"), "KEEP": R("KEEP", "FK"), "IPO": R("IPO", "FI")}
d2 = L.diff_snapshots(prev2, cur2)
check("同 FIGI 舊代號消失、新代號出現＝改名", d2["renamed"] == [("OLD", "NEW")], d2)
check("改名配對到的不算新上市／消失", d2["appeared"] == ["IPO"] and d2["disappeared"] == [], d2)

check("沒 FIGI 沒 CIK 無法判斷同家", L.same_company({"name": "A"}, {"name": "A"}) is None)
check("entity：FIGI 優先", L.entity_of(R("X", "F1", "5")) == "FIGI:F1")
check("entity：無 FIGI 用 CIK 補 10 碼", L.entity_of(R("X", "", "5")) == "CIK:0000000005")
check("代號正規化：BRK-B 與 BRK.B 相同", L.norm("brk-b") == L.norm("BRK.B"))
check("代號正規化：Polygon 優先股 ABRpD＝MPQ ABR.D", L.norm("ABRpD") == L.norm("ABR.D") == "ABR.D", L.norm("ABRpD"))
check("代號正規化：結尾單獨 p（DCOMp）＝MPQ DCOMP", L.norm("DCOMp") == L.norm("DCOMP") == "DCOMP", L.norm("DCOMp"))
check("代號正規化：一般大寫代號不動", L.norm("APP") == "APP" and L.norm("SPY") == "SPY")
check("清單內：優先股以 Polygon 寫法命中 MPQ 代號", L.in_scope("BACpB", "CIK:0000070858", {L.norm("BAC.B")}, set()))

S, E = {L.norm("ROKU")}, {"FIGI:F2"}
check("清單內：代號命中", L.in_scope("ROKU", "FIGI:Z", S, E))
check("清單內：實體命中", L.in_scope("ZZZ", "FIGI:F2", S, E))
check("NAME: 實體不算命中", not L.in_scope("ZZZ", "NAME:X", S, {"NAME:X"}))

check("消失後別的市場還在交易＝換市場", L.classify_gone({"active": [{"primary_exchange": "OTCM"}], "inactive": []})[:1] == ("market_changed",))
k = L.classify_gone({"active": [], "inactive": [{"delisted_utc": "2026-11-09T00:00:00Z"}, {"delisted_utc": "2006-01-01T00:00:00Z"}]})
check("有下市日＝下市，取最新那筆", k[0] == "delisted" and k[1] == "2026-11-09" and k[3] == "high", k)
check("查無下市日＝疑似", L.classify_gone({"active": [], "inactive": []})[0] == "suspected_delisted")
check("沒查（超過上限）＝疑似", L.classify_gone(None) == ("suspected_delisted", "", "", "suspect"))

e = L.event("2026-11-24", "renamed", "INDV", R("SUPN", "BBG01ZTXFLX8"), "local_universe", new_symbol="SUPN")
check("_id＝日期-類型-代號", e["_id"] == "2026-11-24-renamed-INDV", e["_id"])
check("event 不含價格欄位", not any(x in e["evidence"] for x in ("c", "close", "price", "o", "h", "l")), e["evidence"])
e2 = L.event("2026-11-24", "split_announced", "SMX", {}, "dc_list", id_key="SMX-2026-11-30")
check("拆股公告 _id 帶生效日、symbol 維持代號", e2["_id"] == "2026-11-24-split_announced-SMX-2026-11-30" and e2["symbol"] == "SMX", e2)
try:
    L.event("2026-11-24", "ack", "X", {}, "market")
    check("不認得的類型丟例外", False)
except AssertionError:
    check("不認得的類型丟例外", True)

check("保留 30 天：刪 detected 早於 30 天前", L.prune_query("2026-11-24") == {"detected": {"$lt": "2026-10-25"}})

ET = ZoneInfo("America/New_York")
summer = dt.datetime(2026, 7, 14, 7, 3, tzinfo=ET)
winter = dt.datetime(2026, 11, 24, 7, 3, tzinfo=ET)
check("夏令只放行 UTC 11:00", L.schedule_ok("schedule", L.CRON_EDT, summer) and not L.schedule_ok("schedule", L.CRON_EST, summer))
check("冬令只放行 UTC 12:00", L.schedule_ok("schedule", L.CRON_EST, winter) and not L.schedule_ok("schedule", L.CRON_EDT, winter))
check("手動觸發一律放行", L.schedule_ok("workflow_dispatch", "", winter))
EV = {L.PREFIX + "/events/2026-11-24.csv"}
check("同日已寫過：排程重跑跳過", L.already_done(EV, "2026-11-24", "schedule"))
check("同日已寫過：手動觸發照跑", not L.already_done(EV, "2026-11-24", "workflow_dispatch"))
check("今天還沒寫：排程照跑", not L.already_done(EV, "2026-11-25", "schedule"))

rows = L.parse_csv(L.to_csv(L.SNAP_COLS, [R("A,B", "F1")]))
check("快照 CSV 來回（含逗號）", rows[0]["ticker"] == "A,B" and rows[0]["share_class_figi"] == "F1", rows)
m = L.message("2026-11-24", [e, L.event("2026-11-24", "new_listing", "ANT", R("ANT"), "market")], False)
check("通知列出改名與新上市", "改名 INDV → SUPN" in m and "ANT" in m, m)
check("首次執行有說明", "首次執行" in L.message("2026-11-24", [], True))

print("\n通過 %d／失敗 %d" % (len(PASS), len(FAIL)))
sys.exit(1 if FAIL else 0)
