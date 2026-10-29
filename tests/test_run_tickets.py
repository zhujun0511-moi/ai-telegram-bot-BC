# -*- coding: utf-8 -*-
"""
test_run_tickets.py — 工單通道（run_tickets.py）離線測試：只測純函數（判定、回覆格式、寫死的權限條件），不連網。
用法：python tests/test_run_tickets.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import run_tickets as RT   # noqa: E402

PASS, FAIL = [], []


def check(name, ok, info=""):
    (PASS if ok else FAIL).append(name)
    print(("✅ " if ok else "❌ ") + name + ("" if ok else "  " + str(info)))


def T(round_=1, type_="stream_break", last="2026-10-26"):
    return {"_id": "local-stream_break-ABC-2026-10-29", "from": "local", "to": "datacenter", "type": type_,
            "subject": {"symbol": "ABC", "asset": "stock"}, "evidence": {"last_data_day": last, "missing_trading_days": 3},
            "status": "working", "round": round_, "replies": []}


ACTIVE = {"ticker": "ABC", "active": True, "primary_exchange": "XNAS", "cik": "1", "composite_figi": "BBG1"}

# ── 判定 ──
v, s, r, a = RT.classify(T(), {"overview": ACTIVE, "bar_dates": ["2026-10-23", "2026-10-26", "2026-10-27", "2026-10-28"]})
check("Polygon 有斷流後日線＋現役 → 自動 source_gap", v == "auto_replied" and a[0] == "source_gap", (v, s, a))
check("source_gap 只列 last_data_day 之後的日子", a and a[1]["trading_days"] == ["2026-10-27", "2026-10-28"], a)
check("自動回覆本身通過格式檢查", a and RT.validate_reply(*a) is None, a and RT.validate_reply(*a))

v, s, r, a = RT.classify(T(round_=2), {"overview": ACTIVE, "bar_dates": ["2026-10-27"]})
check("第 2 輪（本地端追問）一律交人工", v == "needs_human" and a is None and s == "follow_up", (v, s))

v, s, r, a = RT.classify(T(), {"overview": {}, "inactive": [{"ticker": "ABC", "delisted_utc": "2026-10-27T00:00:00Z"}],
                               "events": [{"date": "2026-10-27", "ticker": "XYZ"}], "bar_dates": []})
check("有代號變更事件 → 建議 renamed、交人工（不自動回）", v == "needs_human" and s == "renamed" and a is None, (v, s))
check("理由寫出新代號", "XYZ" in r, r)

v, s, r, a = RT.classify(T(), {"overview": {}, "inactive": [{"ticker": "ABC", "delisted_utc": "2026-10-27T00:00:00Z"}],
                               "events": [], "bar_dates": [], "sec": [{"form": "25-NSE", "date": "2026-10-20", "items": ""}]})
check("已停止交易＋Form 25 → 建議 delisted、交人工", v == "needs_human" and s == "delisted" and a is None, (v, s))
check("理由含 SEC 申報", "25-NSE" in r, r)

v, s, r, a = RT.classify(T(), {"overview": {}, "inactive": [{"ticker": "ABC", "delisted_utc": "2019-01-01T00:00:00Z"}],
                               "events": [{"date": "2018-01-01", "ticker": "OLD"}], "bar_dates": []})
check("很久以前的下市／改名不算 → unknown", s == "unknown" and v == "needs_human", (v, s, r))

v, s, r, a = RT.classify(T(), {"overview": ACTIVE, "bar_dates": []})
check("現役但沒日線 → 建議停牌或換市場、交人工", s == "halted_or_market_changed" and a is None, (v, s))

v, s, r, a = RT.classify(T(), {"overview": None, "bar_dates": ["2026-10-27"]})
check("代號現況查不到（狀態未知）時不自動回", v == "needs_human" and a is None, (v, s))

v, s, r, a = RT.classify(T(type_="seam_fail"), {"overview": ACTIVE, "bar_dates": ["2026-10-27"]})
check("未支援的類型交人工", v == "needs_human" and s == "unsupported_type", (v, s))

v, s, r, a = RT.classify(T(), {"overview": None, "bar_dates": None})
check("全查不到 → unknown 並註明日線狀態未知", s == "unknown" and "狀態未知" in r, r)

# ── 回覆格式 ──
check("renamed 缺 identity_version 擋下", RT.validate_reply("renamed", {"new_symbol": "X", "effective": "2026-10-27"}, "") is not None)
check("renamed identity_version 要整數", RT.validate_reply("renamed", {"new_symbol": "X", "effective": "2026-10-27", "identity_version": "3"}, "") is not None)
check("renamed 完整通過", RT.validate_reply("renamed", {"new_symbol": "X", "effective": "2026-10-27", "identity_version": 3}, "") is None)
check("delisted 完整通過", RT.validate_reply("delisted", {"last_trading_day": "2026-10-26", "identity_version": 3}, "") is None)
check("halted 的 expected_resume 可空", RT.validate_reply("halted", {"expected_resume": None}, "") is None)
check("unknown 沒寫 note 擋下", RT.validate_reply("unknown", {}, " ") is not None)
check("未同意的 market_changed 擋下", RT.validate_reply("market_changed", {"new_market": "OTC"}, "") is not None)
check("source_gap 日期格式不對擋下", RT.validate_reply("source_gap", {"trading_days": ["10/27"]}, "") is not None)

q, u = RT.reply_update(T(round_=2), "unknown", {}, "查無", "2026-10-29T08:00:00-04:00")
check("回覆條件鎖住給我方、working、同一輪", q == {"_id": T()["_id"], "to": "datacenter", "status": "working", "round": 2}, q)
check("回覆只 $push replies＋$set status（不碰開單方欄位）",
      set(u) == {"$push", "$set"} and set(u["$push"]) == {"replies"} and u["$set"] == {"status": "replied"}, u)
check("回覆帶 by／round／美東時間", u["$push"]["replies"]["by"] == "datacenter" and u["$push"]["replies"]["round"] == 2
      and u["$push"]["replies"]["at"].endswith("-04:00"), u)
try:
    RT.reply_update(T(), "renamed", {"new_symbol": "X"}, "", "x")
    check("格式不合的回覆丟例外", False)
except ValueError:
    check("格式不合的回覆丟例外", True)

# ── 權限與開單 ──
check("刪除條件寫死 from=datacenter", RT.delete_query("x") == {"_id": "x", "from": "datacenter"})
d = RT.new_ticket("spec_reply", "mongo_tickets-2026-10-28", {"spec": "x"}, "摘要", "2026-10-28T15:00:00-04:00", detail={"a": 1})
check("開單 _id 以 datacenter- 開頭、確定性", d["_id"] == "datacenter-spec_reply-mongo_tickets-2026-10-28", d["_id"])
check("開單欄位齊（from/to/status/round/replies）", d["from"] == "datacenter" and d["to"] == "local" and d["status"] == "open"
      and d["round"] == 1 and d["replies"] == [] and d["detail"] == {"a": 1}, d)
check("現在時間是美東帶時區", RT.now_et_iso()[-6:] in ("-04:00", "-05:00"), RT.now_et_iso())

print("\n通過 %d／失敗 %d" % (len(PASS), len(FAIL)))
sys.exit(1 if FAIL else 0)
