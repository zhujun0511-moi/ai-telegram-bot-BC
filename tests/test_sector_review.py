# -*- coding: utf-8 -*-
"""
test_sector_review.py — 歷史板塊問題股詳查（sector_history.py --step polygon／llm／apply）的離線測試：
假資料、不連 Polygon、不連 OpenRouter（Jun 10-18：呼叫 OpenRouter 之前止步）。用法：python tests/test_sector_review.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import sector_history as SH            # noqa: E402
from courier import openrouter as OR   # noqa: E402

PASS, FAIL = [], []


def check(name, ok, info=""):
    (PASS if ok else FAIL).append(name)
    print(("✅ " if ok else "❌ ") + name + ("" if ok else "  " + str(info)))


problems = [{"ticker": "SPYX", "reason": "SEC 查無"}, {"ticker": "NEE.S", "reason": "SEC 查無"},
            {"ticker": "ABCD", "reason": "SIC 7389 對照信心低"}, {"ticker": "ZZZZ", "reason": "SEC 查無"},
            {"ticker": "ABCD", "reason": "重複"}, {"ticker": "OLDCO", "reason": "SEC 無 SIC 代碼"}]
profiles = {"ABCD": {"name": "Abcd Inc", "sic": "7389", "sic_desc": "Services-Business Services, NEC"},
            "OLDCO": {"name": "Old Co", "sic": None, "sic_desc": None}}
poly = {"SPYX": {"ticker": "SPYX", "name": "SPDR fund", "type": "ETF"},
        "NEE-S": {"ticker": "NEE.S", "name": "NextEra pref", "type": "PFD"},
        "ABCD": {"ticker": "ABCD", "name": "ABCD Holdings", "type": "CS"}}
upd, queue = SH.classify_problems(problems, profiles, poly)
kinds = {u["ticker"]: u["kind"] for u in upd}
check("ETF → 基金不歸板塊", kinds.get("SPYX") == "fund")
check("優先股 → 對回母公司 NEE", kinds.get("NEE.S") == "derivative" and
      next(u for u in upd if u["ticker"] == "NEE.S")["parent"] == "NEE")
check("Polygon 查無、SEC 也無名稱 → 留人工", kinds.get("ZZZZ") == "unknown")
qt = [q["ticker"] for q in queue]
check("普通股（Polygon CS）→ 送模型詳查，名稱用 Polygon 的", qt.count("ABCD") == 1 and
      next(q for q in queue if q["ticker"] == "ABCD")["name"] == "ABCD Holdings")
check("Polygon 查無但 SEC 有名稱 → 也送詳查", "OLDCO" in qt)
check("重複的問題股只算一次", len(qt) == len(set(qt)))

pr = SH.build_prompt(queue)
check("提示詞含 11 個 XL、代號、公司名、SIC 說明，要求只回 JSON", all(x in pr for x in SH.XLS) and "ABCD Holdings" in pr
      and "Services-Business Services" in pr and "JSON array" in pr)

txt = 'Sure! ```json\n[{"ticker":"ABCD","xl":"XLI","before_2018":null,"confidence":"high"},' \
      '{"ticker":"OLDCO","xl":"XLQ","confidence":"high"},{"ticker":"NOTASKED","xl":"XLK"}]\n```'
ans = SH.parse_answer(txt, queue)
check("解析：合法答案保留", ans.get("ABCD", {}).get("xl") == "XLI")
check("解析：不認得的板塊（XLQ）丟掉、沒問的代號丟掉", "OLDCO" not in ans and "NOTASKED" not in ans)
check("解析：不是 JSON → 空", SH.parse_answer("I don't know", queue) == {})

m1, m2 = OR.ALLOWED_MODELS
take, manual = SH.agree({m1: {"A": {"xl": "XLK", "before_2018": None, "confidence": "high"},
                              "B": {"xl": "XLK", "before_2018": None, "confidence": "high"},
                              "C": {"xl": "XLV", "before_2018": None, "confidence": "low"},
                              "D": {"xl": None, "before_2018": None, "confidence": "low"}},
                         m2: {"A": {"xl": "XLK", "before_2018": "XLY", "confidence": "medium"},
                              "B": {"xl": "XLC", "before_2018": None, "confidence": "high"},
                              "C": {"xl": "XLV", "before_2018": None, "confidence": "high"}}}, OR.ALLOWED_MODELS)
check("兩模型一致且信心非 low → 採用", list(take) == ["A"] and take["A"]["xl"] == "XLK")
check("before_2018 不一致 → 不採用該欄（None）", take["A"]["before_2018"] is None)
check("不一致／有 low／只有一個模型答 → 人工", sorted(x["ticker"] for x in manual) == ["B", "C", "D"])

try:
    OR.chat("deepseek/whatever:free", "hi")
    check("不在白名單例外的模型 → 拒絕（不發請求）", False)
except ValueError:
    check("不在白名單例外的模型 → 拒絕（不發請求）", True)
check("白名單例外只有 Jun 指定的兩個", OR.ALLOWED_MODELS == ("nvidia/nemotron-3-ultra-550b-a55b:free",
                                                       "google/gemma-4-31b-it:free"))

print("\n%d 通過，%d 失敗" % (len(PASS), len(FAIL)))
sys.exit(1 if FAIL else 0)
