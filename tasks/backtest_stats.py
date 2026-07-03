
"""
backtest_stats.py — 統計分析 + 閾值優化（學習中心）

職責：
  - 讀取 Backtest_Events，按條件組合分組統計
  - 輸出：勝率、平均RR、樣本數、最優閾值
  - 三層連動分析：連動 vs 非連動的勝率差異
  - 結果寫入 Backtest_Stats（供 prompt 注入使用）
  - 結果寫入 Backtest_Thresholds（供 cfet_scanner 動態調整使用）

設計原則：
  - 樣本數 < 20 的組合標記為「數據不足」，不輸出結論
  - 所有結論附樣本數，讓 prompt 能標注置信度
  - Python 3.9 兼容
"""

import os
import pymongo
from datetime import datetime
from typing import Optional, List, Dict, Any
import pytz

MONGO_URI = os.getenv("MONGO_URI", "")
EST_TZ    = pytz.timezone("US/Eastern")

# 最少樣本數（低於此值不輸出結論）
MIN_SAMPLE = 20

# 連動溢價閾值：連動時勝率比非連動高多少才算顯著
CHAIN_PREMIUM_THRESHOLD = 0.10   # 10%


class StatsDB:
    def __init__(self):
        if not MONGO_URI:
            raise RuntimeError("MONGO_URI 未設定")
        self.client   = pymongo.MongoClient(MONGO_URI)
        self.stock_db = self.client["StockData"]

    def get_all_events(self) -> List[dict]:
        """讀取所有有結果的回測事件（排除 open）"""
        col  = self.stock_db["Backtest_Events"]
        docs = list(col.find(
            {"outcome.result": {"$in": ["win", "loss"]}},
            {"_id": 0}
        ))
        return docs

    def upsert_stat(self, condition_key: str, stat: dict):
        """寫入統計結果"""
        col = self.stock_db["Backtest_Stats"]
        col.update_one(
            {"condition_key": condition_key},
            {"$set": {**stat, "condition_key": condition_key,
                      "updated_at": datetime.now(EST_TZ)}},
            upsert=True
        )

    def upsert_thresholds(self, thresholds: dict):
        """寫入最優閾值"""
        col = self.stock_db["Backtest_Thresholds"]
        col.update_one(
            {"_id": "latest"},
            {"$set": {**thresholds, "updated_at": datetime.now(EST_TZ)}},
            upsert=True
        )

    def get_stats_summary(self) -> List[dict]:
        """取前20個樣本數最多的條件組合（供 prompt 注入）"""
        col  = self.stock_db["Backtest_Stats"]
        docs = list(
            col.find(
                {"sample_count": {"$gte": MIN_SAMPLE}},
                {"_id": 0}
            ).sort("win_rate", -1).limit(20)
        )
        return docs


# ─────────────────────────────────────────────
# 核心統計函數
# ─────────────────────────────────────────────

def _calc_win_rate(events: List[dict]) -> Optional[float]:
    if not events:
        return None
    wins = sum(1 for e in events if e["outcome"]["result"] == "win")
    return round(wins / len(events), 3)


def _calc_avg_rr(events: List[dict]) -> Optional[float]:
    rrs = [e["outcome"]["actual_rr"] for e in events
           if e["outcome"].get("actual_rr") is not None]
    if not rrs:
        return None
    return round(sum(rrs) / len(rrs), 2)


def _calc_expectancy(win_rate: float, avg_rr: float) -> float:
    """期望值 = 勝率 × 平均盈利RR + 敗率 × (-1)"""
    return round(win_rate * avg_rr + (1 - win_rate) * (-1), 3)


def _group_by(events: List[dict], key_fn) -> Dict[Any, List[dict]]:
    result = {}
    for e in events:
        k = key_fn(e)
        if k not in result:
            result[k] = []
        result[k].append(e)
    return result


def _stat_block(label: str, events: List[dict]) -> Optional[dict]:
    """計算一組事件的統計塊，樣本不足返回 None"""
    n = len(events)
    if n < MIN_SAMPLE:
        return None

    win_rate = _calc_win_rate(events)
    avg_rr   = _calc_avg_rr(events)
    if win_rate is None or avg_rr is None:
        return None

    return {
        "label":        label,
        "sample_count": n,
        "win_rate":     win_rate,
        "win_rate_pct": f"{win_rate*100:.1f}%",
        "avg_rr":       avg_rr,
        "expectancy":   _calc_expectancy(win_rate, avg_rr),
        "data_quality": "sufficient" if n >= 50 else "limited",
    }


# ─────────────────────────────────────────────
# 分析模塊
# ─────────────────────────────────────────────

def analyze_chain_alignment(events: List[dict], db: StatsDB):
    """
    核心分析：三層連動 vs 非連動的勝率差異
    這是整個自學習系統最重要的輸出。
    """
    print("\n── 連動鏈分析 ──")

    # 按 chain_aligned 分組
    aligned     = [e for e in events if e.get("chain", {}).get("chain_aligned") is True]
    not_aligned = [e for e in events if e.get("chain", {}).get("chain_aligned") is False]
    spy_only    = [e for e in events if e.get("chain", {}).get("spy_aligned") is True
                   and e.get("chain", {}).get("sector_aligned") is not True]

    for label, group, key in [
        ("SPY+板塊+個股 三層共振", aligned,     "chain_fully_aligned"),
        ("SPY對齊但板塊未對齊",   spy_only,     "chain_spy_only"),
        ("環境未對齊",           not_aligned,   "chain_not_aligned"),
    ]:
        stat = _stat_block(label, group)
        if stat:
            db.upsert_stat(f"chain_{key}", stat)
            print(f"  {label}: 勝率 {stat['win_rate_pct']} | "
                  f"平均RR {stat['avg_rr']} | 樣本 {stat['sample_count']}")
        else:
            print(f"  {label}: 樣本不足（{len(group)} 個）")

    # 計算連動溢價
    if aligned and not_aligned:
        wr_aligned     = _calc_win_rate(aligned)
        wr_not_aligned = _calc_win_rate(not_aligned)
        if wr_aligned and wr_not_aligned:
            premium = wr_aligned - wr_not_aligned
            print(f"  連動溢價: +{premium*100:.1f}% "
                  f"({'顯著' if premium >= CHAIN_PREMIUM_THRESHOLD else '不顯著'})")


def analyze_by_trend_combo(events: List[dict], db: StatsDB):
    """分析 D1+W1 趨勢組合的勝率"""
    print("\n── 趨勢組合分析 ──")
    grouped = _group_by(events, lambda e: (
        e.get("conditions", {}).get("trend_d1", "unknown"),
        e.get("conditions", {}).get("trend_w1", "unknown"),
    ))
    for (d1, w1), group in sorted(grouped.items(), key=lambda x: -len(x[1])):
        label = f"D1={d1} W1={w1}"
        stat  = _stat_block(label, group)
        if stat:
            db.upsert_stat(f"trend_{d1}_{w1}", stat)
            print(f"  {label}: 勝率 {stat['win_rate_pct']} | "
                  f"RR {stat['avg_rr']} | 樣本 {stat['sample_count']}")


def analyze_by_rvol(events: List[dict], db: StatsDB):
    """
    分析 RVOL 閾值的影響。
    找到最優 RVOL 切分點。
    """
    print("\n── RVOL 閾值分析 ──")
    thresholds = [0.8, 1.0, 1.2, 1.5, 2.0]

    best_wr    = 0.0
    best_rvol  = 1.0
    results    = []

    for rvol_min in thresholds:
        group = [e for e in events
                 if e.get("conditions", {}).get("rvol_at_signal") is not None
                 and e["conditions"]["rvol_at_signal"] >= rvol_min]
        stat = _stat_block(f"RVOL>={rvol_min}", group)
        if stat:
            results.append((rvol_min, stat))
            db.upsert_stat(f"rvol_min_{str(rvol_min).replace('.','_')}", stat)
            print(f"  RVOL >= {rvol_min}: 勝率 {stat['win_rate_pct']} | "
                  f"RR {stat['avg_rr']} | 樣本 {stat['sample_count']}")
            if stat["win_rate"] > best_wr:
                best_wr   = stat["win_rate"]
                best_rvol = rvol_min

    return best_rvol


def analyze_by_list(events: List[dict], db: StatsDB):
    """分析不同 List 標的的表現差異"""
    print("\n── List 分類分析 ──")
    grouped = _group_by(events, lambda e: e.get("ticker_list", "unknown"))
    for ticker_list, group in sorted(grouped.items()):
        stat = _stat_block(ticker_list, group)
        if stat:
            db.upsert_stat(f"list_{ticker_list}", stat)
            print(f"  {ticker_list}: 勝率 {stat['win_rate_pct']} | "
                  f"RR {stat['avg_rr']} | 樣本 {stat['sample_count']}")


def analyze_rr_filter(events: List[dict], db: StatsDB):
    """分析 RR 門檻過濾效果：只做高RR信號是否勝率更高"""
    print("\n── RR 門檻分析 ──")
    thresholds = [1.5, 2.0, 2.5, 3.0]

    best_expectancy = -999.0
    best_rr_min     = 1.5

    for rr_min in thresholds:
        group = [e for e in events
                 if e.get("prices", {}).get("tp") is not None
                 and e.get("conditions", {}).get("rr_estimated") is not None
                 and e["conditions"]["rr_estimated"] >= rr_min]
        stat = _stat_block(f"RR_est>={rr_min}", group)
        if stat:
            db.upsert_stat(f"rr_min_{str(rr_min).replace('.','_')}", stat)
            print(f"  RR_est >= {rr_min}: 勝率 {stat['win_rate_pct']} | "
                  f"期望值 {stat['expectancy']} | 樣本 {stat['sample_count']}")
            if stat["expectancy"] > best_expectancy:
                best_expectancy = stat["expectancy"]
                best_rr_min     = rr_min

    return best_rr_min


def build_prompt_injection(db: StatsDB) -> str:
    """
    生成可以直接注入 prompt_1b.md 的統計摘要文本。
    這是方向A的核心輸出。
    """
    stats = db.get_stats_summary()
    if not stats:
        return "（歷史統計數據暫無，樣本積累中）"

    lines = ["【歷史回測統計（自學習系統）】"]

    for s in stats[:8]:   # 只注入前8條，控制 token 消耗
        quality = "⚠️數據有限" if s["data_quality"] == "limited" else ""
        lines.append(
            f"- {s['label']}：勝率 {s['win_rate_pct']}，"
            f"平均RR {s['avg_rr']}，"
            f"期望值 {s['expectancy']}，"
            f"樣本 {s['sample_count']} 次 {quality}"
        )

    lines.append("（以上為歷史數據參考，當前市場環境可能有所不同）")
    return "\n".join(lines)


# ─────────────────────────────────────────────
# 主入口
# ─────────────────────────────────────────────

def run_stats_analysis(db: StatsDB) -> dict:
    """
    執行完整統計分析。
    回測完成後調用一次即可，後續每次回測完成後增量更新。
    """
    print("📊 開始統計分析")

    events = db.get_all_events()
    total  = len(events)
    print(f"  讀取到 {total} 個有效回測事件")

    if total < MIN_SAMPLE:
        print(f"  ⚠️ 樣本數不足 {MIN_SAMPLE}，暫停分析，等待更多數據")
        return {"status": "INSUFFICIENT_DATA", "total_events": total}

    # 執行各維度分析
    analyze_chain_alignment(events, db)
    analyze_by_trend_combo(events, db)
    best_rvol   = analyze_by_rvol(events, db)
    best_rr_min = analyze_rr_filter(events, db)
    analyze_by_list(events, db)

    # 寫入最優閾值（方向B輸出）
    thresholds = {
        "rvol_min":  best_rvol,
        "rr_min":    best_rr_min,
        "sample_base": total,
        "generated_at": str(datetime.now(EST_TZ).date()),
    }
    db.upsert_thresholds(thresholds)
    print(f"\n✅ 最優閾值：RVOL >= {best_rvol}，RR >= {best_rr_min}")

    # 生成 prompt 注入文本
    injection = build_prompt_injection(db)
    print(f"\n── Prompt 注入預覽 ──\n{injection}")

    return {
        "status":         "DONE",
        "total_events":   total,
        "best_rvol":      best_rvol,
        "best_rr_min":    best_rr_min,
        "prompt_injection": injection,
    }


if __name__ == "__main__":
    db     = StatsDB()
    result = run_stats_analysis(db)
    print(f"\n最終結果: {result['status']}")
