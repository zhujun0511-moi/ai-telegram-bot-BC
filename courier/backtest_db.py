# -*- coding: utf-8 -*-
"""
courier/backtest_db.py — 每日回測的 Mongo（2026-10-16「唯一的門」）

tasks/cfet_backtest.BacktestDB（含 _parse_list3_line）與 tasks/backtest_stats.StatsDB 逐字搬進門；
連線字串在門內讀。⚠️ 兩個類別保留 .client／.stock_db 屬性只給類別自己的方法用，
門外不得取用（tools/door_audit.py 規則 5）。bc_backtest_daily.yml 目前已停用（Jun 10-02）。
Python 3.9 兼容。
"""
import os
import pymongo
from datetime import datetime
from typing import Optional, List, Dict
import pytz

MONGO_URI  = os.getenv("MONGO_URI", "")
EST_TZ     = pytz.timezone("US/Eastern")

# StatsDB 用（原 tasks/backtest_stats 常數，逐字）
MIN_SAMPLE = 20


# ─────────────────────────────────────────────
# List3 解析（與 database.py 保持一致）
# ─────────────────────────────────────────────

def _parse_list3_line(line: str):
    """
    解析 List3 一行：
      xlb:alb,apd,...      → sector="XLB", tier="leader"
      xlb_mid:oln,ame,...  → sector="XLB", tier="mid"

    返回 (sector_upper, [ticker_upper, ...], tier)
    自動過濾空 ticker（smh 那行有 ",," 的空值）
    """
    line = line.strip().upper()
    if ":" not in line:
        return None, [], "leader"

    sector_raw, rest = line.split(":", 1)
    sector_raw = sector_raw.strip()
    tickers    = [t.strip() for t in rest.split(",") if t.strip()]

    if sector_raw.endswith("_MID"):
        sector = sector_raw[:-4]
        tier   = "mid"
    else:
        sector = sector_raw
        tier   = "leader"

    return sector, tickers, tier


# ─────────────────────────────────────────────
# DB 操作
# ─────────────────────────────────────────────

class BacktestDB:
    def __init__(self):
        if not MONGO_URI:
            raise RuntimeError("MONGO_URI 未設定")
        self.client   = pymongo.MongoClient(MONGO_URI)
        self.stock_db = self.client["StockData"]
        self._setup_indices()

        # 從 MongoDB 讀取 ticker 映射（啟動時加載一次）
        self._leader_to_sector = {}   # ticker → sector ETF
        self._leader_to_tier   = {}   # ticker → "leader" / "mid"
        self._list1_tickers    = set()
        self._list2_tickers    = set()
        self._load_configs()

    def _setup_indices(self):
        self.stock_db["Backtest_Progress"].create_index("task_id", unique=True)
        self.stock_db["Backtest_Events"].create_index(
            [("ticker", pymongo.ASCENDING), ("signal_date", pymongo.DESCENDING)]
        )
        self.stock_db["Backtest_Stats"].create_index("condition_key", unique=True)

    def _load_configs(self):
        """
        從 MongoDB Configs 讀取所有 ticker 映射。
        禁止硬編碼 ticker 列表。
        """
        cfg = self.stock_db["Configs"].find_one({"type": "ticker_lists"})
        if not cfg:
            print("⚠️ BacktestDB: Configs 未找到，映射為空")
            return

        lists = cfg.get("lists", {})

        # List1
        self._list1_tickers = set(lists.get("list_1", []))

        # List2
        self._list2_tickers = set(lists.get("list_2", []))

        # List3：解析 leader / mid，建立映射
        for line in lists.get("list_3", []):
            sector, tickers, tier = _parse_list3_line(line)
            if sector and tickers:
                for t in tickers:
                    self._leader_to_sector[t] = sector
                    self._leader_to_tier[t]   = tier

        print(
            f"✅ BacktestDB Configs 加載完成："
            f"List1={len(self._list1_tickers)} "
            f"List2={len(self._list2_tickers)} "
            f"個股映射={len(self._leader_to_sector)}"
        )

    def get_bars(self, ticker: str, period: str,
                 snapshot_date: Optional[str] = None) -> List[dict]:
        """
        讀取 bars，支持快照隔離。
        snapshot_date 不為 None 時，只返回該日期之前的 bar。
        bars 新在前排序，與數據中心一致。
        """
        ticker = ticker.upper()
        col    = self.stock_db["Bars"]
        doc    = col.find_one({"ticker": ticker, "period": period})
        if not doc or "bars" not in doc:
            return []

        bars = doc["bars"]

        if snapshot_date:
            bars = [b for b in bars if b["t"][:10] <= snapshot_date]

        return bars

    def get_spy_bars_d(self, snapshot_date: Optional[str] = None) -> List[dict]:
        return self.get_bars("SPY", "D", snapshot_date)

    def save_event(self, event: dict):
        """保存回測事件到 Backtest_Events"""
        col = self.stock_db["Backtest_Events"]
        col.insert_one(event)

    def event_exists(self, ticker: str, signal_date: str, frame: str) -> bool:
        """防止重複記錄同一信號"""
        col = self.stock_db["Backtest_Events"]
        return col.find_one({
            "ticker":        ticker,
            "signal_date":   signal_date,
            "trigger_frame": frame,
        }) is not None

    # ── 進度管理 ──

    def get_backtest_progress(self) -> int:
        col = self.stock_db["Backtest_Progress"]
        doc = col.find_one({"task_id": "cfet_backtest"})
        return doc.get("progress", 0) if doc else 0

    def set_backtest_progress(self, idx: int, snapshot_date: str):
        col = self.stock_db["Backtest_Progress"]
        col.update_one(
            {"task_id": "cfet_backtest"},
            {"$set": {
                "task_id":       "cfet_backtest",
                "progress":      idx,
                "snapshot_date": snapshot_date,
                "updated_at":    datetime.now(EST_TZ),
            }},
            upsert=True
        )

    def get_backtest_snapshot_date(self) -> Optional[str]:
        col = self.stock_db["Backtest_Progress"]
        doc = col.find_one({"task_id": "cfet_backtest"})
        return doc.get("snapshot_date") if doc else None

    def is_backtest_completed(self) -> bool:
        col = self.stock_db["Backtest_Progress"]
        doc = col.find_one({"task_id": "cfet_backtest"})
        return bool(doc and doc.get("completed", False))

    def mark_backtest_completed(self):
        col = self.stock_db["Backtest_Progress"]
        col.update_one(
            {"task_id": "cfet_backtest"},
            {"$set": {"completed": True, "updated_at": datetime.now(EST_TZ)}},
            upsert=True
        )

    def get_all_backtest_tickers(self) -> List[str]:
        """
        從 MongoDB Configs 讀取所有回測標的。
        順序：List1 → List2 → List3個股（leader優先，mid其次）
        禁止硬編碼。
        """
        cfg = self.stock_db["Configs"].find_one({"type": "ticker_lists"})
        if not cfg:
            print("⚠️ get_all_backtest_tickers: Configs 未找到")
            return []

        lists = cfg.get("lists", {})
        seen  = set()
        result = []

        def _add(t: str):
            t = t.upper()
            if t not in seen:
                seen.add(t)
                result.append(t)

        # List1
        for t in lists.get("list_1", []):
            _add(t)

        # List2
        for t in lists.get("list_2", []):
            _add(t)

        # List3 個股：leader 優先，mid 其次
        leaders = []
        mids    = []
        for line in lists.get("list_3", []):
            sector, tickers, tier = _parse_list3_line(line)
            if tier == "leader":
                leaders.extend(tickers)
            else:
                mids.extend(tickers)

        for t in leaders:
            _add(t)
        for t in mids:
            _add(t)

        return result

    # ── 映射查詢（從內存讀，啟動時已加載）──

    def get_sector(self, ticker: str) -> Optional[str]:
        """返回個股所屬板塊ETF，如 'XLB'"""
        return self._leader_to_sector.get(ticker.upper())

    def get_tier(self, ticker: str) -> Optional[str]:
        """返回個股 tier：'leader' / 'mid'"""
        return self._leader_to_tier.get(ticker.upper())

    def is_list1(self, ticker: str) -> bool:
        return ticker.upper() in self._list1_tickers

    def is_list2(self, ticker: str) -> bool:
        return ticker.upper() in self._list2_tickers



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


