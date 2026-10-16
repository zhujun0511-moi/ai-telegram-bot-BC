# -*- coding: utf-8 -*-
"""
courier/verify_db.py — 週末核查的 Mongo（2026-10-16「唯一的門」）

run_verify_weekend.VerifyDB 逐字搬進門（Bars 讀寫走 data_core）；連線字串在門內讀。
原本引用的腳本常數 BARS_LIMIT／TASK_NAME／_now_est 改由建立時傳入（值不變）。
另收 run_verify_weekend 原本在門外直接操作 db.stock_db 的：鎖（acquire_lock／release_lock）、交易日曆讀取。
⚠️ .client／.stock_db 只給本類別方法用，門外不得取用（tools/door_audit.py 規則 5）。Python 3.9 相容。
"""
import os
import uuid
from datetime import datetime, timedelta
from typing import List, Optional

import pymongo
from pymongo.errors import DuplicateKeyError

from courier import data_core as dc

MONGO_URI = os.getenv("MONGO_URI", "")


class VerifyDB:
    def __init__(self, bars_limit, task_name, now_fn):
        # 2026-10-16：原本直接引用 run_verify_weekend 的 BARS_LIMIT／TASK_NAME／_now_est，改成建立時傳入（值不變）
        self._bars_limit, self._task_name, self._now = bars_limit, task_name, now_fn
        if not MONGO_URI:
            raise RuntimeError("MONGO_URI 未設定")
        self.client   = pymongo.MongoClient(MONGO_URI)
        self.stock_db = self.client["StockData"]
        self.stock_db["Verify_Verdicts"].create_index(
            [("ticker", pymongo.ASCENDING),
             ("period", pymongo.ASCENDING),
             ("target_date", pymongo.ASCENDING)],
            unique=True,
        )
        # v1.1 新增：Ticker_Identity 唯一索引
        self.stock_db["Ticker_Identity"].create_index(
            [("ticker", pymongo.ASCENDING)],
            unique=True,
        )

    # ── ticker 清單（與 DC get_all_tickers 同源：Configs.full_set）──
    def get_all_tickers(self) -> List[str]:
        cfg = self.stock_db["Configs"].find_one({"type": "ticker_lists"})
        if not cfg:
            return []
        return cfg.get("full_set", [])

    # ── bars 讀寫（語義克隆自 DC database.py）──
    def get_bars(self, ticker: str, period: str) -> List[dict]:
        ticker = ticker.upper()
        doc = self.stock_db[dc.BARS].find_one(dc.bars_filter(ticker, period))
        if doc and "bars" in doc:
            return doc["bars"]
        return []

    def push_bars(self, ticker: str, period: str, new_bars: List[dict]) -> str:
        """
        與 DC push_bars 同語義（去重合併、新在前、截斷 limit），
        唯一增強：合併後按 t 降序排序——強制執行『bars 新在前』系統標準，
        防止補抓中段缺口時破壞排序（DC 版假設 new 恆比 existing 新）。
        """
        ticker = ticker.upper()
        col = self.stock_db[dc.BARS]
        col.create_index(
            [("ticker", pymongo.ASCENDING), ("period", pymongo.ASCENDING)],
            unique=True,
        )
        limit = self._bars_limit.get(period, dc.DEFAULT_BARS_LIMIT)
        filt = dc.bars_filter(ticker, period)
        doc = col.find_one(filt)
        if doc:
            # 2026-08-23 DAL：同 t 新贏合併走 data_core.merge_bars_by_t（皇冠邏輯共享 DC）。
            merged = dc.merge_bars_by_t(doc.get("bars", []), new_bars, limit)
            col.update_one(filt, dc.bars_set_update(merged))
            return "updated"
        # insert 分支：BC 強制新在前、排序不去重 → data_core.sort_cap_bars。
        col.insert_one(dc.bars_insert_doc(ticker, period, dc.sort_cap_bars(new_bars, limit)))
        return "inserted"

    def replace_bars(self, ticker: str, period: str, bars: List[dict]):
        """整筆覆蓋 bars 陣列（數據品質修正 pass 專用，不走去重合併）。"""
        self.stock_db[dc.BARS].update_one(
            dc.bars_filter(ticker.upper(), period),
            dc.bars_set_update(bars),
        )

    # ── verdict ──
    def get_verdict(self, ticker: str, period: str, target_date: str) -> Optional[str]:
        doc = self.stock_db["Verify_Verdicts"].find_one(
            {"ticker": ticker, "period": period, "target_date": target_date})
        return doc.get("verdict") if doc else None

    def set_verdict(self, ticker: str, period: str, target_date: str,
                    verdict: str, note: str = ""):
        # 精確 filter + upsert，不用 $or（M0 upsert 競態前科）
        self.stock_db["Verify_Verdicts"].update_one(
            {"ticker": ticker, "period": period, "target_date": target_date},
            {"$set": {"verdict": verdict, "note": note,
                      "checked_at": self._now()}},
            upsert=True,
        )

    def count_verdicts(self, target_date: str) -> dict:
        out = {"filled": 0, "confirmed_empty": 0, "blocked": 0}
        for row in self.stock_db["Verify_Verdicts"].aggregate([
            {"$match": {"target_date": target_date}},
            {"$group": {"_id": "$verdict", "n": {"$sum": 1}}},
        ]):
            out[row["_id"]] = row["n"]
        return out

    # ── Ticker_Identity（v1.1 新增：ticker 重複使用防禦）──
    def get_ticker_identity(self, ticker: str) -> Optional[dict]:
        return self.stock_db["Ticker_Identity"].find_one({"ticker": ticker.upper()})

    def set_ticker_identity(self, ticker: str, list_date, cik, company_name):
        self.stock_db["Ticker_Identity"].update_one(
            {"ticker": ticker.upper()},
            {"$set": {
                "list_date":    list_date,
                "cik":          cik,
                "company_name": company_name,
                "checked_at":   self._now(),
            }},
            upsert=True,
        )

    def update_ticker_identity(self, ticker: str, fields: dict):
        """（2026-10-01 E）每週身份刷新寫入：只 $set 給定欄位（schema 見 MongoDB_Standard Ticker_Identity）。"""
        self.stock_db["Ticker_Identity"].update_one(
            {"ticker": ticker.upper()}, {"$set": fields}, upsert=True)

    def get_oldest_bar_date(self, ticker: str, period: str) -> Optional[str]:
        bars = self.get_bars(ticker, period)
        if not bars:
            return None
        return min(b["t"][:10] for b in bars)

    # ── Task_Log（標準欄位）──
    def write_task_log(self, status: str, progress: int, total: int,
                       last_error: str, started_at: datetime):
        self.stock_db["Task_Log"].insert_one({
            "task":        self._task_name,
            "status":      status,
            "progress":    progress,
            "total":       total,
            "last_error":  last_error,
            "started_at":  started_at,
            "finished_at": self._now(),
            "timestamp":   self._now(),
        })
