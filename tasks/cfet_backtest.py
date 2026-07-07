
"""
cfet_backtest.py — CFET 回測引擎（學習中心核心）

職責：
  - 在歷史 D1/H1/W1 數據上重放 CFET 狀態機邏輯
  - 記錄每次觸發的條件組合 + 結果（win/loss/cancel）
  - 同時記錄三層連動：SPY → 板塊ETF → 個股
  - 支持 checkpoint 斷點續跑
  - 輸出到 Backtest_Events collection

快照隔離設計：
  - 回測只讀取 snapshot_date 時間戳之前的 bar 數據
  - 數據中心每天更新不影響回測進行中的數據讀取

Ticker 來源：
  - 永遠從 MongoDB StockData.Configs 讀取，禁止硬編碼
  - 支持 list_3 的 leader / mid 兩個 tier，分層統計

Python 3.9 兼容，不用 str | None
"""

import os
import pymongo
import requests
from datetime import datetime
from typing import Optional, List, Dict
import pytz

# ─────────────────────────────────────────────
# 常數
# ─────────────────────────────────────────────

MONGO_URI  = os.getenv("MONGO_URI", "")
EST_TZ     = pytz.timezone("US/Eastern")

BATCH_SIZE = 10
MIN_D_BARS = 60
MIN_H_BARS = 200

MAX_HOLD_BARS_D = 30
MAX_HOLD_BARS_H = 60

MIN_RR = 1.5


def _send_signal_snapshot(ticker: str):
    """中樞神經 Signal Snapshot 落地呼叫（回測用途）。deliver_to_telegram 不傳，
    沿用通訊中心 bc_backtest 預設值 False（純落地供未來查詢/AI使用，不發送
    Telegram），不影響回測主流程。"""
    comm_hub_url   = os.getenv("COMM_HUB_URL", "").strip()
    webhook_secret = os.getenv("WEBHOOK_SECRET", "").strip()
    if not comm_hub_url or not webhook_secret:
        return
    base = comm_hub_url[:-len("/comm/send")] if comm_hub_url.endswith("/comm/send") else comm_hub_url
    try:
        requests.post(
            f"{base.rstrip('/')}/signal/snapshot",
            json={"ticker": ticker, "triggered_by": "bc_backtest"},
            headers={"WEBHOOK_SECRET": webhook_secret, "Content-Type": "application/json"},
            timeout=10,
        )
    except Exception as e:
        print(f"⚠️ signal_snapshot 呼叫失敗（不影響回測主流程）: {e}")


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
        col    = self.stock_db[f"Bars_{ticker}"]
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


# ─────────────────────────────────────────────
# 技術分析工具（輕量版，不依賴 ta 庫）
# ─────────────────────────────────────────────

def _find_bot_fractals(bars: List[dict]) -> List[dict]:
    """
    識別底部分形（與 cfet_scanner.py 邏輯一致）。
    bars 新在前，轉為舊在前處理。
    """
    if len(bars) < 5:
        return []
    rev  = list(reversed(bars))
    lows = [b["l"] for b in rev]
    n    = len(lows)
    found = []
    for i in range(2, n - 3):
        if (lows[i] < lows[i-1] and lows[i] < lows[i-2]
                and lows[i] < lows[i+1] and lows[i] < lows[i+2]):
            found.append({
                "price":    lows[i],
                "bar_idx":  n - 1 - i,
                "t":        rev[i]["t"],
                "bar_high": rev[i]["h"],
            })
    return found


def _find_top_fractals(bars: List[dict]) -> List[dict]:
    """識別頂部分形，用於找止贏目標"""
    if len(bars) < 5:
        return []
    rev   = list(reversed(bars))
    highs = [b["h"] for b in rev]
    n     = len(highs)
    found = []
    for i in range(2, n - 3):
        if (highs[i] > highs[i-1] and highs[i] > highs[i-2]
                and highs[i] > highs[i+1] and highs[i] > highs[i+2]):
            found.append({
                "price":   highs[i],
                "bar_idx": n - 1 - i,
                "t":       rev[i]["t"],
            })
    return found


def _calc_ema(closes: List[float], window: int) -> Optional[float]:
    if len(closes) < window:
        return None
    k   = 2 / (window + 1)
    ema = closes[0]
    for c in closes[1:]:
        ema = c * k + ema * (1 - k)
    return round(ema, 4)


def _get_trend(bars: List[dict]) -> str:
    if len(bars) < 50:
        return "unknown"
    closes = [b["c"] for b in reversed(bars)]
    ema9   = _calc_ema(closes, 9)
    ema20  = _calc_ema(closes, 20)
    ema50  = _calc_ema(closes, 50)
    if ema9 is None or ema20 is None or ema50 is None:
        return "unknown"
    if ema9 > ema20 > ema50:
        return "bull"
    if ema9 < ema20 < ema50:
        return "bear"
    return "neutral"


def _calc_rvol(bars: List[dict], window: int = 20) -> Optional[float]:
    if len(bars) < window + 1:
        return None
    current_vol = bars[0]["v"]
    avg_vol     = sum(b["v"] for b in bars[1:window+1]) / window
    if avg_vol <= 0:
        return None
    return round(current_vol / avg_vol, 2)


def _find_nearest_top_above(bars: List[dict], price: float) -> Optional[float]:
    tops = _find_top_fractals(bars)
    candidates = [t["price"] for t in tops if t["price"] > price]
    return min(candidates) if candidates else None


def _calc_rr(entry: float, sl: float, tp: float) -> Optional[float]:
    risk   = abs(entry - sl)
    reward = abs(tp - entry)
    if risk <= 0:
        return None
    return round(reward / risk, 2)


# ─────────────────────────────────────────────
# 連動鏈環境判斷
# ─────────────────────────────────────────────

def _get_spy_environment(spy_bars_d: List[dict]) -> dict:
    trend = _get_trend(spy_bars_d)
    close = spy_bars_d[0]["c"] if spy_bars_d else None
    return {"trend": trend, "close": close}


def _get_sector_env(db: BacktestDB, ticker: str,
                    snapshot_date: str) -> Optional[dict]:
    """取個股所屬板塊的環境"""
    sector = db.get_sector(ticker)
    if not sector:
        return None
    bars_d = db.get_bars(sector, "D", snapshot_date)
    if not bars_d:
        return None
    return {"sector": sector, "trend": _get_trend(bars_d)}


# ─────────────────────────────────────────────
# 回測結果評估
# ─────────────────────────────────────────────

def _evaluate_outcome(bars_after: List[dict], entry: float,
                      sl: float, tp: Optional[float]) -> dict:
    """
    在信號觸發後的 bar 序列中評估結果。
    bars_after：信號後的 bar 列表（舊在前順序）
    """
    if not bars_after:
        return {"result": "open", "exit_bar": None, "exit_price": None,
                "actual_rr": None, "max_favorable": 0.0, "max_adverse": 0.0}

    risk = abs(entry - sl)
    max_favorable = 0.0
    max_adverse   = 0.0

    for i, bar in enumerate(bars_after):
        high = bar["h"]
        low  = bar["l"]

        favorable = (high - entry) / risk if risk > 0 else 0
        adverse   = (entry - low)  / risk if risk > 0 else 0
        max_favorable = max(max_favorable, favorable)
        max_adverse   = max(max_adverse,   adverse)

        if low <= sl:
            return {
                "result":        "loss",
                "exit_bar":      i,
                "exit_price":    sl,
                "actual_rr":     round(-1.0, 2),
                "max_favorable": round(max_favorable, 2),
                "max_adverse":   round(max_adverse,   2),
            }

        if tp and high >= tp:
            actual_rr = (tp - entry) / risk if risk > 0 else 0
            return {
                "result":        "win",
                "exit_bar":      i,
                "exit_price":    tp,
                "actual_rr":     round(actual_rr, 2),
                "max_favorable": round(max_favorable, 2),
                "max_adverse":   round(max_adverse,   2),
            }

    last_close = bars_after[-1]["c"]
    actual_rr  = (last_close - entry) / risk if risk > 0 else 0
    return {
        "result":        "open",
        "exit_bar":      len(bars_after),
        "exit_price":    last_close,
        "actual_rr":     round(actual_rr, 2),
        "max_favorable": round(max_favorable, 2),
        "max_adverse":   round(max_adverse,   2),
    }


# ─────────────────────────────────────────────
# 單個 ticker 回測
# ─────────────────────────────────────────────

def _backtest_one_ticker(db: BacktestDB, ticker: str,
                         snapshot_date: str,
                         spy_bars_d: List[dict]) -> int:
    """
    對一個 ticker 掃描所有歷史信號並記錄結果。
    返回：找到的信號數量。
    """
    bars_d = db.get_bars(ticker, "D", snapshot_date)
    bars_w = db.get_bars(ticker, "W", snapshot_date)

    if not bars_d or len(bars_d) < MIN_D_BARS:
        return 0

    spy_env    = _get_spy_environment(spy_bars_d)
    sector_env = _get_sector_env(db, ticker, snapshot_date)
    tier       = db.get_tier(ticker)       # "leader" / "mid" / None
    sector     = db.get_sector(ticker)     # "XLB" / None

    signals_found = 0

    # ── 掃描 D1 信號 ──
    for frame, bars in [("D1", bars_d), ("W1", bars_w)]:
        if not bars or len(bars) < MIN_D_BARS:
            continue

        bot_fractals = _find_bot_fractals(bars)

        for frac in bot_fractals:
            fractal_low  = frac["price"]
            fractal_high = frac["bar_high"]
            frac_date    = frac["t"][:10]

            # 找分形後的 bars（用於判斷突破和評估結果）
            bars_rev  = list(reversed(bars))   # 舊在前
            frac_idx  = next(
                (i for i, b in enumerate(bars_rev) if b["t"][:10] == frac_date),
                None
            )
            if frac_idx is None:
                continue

            bars_after_frac = bars_rev[frac_idx + 1:]
            if len(bars_after_frac) < 3:
                continue

            # 判斷突破確認：分形後第2根bar收盤 > 分形高點
            confirm_bar = bars_after_frac[1] if len(bars_after_frac) > 1 else None
            if confirm_bar is None:
                continue
            if confirm_bar["c"] <= fractal_high:
                continue

            signal_date = bars_after_frac[1]["t"][:10]

            # 防止重複記錄
            if db.event_exists(ticker, signal_date, frame):
                continue

            entry_price = confirm_bar["c"]
            sl_price    = round(fractal_low * 0.999, 4)
            tp_price    = _find_nearest_top_above(bars, entry_price)
            rr_est      = _calc_rr(entry_price, sl_price, tp_price) if tp_price else None

            # RR 過濾
            if rr_est is None or rr_est < MIN_RR:
                continue

            rvol = _calc_rvol(bars_after_frac[::-1])   # 新在前

            # 評估結果（信號後最多 MAX_HOLD_BARS_D 根bar）
            eval_bars = bars_after_frac[2: 2 + MAX_HOLD_BARS_D]
            outcome   = _evaluate_outcome(eval_bars, entry_price, sl_price, tp_price)

            # 獲取信號觸發時的板塊環境（用 signal_date 作為快照）
            sector_env_at_signal = _get_sector_env(db, ticker, signal_date)

            event = {
                "ticker":        ticker,
                "signal_date":   signal_date,
                "trigger_frame": frame,
                "tier":          tier or "unknown",
                "sector":        sector or "unknown",
                "entry": {
                    "price":       entry_price,
                    "sl":          sl_price,
                    "tp":          tp_price,
                    "rr_est":      rr_est,
                    "rvol":        rvol,
                    "touch_count": 1,   # 待實現，暫時硬編碼
                },
                "chain": {
                    "spy_trend":    spy_env.get("trend"),
                    "sector":       sector_env_at_signal.get("sector") if sector_env_at_signal else None,
                    "sector_trend": sector_env_at_signal.get("trend")  if sector_env_at_signal else None,
                    "chain_aligned": (
                        spy_env.get("trend") == "bull"
                        and sector_env_at_signal is not None
                        and sector_env_at_signal.get("trend") == "bull"
                    ),
                },
                "outcome":     outcome,
                "snapshot_date": snapshot_date,
                "created_at":  datetime.now(EST_TZ),
            }

            db.save_event(event)
            _send_signal_snapshot(ticker)
            signals_found += 1

    return signals_found


# ─────────────────────────────────────────────
# 批次回測主入口
# ─────────────────────────────────────────────

def run_backtest_batch(db: BacktestDB) -> dict:
    """
    批次執行回測，checkpoint 續跑。
    每次調用處理 BATCH_SIZE 個 ticker。
    ALL_DONE 後自動觸發統計分析。
    """
    # 固定 snapshot_date（回測期間不變）
    snapshot_date = db.get_backtest_snapshot_date()
    if not snapshot_date:
        snapshot_date = datetime.now(EST_TZ).strftime("%Y-%m-%d")
        db.set_backtest_progress(0, snapshot_date)
        print(f"📅 回測快照日期固定為：{snapshot_date}")

    if db.is_backtest_completed():
        return {"status": "ALREADY_DONE", "snapshot_date": snapshot_date}

    all_tickers = db.get_all_backtest_tickers()
    if not all_tickers:
        return {"status": "NO_TICKERS"}

    progress = db.get_backtest_progress()
    batch    = all_tickers[progress: progress + BATCH_SIZE]

    if not batch:
        db.mark_backtest_completed()
        print("✅ 回測全部完成")
        return {"status": "ALL_DONE", "snapshot_date": snapshot_date}

    print(f"🔄 回測 | 進度 {progress}/{len(all_tickers)} | 本批: {batch}")

    spy_bars_d    = db.get_spy_bars_d(snapshot_date)
    signals_found = 0

    for ticker in batch:
        try:
            n = _backtest_one_ticker(db, ticker, snapshot_date, spy_bars_d)
            signals_found += n
            print(f"  ✅ {ticker}: {n} 個信號")
        except Exception as e:
            print(f"  ❌ {ticker} 回測異常: {e}")

    new_progress = progress + len(batch)
    db.set_backtest_progress(new_progress, snapshot_date)
    print(f"💾 進度已保存: {new_progress}/{len(all_tickers)} | 本批信號: {signals_found}")

    return {
        "status":        "BATCH_DONE",
        "progress":      new_progress,
        "total":         len(all_tickers),
        "signals_found": signals_found,
        "snapshot_date": snapshot_date,
    }

