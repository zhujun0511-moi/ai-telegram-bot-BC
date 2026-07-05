"""
run_verify_weekend.py — V3 數據完整性核查 GitHub Actions 版 v1.0

取代 DC tasks/v3_processor.process_v3_verify_task 的多輪自激發設計。
設計依據：handoff_20260703_bc_buildout.md

舊版為什麼跑很多輪（本版逐條消滅）：
  1. 判準「最後 D bar == 今天」撞上 Polygon 免費層當日數據延遲
     → 本版目標日 = 最近已完成交易日（週六跑 = 週五），數據 100% 已出爐
  2. 「確認無資料」結論不持久化，每輪重抓同一批空結果
     → 本版 verdict 寫入 Verify_Verdicts（filled / confirmed_empty / blocked），
       confirmed_empty 為永久白名單，斷點續跑天然成立（進度即 verdict 本身）
  3. 多實例爭搶 5 req/min 額度 → GHA 單 runner 序列執行，嚴格 13.5s 節奏
  4. 判準不分層 → 歷史數據必須完備，當日數據交給平日增量

假日防禦（canary walk-back）：
  目標日可能是休市日（_prev_trading_day 只排週末，與既有系統一致的簡化）。
  先用 SPY 做金絲雀：Mongo 已有該日 bar → 正常；否則抓 Polygon，
  200-空 → 視為休市，目標日回退一個交易日重試（最多 5 次）。
  避免對休市日燒 608 次無意義 API 呼叫。

鎖：MongoDB 層級鎖（token 比對），沿用 run_phase_calc_gha.py v1.1/v1.2
  的已驗證模式（含 DuplicateKeyError upsert=False 重試）。

Python 3.9 兼容。所有時刻判斷腳本內用 EST 自算，不信任 GHA cron 時刻。
"""

import os
import sys
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional, List

import pytz
import pymongo
import requests
from pymongo.errors import DuplicateKeyError

EST_TZ = pytz.timezone("US/Eastern")

MONGO_URI      = os.getenv("MONGO_URI", "")
POLYGON_KEY    = os.getenv("POLYGON_KEY", "")
COMM_HUB_URL   = os.getenv("COMM_HUB_URL", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

POLYGON_DELAY     = 13.5
POLYGON_WAIT      = 65
POLYGON_MAX_RETRY = 2

VERIFY_PERIODS   = ["D", "W"]
BARS_LIMIT       = {"D": 500, "W": 1500}

BLOCK_MIN_SAMPLE = 10      # 熔斷判斷的最小樣本
BLOCK_RATIO      = 0.5     # blocked 比例門檻
BLOCK_COOLDOWN   = 600     # 熔斷後等待秒數（GHA 時間便宜，等待而非中止）

MAX_JOB_SECONDS   = 5.5 * 3600
LOCK_STALE_SECONDS = MAX_JOB_SECONDS + 15 * 60
CANARY_MAX_WALKBACK = 5

TASK_NAME = "bc_verify_weekend"
REPORT_TYPE = "bc_verify"     # 非 cfet_alert，走標準頻道


def _now_est() -> datetime:
    return datetime.now(EST_TZ)


def _now_str() -> str:
    return _now_est().strftime("%Y-%m-%d %H:%M:%S EST")


def _log(msg: str):
    print(f"[{_now_str()}] {msg}", flush=True)


# ─────────────────────────────────────────────
# 日期工具（16:00 翻轉語義，與 handoff 對照表一致）
# ─────────────────────────────────────────────

FALLBACK_HOLIDAYS = {
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-04-18", "2025-05-26",
    "2025-06-19", "2025-07-04", "2025-09-01", "2025-11-27", "2025-12-25",
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03", "2026-05-25",
    "2026-06-19", "2026-07-03", "2026-09-07", "2026-11-26", "2026-12-25",
}
_HOLIDAY_CACHE = {"holidays": None}


def load_market_calendar(stock_db):
    """
    自 Configs {type:"market_calendar"} 載入假日表（與 AC timectx 同源）。
    失敗回落 FALLBACK_HOLIDAYS。main() 於 DB 初始化後、任何日期計算前呼叫。
    """
    try:
        doc = stock_db["Configs"].find_one({"type": "market_calendar"})
        if doc and doc.get("holidays"):
            _HOLIDAY_CACHE["holidays"] = set(doc["holidays"])
            _log(f"market_calendar 載入 {len(_HOLIDAY_CACHE['holidays'])} 個假日")
            return
    except Exception as e:
        _log(f"market_calendar 讀取失敗，使用內建兜底表: {e}")
    _HOLIDAY_CACHE["holidays"] = None


def _is_trading_day(d) -> bool:
    if d.weekday() >= 5:
        return False
    hol = _HOLIDAY_CACHE["holidays"] or FALLBACK_HOLIDAYS
    return d.strftime("%Y-%m-%d") not in hol


def _prev_trading_day(d):
    """前一個交易日（假日感知，v1.2：不再只排週末）"""
    d -= timedelta(days=1)
    while not _is_trading_day(d):
        d -= timedelta(days=1)
    return d


def get_completed_trading_date(now_est: Optional[datetime] = None) -> str:
    """
    最近『已完成』交易日：
      週末/假日概念日 → 回退到週五
      平日 16:00 前   → 昨天（含盤中，數據鏈永不認領未完成的今天）
      平日 16:00 後   → 今天
    """
    if now_est is None:
        now_est = _now_est()
    today = now_est.date()
    if not _is_trading_day(today) or now_est.hour < 16:
        return _prev_trading_day(today).strftime("%Y-%m-%d")
    return today.strftime("%Y-%m-%d")


def _get_week_monday(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    monday = dt - timedelta(days=dt.weekday())
    return monday.strftime("%Y-%m-%d")


# ─────────────────────────────────────────────
# 通知 / Task_Log
# ─────────────────────────────────────────────

def _notify(msg: str):
    """推送到 Telegram（via AC /comm/send）。失敗只 print，不阻塞主流程。"""
    if not COMM_HUB_URL:
        _log(f"[notify] COMM_HUB_URL 未設定，跳過推送: {msg[:80]}")
        return
    try:
        resp = requests.post(
            COMM_HUB_URL,
            json={"content": msg, "report_type": REPORT_TYPE},
            headers={"x-webhook-secret": WEBHOOK_SECRET,
                     "Content-Type": "application/json"},
            timeout=10,
        )
        _log(f"[notify] 推送: {resp.status_code}")
    except Exception as e:
        _log(f"[notify] 推送失敗: {e}")


def _dispatch_next_workflow():
    """
    workflow 接力：乾淨完成後 dispatch 下一個 workflow。
    NEXT_WORKFLOW = 目標 yml 檔名（空值 = 不接力）。
    使用 GHA 內建 GITHUB_TOKEN（yml 需 permissions: actions: write）。
    """
    wf = os.getenv("NEXT_WORKFLOW", "").strip()
    if not wf:
        return
    repo  = os.getenv("GITHUB_REPOSITORY", "")
    token = os.getenv("GITHUB_TOKEN", "")
    ref   = os.getenv("GITHUB_REF_NAME", "main")
    if not (repo and token):
        _log(f"[chain] 缺 GITHUB_REPOSITORY/GITHUB_TOKEN，無法接力 {wf}")
        return
    try:
        resp = requests.post(
            f"https://api.github.com/repos/{repo}/actions/workflows/{wf}/dispatches",
            json={"ref": ref},
            headers={"Authorization": f"Bearer {token}",
                     "Accept": "application/vnd.github+json"},
            timeout=10,
        )
        _log(f"[chain] 接力 dispatch {wf}: {resp.status_code}")
    except Exception as e:
        _log(f"[chain] 接力失敗 {wf}: {e}")


# ─────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────

class VerifyDB:
    def __init__(self):
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

    # ── ticker 清單（與 DC get_all_tickers 同源：Configs.full_set）──
    def get_all_tickers(self) -> List[str]:
        cfg = self.stock_db["Configs"].find_one({"type": "ticker_lists"})
        if not cfg:
            return []
        return cfg.get("full_set", [])

    # ── bars 讀寫（語義克隆自 DC database.py）──
    def get_bars(self, ticker: str, period: str) -> List[dict]:
        ticker = ticker.upper()
        doc = self.stock_db[f"Bars_{ticker}"].find_one(
            {"ticker": ticker, "period": period})
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
        col = self.stock_db[f"Bars_{ticker}"]
        col.create_index(
            [("ticker", pymongo.ASCENDING), ("period", pymongo.ASCENDING)],
            unique=True,
        )
        limit = BARS_LIMIT.get(period, 500)
        doc = col.find_one({"ticker": ticker, "period": period})
        if doc:
            existing   = doc.get("bars", [])
            existing_t = {b["t"] for b in existing}
            truly_new  = [b for b in new_bars if b["t"] not in existing_t]
            merged     = sorted(truly_new + existing,
                                key=lambda b: b["t"], reverse=True)[:limit]
            col.update_one(
                {"ticker": ticker, "period": period},
                {"$set": {"bars": merged, "updated_at": _now_est()}},
            )
            return "updated"
        col.insert_one({
            "ticker": ticker, "period": period,
            "bars": sorted(new_bars, key=lambda b: b["t"], reverse=True)[:limit],
            "updated_at": _now_est(),
        })
        return "inserted"

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
                      "checked_at": _now_est()}},
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

    # ── Task_Log（標準欄位）──
    def write_task_log(self, status: str, progress: int, total: int,
                       last_error: str, started_at: datetime):
        self.stock_db["Task_Log"].insert_one({
            "task":        TASK_NAME,
            "status":      status,
            "progress":    progress,
            "total":       total,
            "last_error":  last_error,
            "started_at":  started_at,
            "finished_at": _now_est(),
            "timestamp":   _now_est(),
        })


# ─────────────────────────────────────────────
# MongoDB 層級鎖（克隆 run_phase_calc_gha v1.1/v1.2 已驗證模式）
# ─────────────────────────────────────────────

LOCK_ID = "bc_verify_lock"


def _acquire_lock(db: VerifyDB) -> bool:
    col   = db.stock_db["System_State"]
    now   = _now_est()
    stale = now - timedelta(seconds=LOCK_STALE_SECONDS)
    token = uuid.uuid4().hex
    filter_query = {
        "id": LOCK_ID,
        "$or": [
            {"is_running": {"$exists": False}},
            {"is_running": False},
            {"lock_acquired_at": {"$lt": stale}},
        ],
    }
    update_doc = {"$set": {"is_running": True,
                           "lock_acquired_at": now,
                           "lock_token": token}}
    try:
        col.find_one_and_update(filter_query, update_doc, upsert=True)
    except DuplicateKeyError:
        _log("⚠️ 搶鎖 upsert 撞 DuplicateKeyError（併發場景），upsert=False 重試")
        col.find_one_and_update(filter_query, update_doc, upsert=False)
    doc = col.find_one({"id": LOCK_ID})
    return bool(doc) and doc.get("lock_token") == token


def _release_lock(db: VerifyDB):
    db.stock_db["System_State"].update_one(
        {"id": LOCK_ID}, {"$set": {"is_running": False}})


# ─────────────────────────────────────────────
# Polygon（語義克隆自 DC _fetch_polygon v12.32：[]=確認空 / None=狀態未知）
# ─────────────────────────────────────────────

def _fetch_polygon(ticker: str, period_label: str,
                   start_date: str, end_date: str):
    period_map = {"D": (1, "day"), "W": (1, "week")}
    mult, p_api = period_map[period_label]
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{mult}/{p_api}/"
        f"{start_date}/{end_date}"
        f"?adjusted=true&extended_hours=false&sort=desc&limit=50000&apiKey={POLYGON_KEY}"
    )
    for attempt in range(POLYGON_MAX_RETRY):
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code in (429, 403):
                _log(f"  ⚠️ [{ticker}/{period_label}] 限速/封鎖 "
                     f"({resp.status_code})，等待 {POLYGON_WAIT}s 重試")
                time.sleep(POLYGON_WAIT)
                continue
            if resp.status_code != 200:
                _log(f"  ❌ Polygon HTTP {resp.status_code} "
                     f"({ticker}/{period_label})，狀態未知")
                return None
            results = resp.json().get("results", [])
            if not results:
                return []
            processed = []
            for r in results:
                dt_obj = datetime.fromtimestamp(r["t"] / 1000, EST_TZ)
                # W period：Polygon 返回週日戳，統一修正為當週週一（W-MON 錨定）
                if period_label == "W":
                    dt_obj = dt_obj + timedelta(days=1)
                processed.append({
                    "t": dt_obj.strftime("%Y-%m-%d %H:%M:%S"),
                    "o": r["o"], "h": r["h"],
                    "l": r["l"], "c": r["c"], "v": r["v"],
                })
            return processed
        except Exception as e:
            _log(f"  ❌ Polygon 連線異常 ({ticker}/{period_label}): {e}")
            if attempt == POLYGON_MAX_RETRY - 1:
                return None
            time.sleep(POLYGON_WAIT)
    return None


# ─────────────────────────────────────────────
# 核查判準（語義克隆自 DC _check_ticker_period）
# ─────────────────────────────────────────────

def _check_ticker_period(db: VerifyDB, ticker: str,
                         period: str, target_date: str) -> bool:
    try:
        bars = db.get_bars(ticker, period)
        if not bars:
            return False
        last_date = max(b["t"][:10] for b in bars)
        if period == "D":
            return last_date == target_date
        if period == "W":
            return _get_week_monday(last_date) == _get_week_monday(target_date)
        return False
    except Exception as e:
        _log(f"  ⚠️ _check_ticker_period 異常 ({ticker}/{period}): {e}")
        return False


# ─────────────────────────────────────────────
# 金絲雀：目標日休市偵測 + 回退
# ─────────────────────────────────────────────

def _resolve_target_date(db: VerifyDB) -> Optional[str]:
    target = get_completed_trading_date()
    for _ in range(CANARY_MAX_WALKBACK):
        if _check_ticker_period(db, "SPY", "D", target):
            return target                      # Mongo 已有，正常交易日
        bars = _fetch_polygon("SPY", "D", target, target)
        time.sleep(POLYGON_DELAY)
        if bars is None:
            _log(f"⚠️ 金絲雀 SPY 狀態未知（{target}），照常以此日為目標")
            return target                      # 額度問題不等於休市
        if bars:
            db.push_bars("SPY", "D", bars)     # 順手補上
            return target
        _log(f"ℹ️ 金絲雀判定 {target} 為休市日（SPY 200-空），回退一個交易日")
        target = _prev_trading_day(
            datetime.strptime(target, "%Y-%m-%d").date()).strftime("%Y-%m-%d")
    return None


# ─────────────────────────────────────────────
# 主流程
# ─────────────────────────────────────────────

def main() -> int:
    started_at = _now_est()
    start_mono = time.monotonic()

    try:
        db = VerifyDB()
    except Exception as e:
        _log(f"❌ 初始化 VerifyDB 失敗: {e}")
        return 1

    load_market_calendar(db.stock_db)

    # ── 輸入回聲塊 ──
    all_tickers = db.get_all_tickers()
    _log("=== 輸入回聲 ===")
    _log(f"  ticker 數: {len(all_tickers)} | periods: {VERIFY_PERIODS}")
    _log(f"  Mongo ping: {db.client.admin.command('ping')}")
    _log(f"  POLYGON_DELAY={POLYGON_DELAY}s | 熔斷: 樣本≥{BLOCK_MIN_SAMPLE} "
         f"且 blocked≥{BLOCK_RATIO:.0%} → 冷卻 {BLOCK_COOLDOWN}s")
    _log(f"  假日表來源: {'Mongo' if _HOLIDAY_CACHE['holidays'] else '內建兜底'} | "
         f"今日是否交易日: {_is_trading_day(_now_est().date())}")

    if not all_tickers:
        _log("❌ ticker 清單為空（Configs.full_set），中止")
        _notify("❌ [BC verify] ticker 清單為空，核查中止")
        db.write_task_log("NO_TICKERS", 0, 0, "Configs.full_set empty", started_at)
        return 1

    if not _acquire_lock(db):
        _log("⏸️ 搶鎖失敗，另一個 verify job 仍在合法運行中，本次跳過")
        return 0
    _log("🔒 搶鎖成功")

    last_error = ""
    fetched = filled = empty = blocked = skipped = 0

    try:
        target_date = _resolve_target_date(db)
        if not target_date:
            _log("❌ 金絲雀回退超限，找不到有效目標交易日")
            _notify("❌ [BC verify] 連續回退仍找不到有效交易日，請人工檢查")
            db.write_task_log("NO_TARGET_DATE", 0, 0, "canary walkback exceeded",
                              started_at)
            return 1

        _log(f"=== 核查開始 | 目標日: {target_date} ===")

        # ── 第一遍：Mongo 掃描，分揀待補抓清單 ──
        to_fetch = []
        for ticker in all_tickers:
            for period in VERIFY_PERIODS:
                v = db.get_verdict(ticker, period, target_date)
                if v in ("filled", "confirmed_empty"):
                    skipped += 1
                    continue
                if _check_ticker_period(db, ticker, period, target_date):
                    db.set_verdict(ticker, period, target_date,
                                   "filled", "mongo_scan")
                    filled += 1
                else:
                    to_fetch.append((ticker, period))

        total_points = len(all_tickers) * len(VERIFY_PERIODS)
        _log(f"  掃描完成：{total_points} 檢查點 | Mongo 已達標 {filled} | "
             f"歷史 verdict 跳過 {skipped} | 待補抓 {len(to_fetch)}")

        # ── 第二遍：序列補抓 ──
        window_processed = 0
        window_blocked   = 0
        for i, (ticker, period) in enumerate(to_fetch):
            if time.monotonic() - start_mono >= MAX_JOB_SECONDS:
                _log("⏱️ 逼近時限，提前結束本輪補抓")
                break

            # v12.35 修復：W period 用單日窗口查詢是錯的，會抓到/寫入錯誤的舊週線資料
            if period == "W":
                fetch_start = (
                    datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=10)
                ).strftime("%Y-%m-%d")
            else:
                fetch_start = target_date
            bars = _fetch_polygon(ticker, period, fetch_start, target_date)
            if bars:
                result = db.push_bars(ticker, period, bars)
                db.set_verdict(ticker, period, target_date,
                               "filled", f"polygon_{result}")
                filled  += 1
                fetched += 1
            elif bars is None:
                db.set_verdict(ticker, period, target_date,
                               "blocked", "polygon_unknown")
                blocked        += 1
                window_blocked += 1
            else:
                db.set_verdict(ticker, period, target_date,
                               "confirmed_empty", "polygon_200_empty")
                empty += 1

            window_processed += 1
            if (i + 1) % 20 == 0:
                _log(f"  進度 {i+1}/{len(to_fetch)} | "
                     f"補抓成功 {fetched} | 確認空 {empty} | blocked {blocked}")

            time.sleep(POLYGON_DELAY)

            # ── 熔斷：改為冷卻等待而非中止（GHA 時間便宜）──
            if (window_processed >= BLOCK_MIN_SAMPLE
                    and window_blocked / window_processed >= BLOCK_RATIO):
                _log(f"🚨 偵測系統性限速（{window_blocked}/{window_processed}），"
                     f"冷卻 {BLOCK_COOLDOWN}s 後繼續")
                time.sleep(BLOCK_COOLDOWN)
                window_processed = 0
                window_blocked   = 0
            if bars:
                result = db.push_bars(ticker, period, bars)
                db.set_verdict(ticker, period, target_date,
                               "filled", f"polygon_{result}")
                filled  += 1
                fetched += 1
            elif bars is None:
                db.set_verdict(ticker, period, target_date,
                               "blocked", "polygon_unknown")
                blocked        += 1
                window_blocked += 1
            else:
                db.set_verdict(ticker, period, target_date,
                               "confirmed_empty", "polygon_200_empty")
                empty += 1

            window_processed += 1
            if (i + 1) % 20 == 0:
                _log(f"  進度 {i+1}/{len(to_fetch)} | "
                     f"補抓成功 {fetched} | 確認空 {empty} | blocked {blocked}")

            time.sleep(POLYGON_DELAY)

            # ── 熔斷：改為冷卻等待而非中止（GHA 時間便宜）──
            if (window_processed >= BLOCK_MIN_SAMPLE
                    and window_blocked / window_processed >= BLOCK_RATIO):
                _log(f"🚨 偵測系統性限速（{window_blocked}/{window_processed}），"
                     f"冷卻 {BLOCK_COOLDOWN}s 後繼續")
                time.sleep(BLOCK_COOLDOWN)
                window_processed = 0
                window_blocked   = 0

        # ── 收尾 ──
        counts  = db.count_verdicts(target_date)
        elapsed = (time.monotonic() - start_mono) / 60
        status  = "DONE" if counts["blocked"] == 0 else "DONE_WITH_BLOCKED"
        summary = (
            f"🔍 [BC verify] {target_date} 核查完成\n"
            f"檢查點 {total_points} | 達標 {counts['filled']} | "
            f"確認空 {counts['confirmed_empty']} | blocked {counts['blocked']}\n"
            f"本次補抓 {fetched} | 耗時 {elapsed:.1f} 分鐘"
        )
        _log(summary.replace("\n", " | "))
        _notify(summary)
        db.write_task_log(status, counts["filled"] + counts["confirmed_empty"],
                          total_points, last_error, started_at)
        _dispatch_next_workflow()   # 乾淨完成才接力
        return 0

    except Exception as e:
        last_error = str(e)
        _log(f"❌ verify 執行異常: {e}")
        _notify(f"❌ [BC verify] 執行異常: {e}")
        db.write_task_log("EXCEPTION", filled + empty, 0, last_error, started_at)
        return 1

    finally:
        _release_lock(db)
        _log("🔓 鎖已釋放")


if __name__ == "__main__":
    sys.exit(main())
