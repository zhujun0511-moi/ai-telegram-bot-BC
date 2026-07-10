"""
run_backtest_daily.py — 每日回測 GitHub Actions 版 v1.1（取代 LC /webhook/daily）

觸發模式：拉（輪詢），不是推。設計依據：handoff_20260703_bc_buildout.md

梯次輪詢 + 截止告警 + 自動補課：
  GHA 排四班崗（22:00 / 23:30 / 01:00 / 03:00 EST 附近，cron 為 UTC 近似，
  一切時刻判斷腳本內用 EST 自算）。每班醒來：
    1. 讀 System_State（id="global"）找『最老的日期 D：
       after_hours_done_{D} 存在 且 backtest_done_{D} 不存在』
    2. 找到 → 對 D 跑回測（重置進度 + snapshot_date=D → 批次迴圈至 ALL_DONE
       → 統計分析）→ 設 backtest_done_{D} → 摘要一條 → 退出
    3. 找不到：
       - 若本班為末班（EST ≥ 02:30 且 < 09:00）且最近應完成交易日的鏈路仍未
         done → Telegram 截止告警（BC 是 DC 進程之外的第一個外部審計者）
       - 否則靜默秒退
  冪等：backtest_done_{D} 鍵，四班崗只有第一個等到的真跑。
  補課：條件是『最老欠課日期』，DC 遲到/故障多日均自動逐日補齊。
  一次 run 只處理一個日期，多個欠課由後續班次自然消化。

與 LC 版差異：
  - _task_lock / 自激發 / BUSY 429 → 全部刪除，GHA 單進程 while 迴圈跑到完
  - tasks/cfet_backtest.py 與 tasks/backtest_stats.py 原樣遷移，不混入重構
    （已知技術債隨遷不修：僅多頭 outcome、_get_trend 用 EMA 而非相位分類）
  - 每日重置 Backtest_Progress（completed=False, progress=0, snapshot_date=D），
    Backtest_Events 靠 event_exists 去重，重掃描實際上是增量

Python 3.9 兼容。
"""

import os
import sys
import time
import uuid
import re
from datetime import datetime, timedelta
from typing import Optional

import pytz
from pymongo.errors import DuplicateKeyError

from tasks.cfet_backtest import BacktestDB, run_backtest_batch
from tasks.backtest_stats import StatsDB, run_stats_analysis
from tasks.outbound import notify as _notify_shared
from tasks.outbound import dispatch_next_workflow as _dispatch_next_workflow_shared

EST_TZ = pytz.timezone("US/Eastern")

MAX_JOB_SECONDS    = 5.5 * 3600
LOCK_STALE_SECONDS = MAX_JOB_SECONDS + 15 * 60
BACKFILL_MAX_DAYS  = 14      # 只補最近 14 天的欠課，避免遠古鍵觸發巨量回填

TASK_NAME   = "bc_backtest_daily"
REPORT_TYPE = "bc_backtest"   # 非 cfet_alert，走標準頻道
LOCK_ID     = "bc_backtest_lock"

DONE_KEY_RE = re.compile(r"^after_hours_done_(\d{4}-\d{2}-\d{2})$")


def _now_est() -> datetime:
    return datetime.now(EST_TZ)


def _now_str() -> str:
    return _now_est().strftime("%Y-%m-%d %H:%M:%S EST")


def _log(msg: str):
    print(f"[{_now_str()}] {msg}", flush=True)


def _notify(msg: str):
    """2026-07-10改用 tasks/outbound.py 統一出口，report_type/行為不變（bc_backtest，失敗只print不重試）。"""
    _notify_shared(msg, report_type=REPORT_TYPE)


def _dispatch_next_workflow():
    """2026-07-10改用 tasks/outbound.py 統一出口，行為不變（讀NEXT_WORKFLOW env，同repo dispatch）。"""
    _dispatch_next_workflow_shared()


# ─────────────────────────────────────────────
# 日期工具
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
    """最近已完成交易日（16:00 翻轉語義，週末回退週五）"""
    if now_est is None:
        now_est = _now_est()
    today = now_est.date()
    if not _is_trading_day(today) or now_est.hour < 16:
        return _prev_trading_day(today).strftime("%Y-%m-%d")
    return today.strftime("%Y-%m-%d")


def _is_last_shift(now_est: datetime) -> bool:
    """末班崗：EST 02:30–09:00 之間醒來的班次負責截止告警"""
    minutes = now_est.hour * 60 + now_est.minute
    return 150 <= minutes < 540


# ─────────────────────────────────────────────
# System_State 讀寫（schema 與 DC 完全一致：id="global" 單文檔）
# ─────────────────────────────────────────────

def _get_global_state(db: BacktestDB) -> dict:
    doc = db.stock_db["System_State"].find_one({"id": "global"})
    return doc or {}


def _set_global_key(db: BacktestDB, key: str, value):
    db.stock_db["System_State"].update_one(
        {"id": "global"},
        {"$set": {key: value, "updated_at": _now_est()}},
        upsert=True,
    )


def _find_owed_date(state: dict) -> Optional[str]:
    """最老的『鏈路已完成但回測未完成』日期，僅看最近 BACKFILL_MAX_DAYS 天"""
    cutoff = (_now_est().date()
              - timedelta(days=BACKFILL_MAX_DAYS)).strftime("%Y-%m-%d")
    owed = []
    for key, value in state.items():
        m = DONE_KEY_RE.match(key)
        if not m or not value:
            continue
        d = m.group(1)
        if d < cutoff:
            continue
        if not state.get(f"backtest_done_{d}"):
            owed.append(d)
    return min(owed) if owed else None


# ─────────────────────────────────────────────
# MongoDB 層級鎖（克隆 run_phase_calc_gha v1.1/v1.2 已驗證模式）
# ─────────────────────────────────────────────

def _acquire_lock(db: BacktestDB) -> bool:
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


def _release_lock(db: BacktestDB):
    db.stock_db["System_State"].update_one(
        {"id": LOCK_ID}, {"$set": {"is_running": False}})


# ─────────────────────────────────────────────
# 回測執行（重置 + 批次迴圈 + 統計）
# ─────────────────────────────────────────────

def _reset_backtest_progress(db: BacktestDB, snapshot_date: str):
    """
    每日重置：completed=False、progress=0、snapshot_date=目標日。
    直接操作 Backtest_Progress collection，不改動模組本體
    （tasks/cfet_backtest.py 原樣遷移原則）。
    """
    db.stock_db["Backtest_Progress"].update_one(
        {"task_id": "cfet_backtest"},
        {"$set": {"task_id": "cfet_backtest",
                  "completed": False,
                  "progress": 0,
                  "snapshot_date": snapshot_date,
                  "updated_at": _now_est()}},
        upsert=True,
    )


def _run_backtest_for_date(db: BacktestDB, target_date: str,
                           start_mono: float) -> dict:
    _reset_backtest_progress(db, target_date)
    batch_count   = 0
    signals_total = 0
    while True:
        if time.monotonic() - start_mono >= MAX_JOB_SECONDS:
            return {"status": "TIMEOUT_GRACEFUL_STOP",
                    "batches": batch_count, "signals": signals_total}
        result = run_backtest_batch(db)
        status = result.get("status", "UNKNOWN")
        batch_count += 1
        signals_total += result.get("signals_found", 0)
        if status == "BATCH_DONE":
            continue
        return {"status": status,
                "batches": batch_count, "signals": signals_total}


def _write_task_log(db: BacktestDB, status: str, progress: int, total: int,
                    last_error: str, started_at: datetime):
    db.stock_db["Task_Log"].insert_one({
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
# 主流程
# ─────────────────────────────────────────────

def main() -> int:
    started_at = _now_est()
    start_mono = time.monotonic()

    try:
        db = BacktestDB()
    except Exception as e:
        _log(f"❌ 初始化 BacktestDB 失敗: {e}")
        return 1

    load_market_calendar(db.stock_db)

    # ── 輸入回聲塊 ──
    state       = _get_global_state(db)
    all_tickers = db.get_all_backtest_tickers()
    done_keys   = sorted(k for k in state if DONE_KEY_RE.match(k))
    _log("=== 輸入回聲 ===")
    _log(f"  回測標的數: {len(all_tickers)}")
    _log(f"  Mongo ping: {db.client.admin.command('ping')}")
    _log(f"  System_State 內 after_hours_done 鍵（近況）: {done_keys[-5:]}")
    _log(f"  補課視窗: 最近 {BACKFILL_MAX_DAYS} 天 | 末班判定: "
         f"{_is_last_shift(started_at)}")

    if not _find_owed_date(state):
        expected = get_completed_trading_date(started_at)
        chain_ok = bool(state.get(f"after_hours_done_{expected}"))
        if _is_last_shift(started_at) and not chain_ok:
            msg = (f"⚠️ [BC backtest] 末班檢查：DC 鏈路至 "
                   f"{started_at.strftime('%H:%M EST')} 仍未完成 {expected} "
                   f"的 after-hours（done 鍵不存在），今日回測缺席。"
                   f"欠課將於鏈路恢復後自動補跑。")
            _log(msg)
            _notify(msg)
        else:
            _log(f"✅ 無欠課日期（最近應完成日 {expected} "
                 f"done={chain_ok}），靜默退出")
        return 0

    if not _acquire_lock(db):
        _log("⏸️ 搶鎖失敗，另一個 backtest job 仍在合法運行中，本次跳過")
        return 0
    _log("🔒 搶鎖成功")

    last_error = ""
    processed  = []       # [(date, batches, signals)]
    timed_out  = False
    try:
        # ── v1.1：循環吃光全部欠課（由老到新）──
        while True:
            if time.monotonic() - start_mono >= MAX_JOB_SECONDS:
                timed_out = True
                break
            owed = _find_owed_date(_get_global_state(db))
            if not owed:
                break
            _log(f"📌 欠課日期: {owed}，開始回測 "
                 f"（已完成 {len(processed)} 個）")
            result = _run_backtest_for_date(db, owed, start_mono)
            status = result["status"]
            if status == "TIMEOUT_GRACEFUL_STOP":
                timed_out = True
                break
            if status not in ("ALL_DONE", "ALREADY_DONE"):
                raise RuntimeError(f"回測異常終態: {status} @ {owed}")
            _set_global_key(db, f"backtest_done_{owed}", True)
            processed.append((owed, result["batches"], result["signals"]))
            _log(f"✅ {owed} 完成 | 批次 {result['batches']} | "
                 f"新信號 {result['signals']}")

        if timed_out:
            _log("⏰ 5.5h 優雅逾時，進度已存檔，下一班崗續跑"
                 "（當前日期不設 done 鍵，不接力）")
            if processed:
                _notify(f"⏰ [BC backtest] 逾時暫停：本輪已完成 "
                        f"{len(processed)} 個日期"
                        f"（{processed[0][0]} ~ {processed[-1][0]}），"
                        f"餘量下班崗續跑")
            _write_task_log(db, "TIMEOUT_GRACEFUL_STOP", len(processed), 0,
                            "graceful timeout", started_at)
            return 0

        # ── 統計分析（循環結束後執行一次）──
        stats_result = run_stats_analysis(StatsDB())
        stats_status = stats_result.get("status", "UNKNOWN")

        elapsed = (time.monotonic() - start_mono) / 60
        dates_line = "、".join(p[0] for p in processed)
        summary = (
            f"📚 [BC backtest] 完成 {len(processed)} 個日期：{dates_line}\n"
            f"新信號合計 {sum(p[2] for p in processed)} | "
            f"統計 {stats_status}"
            f"（事件總數 {stats_result.get('total_events', 'N/A')}）\n"
            f"耗時 {elapsed:.1f} 分鐘"
        )
        _log(summary.replace("\n", " | "))
        _notify(summary)
        _write_task_log(db, "DONE", len(processed), len(processed),
                        last_error, started_at)
        _dispatch_next_workflow()   # 乾淨完成才接力
        return 0

    except Exception as e:
        last_error = str(e)
        _log(f"❌ backtest 執行異常: {e}")
        _notify(f"❌ [BC backtest] {owed} 執行異常: {e}")
        _write_task_log(db, "EXCEPTION", 0, 0, last_error, started_at)
        return 1

    finally:
        _release_lock(db)
        _log("🔓 鎖已釋放")


if __name__ == "__main__":
    sys.exit(main())
