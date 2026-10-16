"""
run_verify_weekend.py — V3 數據完整性核查 GitHub Actions 版 v1.1

取代 DC tasks/v3_processor.process_v3_verify_task 的多輪自激發設計。
設計依據：handoff_20260703_bc_buildout.md

v1.1 新增（2026-07-05）：
  Ticker 身份核對（Ticker_Identity）——防禦「舊ticker下市後被新公司重新
  註冊、Polygon Aggregates API 把兩段不相關歷史資料拼接」的資料污染問題
  （2026-07-04 SPCX 事故：SpaceX 借用已下市的 SPAC and New Issue ETF 舊代碼）。

  設計原則（低頻任務，只搭週六BC verify順風車）：
  - Ticker_Identity 只記錄 338 支核心ticker
  - list_date 只在該 ticker 第一次沒有記錄時才打 Polygon Reference API，
    一旦寫入永久快取，之後每週不再重查（list_date 不會變）
  - 每週核對本身完全不打 Polygon，純 MongoDB 內部比較「最舊 bar 日期」
    vs「list_date」，成本趨近於零
  - 只標記、寫入告警，不自動刪除任何資料——刪除需要人工核實
    （SPCX 這次是人工查證 SpaceX 實際上市日後才動手刪的，模式延續）

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

# 2026-10-16「唯一的門」：Mongo（VerifyDB）、Polygon、通知、dispatch、AC webhook 一律經 courier。
from courier import mongo as M
from courier import polygon as PG
from courier.mongo import DuplicateKeyError
from courier.verify_db import VerifyDB
from courier.outbound import notify as _notify_shared
from courier.outbound import dispatch_next_workflow as _dispatch_next_workflow_shared
from courier.outbound import trigger_ac_webhook as _trigger_ac_webhook_shared
from courier.keys import scrub  # noqa: F401

EST_TZ = pytz.timezone("US/Eastern")

# MONGO_URI／POLYGON_KEY 改在門內讀（courier/verify_db、courier/polygon；2026-10-16）

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

# 2026-07-25 新增：數據品質修正 pass 的乾跑開關。
# 預設 True＝只偵測+報告、不寫回（首輪安全）；核實報告後把 env VERIFY_REPAIR_DRY_RUN=false
# （或改此預設）即開啟實際寫回。範圍僅 D/W，H 留給第三階段批次清理。
REPAIR_DRY_RUN = os.getenv("VERIFY_REPAIR_DRY_RUN", "true").strip().lower() != "false"


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
    # 2026-07-04（獨立日）落在週六，NYSE 2026年沒有對應的補假全休市日，
    # 7/3只是半日提早收盤（非本文件範圍，本文件只管全休市）——2026-07-19
    # 查證NYSE官網修正此前誤把"2026-07-03"當全休市日的bug。
    "2026-09-07", "2026-11-26", "2026-12-25",
}
_HOLIDAY_CACHE = {"holidays": None}


def load_market_calendar(stock_db):
    """
    自 Configs {type:"market_calendar"} 載入假日表（與 AC timectx 同源）。
    失敗回落 FALLBACK_HOLIDAYS。main() 於 DB 初始化後、任何日期計算前呼叫。
    """
    try:
        doc = M.find_one("StockData", "Configs", {"type": "market_calendar"})
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
    """2026-07-10改用 tasks/outbound.py 統一出口，report_type/行為不變（bc_verify，失敗只print不重試）。"""
    _notify_shared(msg, report_type=REPORT_TYPE)


def _trigger_weekly_report(target_date: str):
    """2026-07-10改用 tasks/outbound.py 統一出口，行為不變（POST AC /webhook/weekly，WEBHOOK_SECRET header）。"""
    _trigger_ac_webhook_shared("/webhook/weekly", {"trading_date": target_date})


def _dispatch_next_workflow():
    """2026-07-10改用 tasks/outbound.py 統一出口，行為不變（讀NEXT_WORKFLOW env，同repo dispatch）。"""
    _dispatch_next_workflow_shared()


# ─────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────

# VerifyDB 已逐字搬到 courier/verify_db（2026-10-16「唯一的門」）；建立時傳入 BARS_LIMIT／TASK_NAME／_now_est。


# ─────────────────────────────────────────────
# MongoDB 層級鎖（克隆 run_phase_calc_gha v1.1/v1.2 已驗證模式）
# ─────────────────────────────────────────────

LOCK_ID = "bc_verify_lock"


def _acquire_lock(db: VerifyDB) -> bool:
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
        M.find_one_and_update("StockData", "System_State", filter_query, update_doc, upsert=True)
    except DuplicateKeyError:
        _log("⚠️ 搶鎖 upsert 撞 DuplicateKeyError（併發場景），upsert=False 重試")
        M.find_one_and_update("StockData", "System_State", filter_query, update_doc, upsert=False)
    doc = M.find_one("StockData", "System_State", {"id": LOCK_ID})
    return bool(doc) and doc.get("lock_token") == token


def _release_lock(db: VerifyDB):
    M.update_one("StockData", "System_State",
                 {"id": LOCK_ID}, {"$set": {"is_running": False}})


# ─────────────────────────────────────────────
# Polygon（語義克隆自 DC _fetch_polygon v12.32：[]=確認空 / None=狀態未知）
# ─────────────────────────────────────────────

# Polygon 五個函數已逐字搬到 courier/polygon（2026-10-16「唯一的門」，金鑰在門內讀）；這裡只轉接、帶入本檔的 _log。

def _fetch_polygon(ticker: str, period_label: str,
                   start_date: str, end_date: str):
    return PG.verify_fetch_polygon(ticker, period_label, start_date, end_date, _log)


def _fetch_polygon_reference(ticker: str) -> Optional[dict]:
    return PG.verify_fetch_reference(ticker, _log)


def _fetch_polygon_active_map() -> Optional[dict]:
    return PG.verify_fetch_active_map(_log)


def _fetch_polygon_inactive(ticker: str) -> Optional[dict]:
    return PG.verify_fetch_inactive(ticker, _log)


def _same_cik(a, b) -> bool:
    return str(a).lstrip("0") == str(b).lstrip("0")


def _refresh_identity(db, ticker: str, identity: Optional[dict], active_map: dict,
                      newly_delisted: list, cik_changed: list, ref_missing: list) -> Optional[dict]:
    """
    （2026-10-01 E）單支每週身份刷新。已標 delisted 的不再查。回傳刷新後的 identity。
      - 在 Polygon 現役名單 → 刷新 active/polygon_name/polygon_cik；cik 與既存不同 → identity_flag
        ＝cik_changed（只標記＋通知，不改 cik/list_date）；既存缺 cik → 補上
      - 不在現役名單 → 查 inactive：有 delisted_utc → 自動 delisted=true（用戶 2026-10-01 拍板）；
        查無 → 只記 ref_missing（不標記）；狀態未知 → 下週再查
    """
    if (identity or {}).get("delisted"):
        return identity
    tku = ticker.upper()
    a = active_map.get(tku)
    if a:
        upd = {"active": True, "polygon_name": a.get("name"),
               "polygon_cik": a.get("cik"), "identity_checked_at": _now_est()}
        old_cik = (identity or {}).get("cik")
        if old_cik and a.get("cik") and not _same_cik(old_cik, a["cik"]):
            upd["identity_flag"] = "cik_changed"
            cik_changed.append((tku, (identity or {}).get("company_name"), a.get("name")))
        elif not old_cik and a.get("cik"):
            upd["cik"] = a["cik"]
        db.update_ticker_identity(tku, upd)
        return identity
    info = _fetch_polygon_inactive(tku)
    time.sleep(POLYGON_DELAY)
    if info is None:
        return identity            # 狀態未知，下週再查
    if info.get("delisted_utc"):
        db.update_ticker_identity(tku, {
            "active": False, "delisted": True,
            "delisted_reason": "polygon_inactive",
            "delisted_utc": info["delisted_utc"],
            "delisted_at": _now_est(),
            "polygon_name": info.get("name"), "polygon_cik": info.get("cik"),
            "identity_checked_at": _now_est(),
        })
        newly_delisted.append((tku, info["delisted_utc"][:10]))
        return db.get_ticker_identity(tku)
    ref_missing.append(tku)        # 現役/下市都查無 → 只通知、不標記
    return identity


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
        _log(f"  ⚠️ _check_ticker_period 異常 ({ticker}/{period}): {scrub(e)}")
        return False


# ─────────────────────────────────────────────
# 數據品質修正（2026-07-25 新增，DC 止血之外的安全網）
# ─────────────────────────────────────────────

def _normalize_dedup_bars(bars: List[dict], period: str):
    """
    純函式，無 I/O。只動 t 字串與重複筆，不改任何 OHLCV 數值。
      1. 正規化時間戳：'T' → 空格；D/W 一律「日期 00:00:00」
         （消 isoformat 'T' vs str 空格 的格式不統一）。
      2. 同交易日(D)/同週(W)去重，保留權威筆：原始時刻 00:00:00（/range）
         優先於 16:00:00（/prev 暫定，值可能是髒的，如 AMZN 低點/HBAN 量能）。
    回傳 (new_bars 新在前, n_removed 去重掉幾筆, n_reformatted 幾筆 t 被改寫)。
    """
    if not bars:
        return bars, 0, 0

    def _norm_t(t) -> str:
        s = str(t).replace("T", " ")
        # D/W 都以「日期 00:00:00」為準（W bar 本來就是週一 00:00）
        return s[:10] + " 00:00:00" if period in ("D", "W") else s

    def _orig_time(t) -> str:
        s = str(t).replace("T", " ")
        return s[11:19] if len(s) >= 19 else ""

    best = {}  # norm_t -> bar（原樣，t 稍後才改寫）
    for b in bars:
        nt = _norm_t(b.get("t", ""))
        cur = best.get(nt)
        if cur is None:
            best[nt] = b
        elif _orig_time(b.get("t", "")) == "00:00:00" and _orig_time(cur.get("t", "")) != "00:00:00":
            best[nt] = b  # 偏好 /range 權威筆（00:00），取代 /prev 暫定筆（16:00）
        # 其餘（同 00:00、或格式差異但值相同）：保留先出現的（新在前，較新）

    n_removed = len(bars) - len(best)
    n_reformatted = 0
    out = []
    for nt, b in best.items():
        nb = dict(b)
        if nb.get("t") != nt:
            nb["t"] = nt
            n_reformatted += 1
        out.append(nb)
    out.sort(key=lambda x: x["t"], reverse=True)
    return out, n_removed, n_reformatted


def _repair_bars_quality(db: "VerifyDB", all_tickers: List[str], start_mono: float) -> dict:
    """
    掃核心票 × [D,W]，用 _normalize_dedup_bars 偵測同交易日重複 / T-vs-空格格式。
    REPAIR_DRY_RUN=True 只統計不寫回；False 才 replace_bars 實際寫回。
    H 不在此範圍（第三階段）。受 MAX_JOB_SECONDS 約束、per-ticker try/except。
    """
    stat = {"scanned": 0, "dup_tickers": 0, "dups": 0,
            "fmt_tickers": 0, "repaired": 0, "dry_run": REPAIR_DRY_RUN}
    sample = []
    for ticker in all_tickers:
        if time.monotonic() - start_mono >= MAX_JOB_SECONDS:
            _log("⏱️ 逼近時限，數據品質修正提前結束")
            break
        for period in ("D", "W"):
            try:
                bars = db.get_bars(ticker, period)
                stat["scanned"] += 1
                if not bars:
                    continue
                new_bars, n_removed, n_fmt = _normalize_dedup_bars(bars, period)
                if n_removed == 0 and n_fmt == 0:
                    continue
                if n_removed > 0:
                    stat["dup_tickers"] += 1
                    stat["dups"] += n_removed
                    if len(sample) < 10:
                        sample.append(f"{ticker}/{period}(-{n_removed})")
                if n_fmt > 0:
                    stat["fmt_tickers"] += 1
                if not REPAIR_DRY_RUN:
                    db.replace_bars(ticker, period, new_bars)
                    stat["repaired"] += 1
            except Exception as e:
                _log(f"  ⚠️ 數據品質修正異常 ({ticker}/{period}): {e}")
    stat["sample"] = sample
    return stat


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
        db = VerifyDB(BARS_LIMIT, TASK_NAME, _now_est)
    except Exception as e:
        _log(f"❌ 初始化 VerifyDB 失敗: {e}")
        return 1

    load_market_calendar(None)   # 2026-10-16：查詢經 courier.mongo，不再傳資料庫物件

    # ── 輸入回聲塊 ──
    all_tickers = db.get_all_tickers()
    _log("=== 輸入回聲 ===")
    _log(f"  ticker 數: {len(all_tickers)} | periods: {VERIFY_PERIODS}")
    _log(f"  Mongo ping: {M.ping()}")
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

        # ── 數據品質修正 pass（2026-07-25 新增，同交易日重複 / T-vs-空格格式）──
        _log("=== 數據品質修正開始 ===")
        quality = _repair_bars_quality(db, all_tickers, start_mono)
        _mode = "偵測(dry-run，未寫回)" if quality["dry_run"] else "已修正寫回"
        _log(f"  數據品質[{_mode}] | 掃描 {quality['scanned']} | "
             f"含重複 {quality['dup_tickers']}支/{quality['dups']}筆 | "
             f"含格式問題 {quality['fmt_tickers']}支 | 實際寫回 {quality['repaired']}"
             + (f" | 樣本 {', '.join(quality['sample'])}" if quality.get("sample") else ""))

        # ── Ticker 身份核對（Ticker_Identity，v1.1 新增，週六BC專屬低頻任務）──
        _log("=== Ticker 身份核對開始 ===")
        identity_fetched = 0
        identity_checked = 0
        suspected_reuse  = []
        # 2026-10-01 E：每週身份刷新（原設計「查一次永久快取」抓不到下市/代號換人——
        # AVB/EQR 08-18 下市掛了六週沒人發現）。用戶拍板：Polygon 明確 active=false＋delisted_utc
        # → 自動 delisted=true＋通知；cik 變更只標 identity_flag＋通知（需人工）。
        newly_delisted, cik_changed, ref_missing = [], [], []
        active_map = _fetch_polygon_active_map()
        if active_map is None:
            _log("  ⚠️ Polygon 現役列表取得失敗，本週跳過身份刷新（不做下市判定）")
        else:
            _log(f"  Polygon 現役代號 {len(active_map)} 支")

        for ticker in all_tickers:
            identity = db.get_ticker_identity(ticker)

            if active_map is not None:
                identity = _refresh_identity(db, ticker, identity, active_map,
                                             newly_delisted, cik_changed, ref_missing)

            # 沒記錄過，才打一次 Polygon（之後永久快取，不重查）
            if identity is None:
                info = _fetch_polygon_reference(ticker)
                time.sleep(POLYGON_DELAY)
                if info is None:
                    continue  # 狀態未知，下週再試
                db.set_ticker_identity(
                    ticker,
                    info.get("list_date"),
                    info.get("cik"),
                    info.get("company_name"),
                )
                identity_fetched += 1
                identity = db.get_ticker_identity(ticker)

            list_date = identity.get("list_date")
            if not list_date:
                continue  # 這支查無上市日資料，跳過核對

            identity_checked += 1
            for period in VERIFY_PERIODS:
                oldest = db.get_oldest_bar_date(ticker, period)
                if oldest and oldest < list_date:
                    suspected_reuse.append((ticker, period, oldest, list_date))

        _log(f"  身份核對完成 | 新登記 {identity_fetched} | 已核對 {identity_checked} | "
             f"疑似ticker重複使用 {len(suspected_reuse)} | 新下市 {len(newly_delisted)} | "
             f"cik變更 {len(cik_changed)} | 查無 {len(ref_missing)}")

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
        _qmode = "偵測(dry-run)" if quality["dry_run"] else "已修正"
        summary += (
            f"\n🧹 數據品質[{_qmode}]：重複 {quality['dup_tickers']}支/"
            f"{quality['dups']}筆、格式 {quality['fmt_tickers']}支"
            + (f"、寫回 {quality['repaired']}支" if not quality["dry_run"] else "")
        )
        if suspected_reuse:
            reuse_list = ", ".join(f"{t}/{p}" for t, p, _, _ in suspected_reuse[:10])
            more = f" 等共{len(suspected_reuse)}個" if len(suspected_reuse) > 10 else ""
            summary += f"\n⚠️ 疑似ticker重複使用: {reuse_list}{more}（需人工核實，未自動處理）"
        # 2026-10-01 E：身份刷新結果
        if active_map is None:
            summary += "\n⚠️ 本週身份刷新未執行（Polygon 現役列表取得失敗）"
        if newly_delisted:
            summary += ("\n🪦 Polygon 確認下市（已自動標記 delisted，DC/CFET 將排除；請從清單移除）: "
                        + ", ".join(f"{t}({d})" for t, d in newly_delisted))
        if cik_changed:
            summary += ("\n🔀 代號身份變更（cik 不同，需人工確認，未自動處理）: "
                        + "; ".join(f"{t}: {o or '?'} → {n or '?'}" for t, o, n in cik_changed[:10]))
        if ref_missing:
            summary += ("\n❓ Polygon 現役/下市皆查無（未標記）: " + ", ".join(ref_missing[:20])
                        + (f" 等共{len(ref_missing)}支" if len(ref_missing) > 20 else ""))
        _log(summary.replace("\n", " | "))
        _notify(summary)
        db.write_task_log(status, counts["filled"] + counts["confirmed_empty"],
                          total_points, last_error, started_at)
        _trigger_weekly_report(target_date)   # 核查完成後觸發週報（此前缺失的一步）
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
