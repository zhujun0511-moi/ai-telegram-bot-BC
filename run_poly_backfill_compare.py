"""
run_poly_backfill_compare.py — BC job：Poly長尾backfill + MP資料稽核 v1.0（2026-07-17新增）

背景：MP（MarketParquet）長尾資料鏈自2026-07-06因資料品質疑慮全線停用（見
handoff_master.md「MP資料鏈路全線停止使用」）。本腳本目的是稽核：MP過去
抓到的日線資料，跟Polygon（免費版，2年歷史、正式資料源）交叉比對，差距
是否在合理範圍——若合理，可考慮重啟MP（免費、無總量上限，比Polygon
5次/分鐘快得多）；若差距大，證實MP資料確實有問題。

職責分工（兩階段，**各自獨立workflow，互相自鏈dispatch**，2026-07-17當天
第二次改版——原本設計是單一job內接力，實跑第一輪發現fetch會把整個job
預算吃光、compare完全分不到時間執行，改成現在這版）：
  Phase 1（抓取，`--phase fetch`，workflow `bc_poly_fetch.yml`）：讀 HF
    Dataset mp_data/ticker/ 現有票範圍 → 逐票抓 Polygon 2年日線（end_date
    主動設為「昨天」，不抓「今天」——免費版對「今天」的range一律
    NOT_AUTHORIZED，範圍設定本身就避開這道牆，不需事後fallback，見DC
    tasks/v3_processor.py._fetch_polygon()同款根因但不同因應策略：DC是
    小範圍增量、被擋了用/prev補一天划算；這裡是2年大範圍，被擋等於整批
    作廢，主動避開才是對的）→ 寫 mp_data/ticker/{ticker}/d-p.csv（新檔名，
    不覆寫原本d.csv，兩份並存）→ 結束後若還有compare backlog且未過16:00，
    dispatch `bc_poly_compare.yml`
  Phase 2（核對，`--phase compare`，workflow `bc_poly_compare.yml`）：找
    「d.csv 和 d-p.csv 都有、但還沒 compare_report.json」的票 → 比對OHLC
    數值差距（精確%，約略對照用）+ volume只比數量級（見下）+ 精確日期
    缺口（MP有Poly沒有/反過來）→ 寫每票的 compare_report.json → 全部核對
    完後重建全域彙總 mp_poly_compare_summary.json（按差距大小排序）→
    結束後若還有fetch backlog且未過16:00，dispatch `bc_poly_fetch.yml`

  ⚠️ volume比對規則（用戶2026-07-17拍板）：只判斷數量級是否一致（log10比值差
  <1，即10倍以內算一致），不算精確百分比差距——不同資料源volume統計口徑
  本來就不同（成交量計算方式、是否含盤前盤後等），算太精確反而製造假警報、
  誤導「這支票有問題」的判斷。OHLC（開高低收）維持精確%比對，因為那才是
  真正該一致的數字。

停止條件（用戶2026-07-17當天二次拍板，取代原本「單一job兩階段共用一個
截止點」的設計）：
  fetch自己的預算固定FETCH_BUDGET_SECONDS（5小時）：滿了就強制中斷，
    不管還有沒有剩票，交棒給compare（原本的bug：fetch跟compare共用同一個
    截止點，fetch把時間全吃光，compare這階段實測跑出「本輪0支」）。
  compare自己的預算COMPARE_BUDGET_SECONDS（30分鐘，2026-07-17當天實測
    compare從未真正執行過、無歷史耗時數據，用「無節流+小檔案下載」的
    合理估計+50%安全邊界訂出來的暫定值，第一輪真跑完後要回頭核對Mongo
    poly_compare_progress的實際耗時再調準）。
  兩者都同時受 wall-clock DAY_CUTOFF_HOUR（16:00 EST，收盤=盤後鏈接手的
  時間點，之後絕不能還在跑Polygon抓取，見用戶2026-07-16拍板「盤後禁止」
  規則）節制，且是「保底預留」不是「算完才發現剩多少」：可用總時間
  = min(自己的budget, 到16:00剩的時間)，這樣就算workflow是接近16:00才被
  上一輪dispatch起來，也不會因為wall-clock比budget先到就被硬吃光。
  斷點續傳：靠HF Dataset檔案存不存在（d-p.csv/compare_report.json）+
  Mongo System_State進度心跳（poly_backfill_progress/poly_compare_progress，
  每處理20支更新一次updated_at，比照DC v3_processor.py的set_verify_round
  心跳模式，避免長輪次被誤判孤兒、也讓外部能查「還在動嗎」）。

觸發：bc_refresh_models.yml（唯一03:00 EST cron）在dispatch bc_backtest_daily
之餘，平行額外dispatch `bc_poly_fetch.yml`（每天鏈路的起點）。之後fetch/
compare兩個workflow互相自鏈dispatch對方（同DC after_hours自激發模式的
精神，但這裡是GitHub Actions workflow_dispatch跨job版本，不是同一進程內
背景執行緒），一路交替到wall-clock過16:00、其中一邊在自己開頭的cutoff
檢查就直接return不再繼續鏈下去為止。這樣設計的好處（用戶2026-07-17拍板）：
①fetch跟compare是完全獨立的job，其中一邊卡死不會連帶讓另一邊永遠跑不到；
②每次workflow_dispatch之間天然有GitHub排程/佇列延遲當緩衝，對GitHub Actions
本身的不準時有容錯空間；③`gh run list`能清楚看到fetch/compare各自獨立的
執行紀錄，比在單一job log裡找階段切換點好追蹤。

回饋機制（用戶2026-07-16拍板「長時間工作要有充足回饋」，四層皆抄既有慣例）：
  1. Mongo心跳（同上，每20支）
  2. 熔斷（僅fetch階段）：連續高比例失敗（CIRCUIT_MIN_SAMPLE起算，比例達
     CIRCUIT_FAIL_RATIO）→ 提前中止本階段 + Telegram告警，比照DC
     VERIFY_BLOCK_MIN_SAMPLE/RATIO
  3. 自癒鎖：fetch/compare各自獨立的鎖（poly_fetch_lock/poly_compare_lock，
     staleness各自對應自己的budget+緩衝），比照BC run_backtest_daily.py/
     BC.p db.py的鎖模式，真的卡死（如GHA平台強制砍job）不會永久卡住後續
     觸發，也不會因為fetch/compare共用一把鎖而互相誤判對方是孤兒
  4. 起訖Telegram摘要：tasks/outbound.py統一出口，report_type="poly_backfill"

HF Dataset寫入比照 run_mp_reorganize_daily.py 慣例：本地暫存 ./output/ →
每處理 UPLOAD_BATCH_SIZE 支批次 upload_folder 一次（不逐票單獨upload_file，
避免頻繁commit/可能的HF端節流），deadline到時強制flush最後一批，不遺漏
已處理完但還沒上傳的成果。

環境變量（GitHub Actions Secrets，皆為BC repo既有secret，未新增）：
  MONGO_URI, POLYGON_KEY, HF_TOKEN, HF_REPO_ID, COMM_HUB_URL, WEBHOOK_SECRET

完整設計/動機/拍板記錄見 handoff_master.md「Poly長尾backfill+MP稽核」章節、
MongoDB_Standard_v3.md StockData.System_State 2026-07-16/17新增段。

Python 3.9 相容。
"""

import argparse
import io
import json
import math
import os
import shutil
import sys
import time
import uuid
from datetime import datetime, timedelta

import pandas as pd
import pymongo
import pytz
import requests
from huggingface_hub import HfApi
from pymongo.errors import DuplicateKeyError

from tasks.outbound import notify as _notify_shared, dispatch_workflow

FETCH_WORKFLOW_FILE   = "bc_poly_fetch.yml"
COMPARE_WORKFLOW_FILE = "bc_poly_compare.yml"

EST_TZ = pytz.timezone("US/Eastern")

MONGO_URI    = os.getenv("MONGO_URI", "").strip()
POLYGON_KEY  = os.getenv("POLYGON_KEY", "").strip()
HF_TOKEN_ENV = os.getenv("HF_TOKEN", "").strip()
HF_REPO_ID   = os.getenv("HF_REPO_ID", "zhujun0511-AI/ai-telegram-bot-dataset").strip()

HF_TICKER_DIR = "mp_data/ticker"
OUTPUT_DIR    = "./output"

POLYGON_DELAY = 12.5   # 秒，5次/分鐘節流（同DC config.py慣例）

# fetch/compare各自獨立的時間預算（用戶2026-07-17當天二次拍板，取代原本
# 「單一job兩階段共用一個截止點」的設計——那個設計實跑第一輪就出包：fetch
# 把整個5h預算吃光，compare階段實測「本輪0支」，完全沒機會執行）。
FETCH_BUDGET_SECONDS   = 5 * 3600      # fetch滿5h強制中斷，不管還有沒有剩票
COMPARE_BUDGET_SECONDS = 30 * 60       # compare的暫定安全網，2026-07-17當天
                                        # compare從未真正執行過、無歷史耗時
                                        # 數據，用「無節流+小檔案下載」推理
                                        # +50%安全邊界訂出來，第一輪真跑完
                                        # 後要回頭核對Mongo poly_compare_
                                        # progress的實際耗時再調準

DAY_CUTOFF_HOUR = 16   # EST 16:00 wall-clock 硬停（fetch/compare都受這個節制）

HEARTBEAT_EVERY     = 20
UPLOAD_BATCH_SIZE   = 50
CIRCUIT_MIN_SAMPLE  = 20
CIRCUIT_FAIL_RATIO  = 0.5

# fetch/compare各自獨立的鎖（用戶2026-07-17拍板：不要共用一把鎖，否則其中
# 一邊卡死會讓另一邊也被誤判成孤兒、或搶不到鎖），staleness各自對應自己
# 的budget留緩衝
FETCH_LOCK_ID             = "poly_fetch_lock"
FETCH_LOCK_STALE_SECONDS  = FETCH_BUDGET_SECONDS + 1.5 * 3600     # 6.5小時
COMPARE_LOCK_ID           = "poly_compare_lock"
COMPARE_LOCK_STALE_SECONDS = COMPARE_BUDGET_SECONDS + 1 * 3600    # 1.5小時

FETCH_PROGRESS_ID   = "poly_backfill_progress"
COMPARE_PROGRESS_ID = "poly_compare_progress"

REPORT_TYPE = "poly_backfill"

OHLC_COLS  = ["open", "high", "low", "close"]
OHLCV_COLS = OHLC_COLS + ["volume"]

VOLUME_MAGNITUDE_LOG_TOL = 1.0   # log10比值差 < 1 視為同數量級（10倍以內）


def _now_est() -> datetime:
    return datetime.now(EST_TZ)


def _log(msg: str):
    print(f"[{_now_est().strftime('%Y-%m-%d %H:%M:%S EST')}] {msg}", flush=True)


def _notify(msg: str):
    _notify_shared(msg, report_type=REPORT_TYPE)


# ─────────────────────────────────────────────
# Mongo：鎖 + 進度心跳（比照 run_backtest_daily.py / BC.p db.py 同款模式）
# ─────────────────────────────────────────────

class PolyDB:
    def __init__(self):
        if not MONGO_URI:
            raise RuntimeError("MONGO_URI 未設定")
        self.client = pymongo.MongoClient(MONGO_URI)
        self.stock_db = self.client["StockData"]
        self._lock_token = None

    def acquire_lock(self, lock_id: str, stale_seconds: float) -> bool:
        col = self.stock_db["System_State"]
        now = _now_est()
        stale = now - timedelta(seconds=stale_seconds)
        token = uuid.uuid4().hex
        filter_query = {
            "id": lock_id,
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
            return False
        self._lock_token = token
        doc = col.find_one({"id": lock_id})
        return bool(doc) and doc.get("lock_token") == token

    def release_lock(self, lock_id: str):
        # 只釋放自己持有的鎖（token比對），避免誤放被下一輪接管的孤兒鎖
        self.stock_db["System_State"].update_one(
            {"id": lock_id, "lock_token": self._lock_token},
            {"$set": {"is_running": False}},
        )

    def set_progress(self, progress_id: str, last_ticker, progress: int):
        self.stock_db["System_State"].update_one(
            {"id": progress_id},
            {"$set": {"last_ticker": last_ticker, "progress": progress,
                     "updated_at": _now_est()}},
            upsert=True,
        )


# ─────────────────────────────────────────────
# HF Dataset：範圍/存在性查詢 + 本地暫存批次上傳
# ─────────────────────────────────────────────

def _hf_api() -> HfApi:
    return HfApi(token=HF_TOKEN_ENV)


def _list_tickers_with_file(api: HfApi, filename: str) -> set:
    """
    列 mp_data/ticker/*/{filename} 底下有這個檔案的票。
    filename="d.csv" → 範圍真相來源（哪些票MP有資料）；
    filename="d-p.csv" → 已抓過Polygon的票；
    filename="compare_report.json" → 已核對過的票。
    """
    files = api.list_repo_files(repo_id=HF_REPO_ID, repo_type="dataset")
    prefix = HF_TICKER_DIR + "/"
    suffix = "/" + filename
    tickers = set()
    for f in files:
        if f.startswith(prefix) and f.endswith(suffix):
            ticker = f[len(prefix):-len(suffix)]
            # 排除含底線的偽ticker目錄（如 _MONTH 後綴，見DANGER_ZONES_master.md
            # 「HF Dataset mp_data/ticker/ 混有 _MONTH 偽ticker目錄」章節）
            if ticker and "/" not in ticker and "_" not in ticker:
                tickers.add(ticker)
    return tickers


def _has_fetch_backlog(api: HfApi) -> bool:
    """source(d.csv) - done(d-p.csv) 是否還有剩，決定compare結束後要不要dispatch fetch。"""
    source = _list_tickers_with_file(api, "d.csv")
    done   = _list_tickers_with_file(api, "d-p.csv")
    return bool(source - done)


def _has_compare_backlog(api: HfApi) -> bool:
    """fetched(d-p.csv) - compared(compare_report.json) 是否還有剩，決定fetch結束後要不要dispatch compare。"""
    fetched  = _list_tickers_with_file(api, "d-p.csv")
    compared = _list_tickers_with_file(api, "compare_report.json")
    return bool(fetched - compared)


def _download_csv(api: HfApi, ticker: str, filename: str):
    """下載單一ticker的CSV到記憶體DataFrame，不存在/失敗回None。"""
    try:
        path = api.hf_hub_download(
            repo_id=HF_REPO_ID, repo_type="dataset",
            filename=f"{HF_TICKER_DIR}/{ticker}/{filename}",
        )
        df = pd.read_csv(path, dtype=str)
        df.columns = [c.lower() for c in df.columns]
        return df
    except Exception:
        return None


def _stage_write(ticker: str, filename: str, content: bytes):
    """寫進本地暫存 ./output/mp_data/ticker/{ticker}/{filename}，供批次upload_folder。"""
    dir_path = os.path.join(OUTPUT_DIR, HF_TICKER_DIR, ticker)
    os.makedirs(dir_path, exist_ok=True)
    with open(os.path.join(dir_path, filename), "wb") as f:
        f.write(content)


def _flush_upload(api: HfApi, commit_message: str) -> bool:
    """把本地暫存整個 upload_folder 推到 HF Dataset，成功後清空暫存。"""
    if not os.path.isdir(OUTPUT_DIR) or not os.listdir(OUTPUT_DIR):
        return True
    try:
        api.upload_folder(
            folder_path=OUTPUT_DIR,
            repo_id=HF_REPO_ID,
            repo_type="dataset",
            commit_message=commit_message,
        )
        shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
        return True
    except Exception as e:
        _log(f"❌ upload_folder 失敗: {e}（暫存保留，下次一併重試）")
        return False


def _stage_root_write(filename: str, content: bytes):
    """寫進本地暫存的 repo 根目錄檔案（如全域彙總）。"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, filename), "wb") as f:
        f.write(content)


# ─────────────────────────────────────────────
# Polygon 抓取
# ─────────────────────────────────────────────

def _fetch_polygon_daily(ticker: str, start_date: str, end_date: str):
    """
    抓 Polygon 日線 range（2年→昨天）。回傳 list（可能為空）或 None（失敗/異常）。
    end_date 由呼叫端主動設為「昨天」，不主動嘗試「今天」——backfill是2年大範圍，
    免費版對「今天」NOT_AUTHORIZED會讓整個range請求失敗，主動避開比事後fallback划算
    （DC小範圍增量用/prev補一天的策略在這裡不適用，見檔頭docstring）。
    """
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
        f"{start_date}/{end_date}"
        f"?adjusted=true&extended_hours=false&sort=asc&limit=50000&apiKey={POLYGON_KEY}"
    )
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            _log(f"  ⚠️ [{ticker}] Polygon回應 {resp.status_code}: {resp.text[:150]}")
            return None
        body = resp.json()
        if body.get("status") not in ("OK", "DELAYED"):
            _log(f"  ⚠️ [{ticker}] Polygon status={body.get('status')}: {str(body.get('message',''))[:100]}")
            return None
        return body.get("results", []) or []
    except Exception as e:
        _log(f"  ❌ [{ticker}] Polygon呼叫異常: {e}")
        return None


def _results_to_csv_bytes(results: list) -> bytes:
    """Polygon aggregates results → d-p.csv bytes，欄位對齊既有 d.csv schema（date,open,high,low,close,volume）。"""
    rows = []
    for r in results:
        date_str = datetime.fromtimestamp(r["t"] / 1000, tz=EST_TZ).strftime("%Y-%m-%d")
        rows.append({
            "date": date_str,
            "open": r.get("o"), "high": r.get("h"),
            "low": r.get("l"), "close": r.get("c"), "volume": r.get("v"),
        })
    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
    df = df.sort_values("date").drop_duplicates("date")
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


# ─────────────────────────────────────────────
# Phase 1：抓取
# ─────────────────────────────────────────────

def run_fetch_phase(db: "PolyDB", api: HfApi, deadline_mono: float) -> dict:
    yesterday = (_now_est().date() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_2y  = (_now_est().date() - timedelta(days=730)).strftime("%Y-%m-%d")

    source = _list_tickers_with_file(api, "d.csv")
    done   = _list_tickers_with_file(api, "d-p.csv")
    remaining = sorted(source - done)

    _log(f"🔍 [fetch] 來源{len(source)}支，已完成{len(done)}支，待抓{len(remaining)}支 "
         f"| 範圍 {start_2y}→{yesterday}")

    processed = failed = since_upload = 0
    last_ticker = None

    for ticker in remaining:
        if time.monotonic() >= deadline_mono:
            _log(f"⏸️ [fetch] 時間到，本輪處理 {processed} 支後暫停（待抓清單原{len(remaining)}支）")
            break

        results = _fetch_polygon_daily(ticker, start_2y, yesterday)
        if results is None:
            failed += 1
        else:
            _stage_write(ticker, "d-p.csv", _results_to_csv_bytes(results))
            _log(f"  ✅ [fetch] {ticker}: {len(results)} 根")

        processed += 1
        since_upload += 1
        last_ticker = ticker
        time.sleep(POLYGON_DELAY)

        if processed % HEARTBEAT_EVERY == 0:
            db.set_progress(FETCH_PROGRESS_ID, last_ticker, processed)

        if since_upload >= UPLOAD_BATCH_SIZE:
            _flush_upload(api, f"poly backfill batch (upto {ticker})")
            since_upload = 0

        if processed >= CIRCUIT_MIN_SAMPLE and failed / processed >= CIRCUIT_FAIL_RATIO:
            msg = (f"🚨 [poly-backfill] fetch階段偵測到系統性失敗（{failed}/{processed}），"
                   f"提前中止本輪")
            _log(msg)
            _notify(msg)
            break

    _flush_upload(api, f"poly backfill final flush (upto {last_ticker})")
    db.set_progress(FETCH_PROGRESS_ID, last_ticker, processed)
    return {"processed": processed, "failed": failed, "remaining_before": len(remaining)}


# ─────────────────────────────────────────────
# Phase 2：核對
# ─────────────────────────────────────────────

def _same_order_of_magnitude(a, b) -> bool:
    """
    數量級是否一致：log10比值差 < VOLUME_MAGNITUDE_LOG_TOL（預設10倍以內）。
    兩邊都<=0（理論上volume不該出現，防禦用）視為一致；只有一邊<=0視為不一致。
    """
    try:
        a, b = float(a), float(b)
    except Exception:
        return False
    if a <= 0 and b <= 0:
        return True
    if a <= 0 or b <= 0:
        return False
    return abs(math.log10(a) - math.log10(b)) < VOLUME_MAGNITUDE_LOG_TOL


def _compare_ohlcv(mp_df: pd.DataFrame, poly_df: pd.DataFrame) -> dict:
    mp_df = mp_df.copy()
    poly_df = poly_df.copy()
    for col in OHLCV_COLS:
        if col in mp_df.columns:
            mp_df[col] = pd.to_numeric(mp_df[col], errors="coerce")
        if col in poly_df.columns:
            poly_df[col] = pd.to_numeric(poly_df[col], errors="coerce")

    mp_dates   = set(mp_df["date"]) if "date" in mp_df.columns else set()
    poly_dates = set(poly_df["date"]) if "date" in poly_df.columns else set()
    overlap    = sorted(mp_dates & poly_dates)

    value_diff = {}
    if overlap:
        merged = mp_df.set_index("date").loc[overlap].join(
            poly_df.set_index("date").loc[overlap], lsuffix="_mp", rsuffix="_poly"
        )

        # OHLC：精確%差距（開高低收本該精確一致，這裡的數字才有判斷意義）
        for col in OHLC_COLS:
            mp_col, poly_col = f"{col}_mp", f"{col}_poly"
            if mp_col not in merged.columns or poly_col not in merged.columns:
                continue
            denom = merged[mp_col].replace(0, pd.NA)
            pct = ((merged[poly_col] - merged[mp_col]).abs() / denom).dropna()
            value_diff[col] = {
                "avg_pct": round(float(pct.mean()) * 100, 4) if len(pct) else None,
                "max_pct": round(float(pct.max()) * 100, 4) if len(pct) else None,
            }

        # volume：只比數量級（用戶2026-07-17拍板，不同資料源統計口徑本來就不同，
        # 算精確%差距只會製造假警報，見檔頭docstring）
        mp_col, poly_col = "volume_mp", "volume_poly"
        if mp_col in merged.columns and poly_col in merged.columns:
            pair = merged[[mp_col, poly_col]].dropna()
            total = len(pair)
            same = sum(
                _same_order_of_magnitude(row[mp_col], row[poly_col])
                for _, row in pair.iterrows()
            )
            value_diff["volume"] = {
                "same_magnitude_pct": round(same / total * 100, 2) if total else None,
                "checked_days": total,
                "note": "僅比對數量級是否一致（10倍以內），不計精確百分比差距",
            }

    return {
        "overlap_days": len(overlap),
        "value_diff": value_diff,
        "missing_in_poly": sorted(mp_dates - poly_dates),
        "missing_in_mp":   sorted(poly_dates - mp_dates),
    }


def run_compare_phase(db: "PolyDB", api: HfApi, deadline_mono: float) -> dict:
    fetched  = _list_tickers_with_file(api, "d-p.csv")
    compared = _list_tickers_with_file(api, "compare_report.json")
    remaining = sorted(fetched - compared)

    _log(f"🔍 [compare] 已抓{len(fetched)}支，已核對{len(compared)}支，待核對{len(remaining)}支")

    processed = since_upload = 0
    last_ticker = None

    for ticker in remaining:
        if time.monotonic() >= deadline_mono:
            _log(f"⏸️ [compare] 時間到，本輪處理 {processed} 支後暫停")
            break

        mp_df   = _download_csv(api, ticker, "d.csv")
        poly_df = _download_csv(api, ticker, "d-p.csv")
        processed += 1
        since_upload += 1
        last_ticker = ticker

        if mp_df is None or poly_df is None:
            _log(f"  ⚠️ [compare] {ticker}: 下載失敗，本輪跳過（下次重試）")
        else:
            report = _compare_ohlcv(mp_df, poly_df)
            report["ticker"] = ticker
            report["generated_at"] = _now_est().isoformat()
            _stage_write(
                ticker, "compare_report.json",
                json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8"),
            )
            _log(f"  ✅ [compare] {ticker}: 重疊{report['overlap_days']}天, "
                 f"缺poly{len(report['missing_in_poly'])}天, 缺mp{len(report['missing_in_mp'])}天")

        if processed % HEARTBEAT_EVERY == 0:
            db.set_progress(COMPARE_PROGRESS_ID, last_ticker, processed)

        if since_upload >= UPLOAD_BATCH_SIZE:
            _flush_upload(api, f"poly compare batch (upto {ticker})")
            since_upload = 0

    _flush_upload(api, f"poly compare final flush (upto {last_ticker})")
    db.set_progress(COMPARE_PROGRESS_ID, last_ticker, processed)
    return {"processed": processed, "remaining_before": len(remaining)}


def rebuild_global_summary(api: HfApi) -> dict:
    """核對完後重新掃描全部 compare_report.json，重建全域彙總（按close最大差距排序）。"""
    compared = sorted(_list_tickers_with_file(api, "compare_report.json"))
    rows = []
    for ticker in compared:
        try:
            path = api.hf_hub_download(
                repo_id=HF_REPO_ID, repo_type="dataset",
                filename=f"{HF_TICKER_DIR}/{ticker}/compare_report.json",
            )
            with open(path, encoding="utf-8") as f:
                report = json.load(f)
        except Exception:
            continue
        value_diff = report.get("value_diff", {}) or {}
        close_diff = value_diff.get("close") or {}
        volume_diff = value_diff.get("volume") or {}
        rows.append({
            "ticker": ticker,
            "overlap_days": report.get("overlap_days"),
            "close_avg_pct": close_diff.get("avg_pct"),
            "close_max_pct": close_diff.get("max_pct"),
            # 僅供參考、不參與排序（用戶2026-07-17拍板：volume只看數量級，
            # 不同資料源統計口徑本來就不同，不該當成「有問題」的判準）
            "volume_same_magnitude_pct": volume_diff.get("same_magnitude_pct"),
            "missing_in_poly_count": len(report.get("missing_in_poly", [])),
            "missing_in_mp_count":   len(report.get("missing_in_mp", [])),
        })

    rows.sort(key=lambda r: (r["close_max_pct"] is None, -(r["close_max_pct"] or 0)))

    summary = {
        "generated_at": _now_est().isoformat(),
        "ticker_count": len(rows),
        "rows": rows,
    }
    _stage_root_write(
        "mp_poly_compare_summary.json",
        json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8"),
    )
    _flush_upload(api, f"poly compare: rebuild global summary ({len(rows)} tickers)")
    return summary


# ─────────────────────────────────────────────
# 主流程（fetch/compare各自獨立、互相自鏈dispatch，見檔頭docstring）
# ─────────────────────────────────────────────

def _phase_deadline_mono(start_mono: float, budget_seconds: float, cutoff_today: datetime) -> float:
    """
    可用時間 = min(自己的budget, 到16:00剩的時間) —— 保底預留寫法，不是
    「算完才發現剩多少」，確保不管這次workflow是幾點被dispatch起來，都不會
    因為wall-clock比budget先到就被硬吃光（見檔頭docstring「停止條件」）。
    """
    wallclock_seconds_left = (cutoff_today - _now_est()).total_seconds()
    return start_mono + max(0, min(budget_seconds, wallclock_seconds_left))


def _run_fetch_cycle(db: "PolyDB", api: HfApi, cutoff_today: datetime) -> int:
    if not db.acquire_lock(FETCH_LOCK_ID, FETCH_LOCK_STALE_SECONDS):
        _log("⏭️ [fetch] 搶不到鎖（上一輪還在跑，或未過staleness判定孤兒），本次跳過")
        return 0

    try:
        start_mono = time.monotonic()
        deadline_mono = _phase_deadline_mono(start_mono, FETCH_BUDGET_SECONDS, cutoff_today)
        _notify(
            f"🚀 [poly-fetch] 開始（本輪預算至多"
            f"{(deadline_mono - start_mono) / 3600:.1f}h，"
            f"{DAY_CUTOFF_HOUR}:00 EST 前必收工）"
        )
        fetch_stats = run_fetch_phase(db, api, deadline_mono)
        msg = (
            f"📸 [poly-fetch] 本輪結束\n"
            f"抓取：本輪{fetch_stats['processed']}支（失敗{fetch_stats['failed']}）"
        )
        _log(msg)
        _notify(msg)
    finally:
        db.release_lock(FETCH_LOCK_ID)

    if _now_est() < cutoff_today and _has_compare_backlog(api):
        dispatch_workflow(COMPARE_WORKFLOW_FILE)
    return 0


def _run_compare_cycle(db: "PolyDB", api: HfApi, cutoff_today: datetime) -> int:
    if not db.acquire_lock(COMPARE_LOCK_ID, COMPARE_LOCK_STALE_SECONDS):
        _log("⏭️ [compare] 搶不到鎖（上一輪還在跑，或未過staleness判定孤兒），本次跳過")
        return 0

    try:
        start_mono = time.monotonic()
        deadline_mono = _phase_deadline_mono(start_mono, COMPARE_BUDGET_SECONDS, cutoff_today)
        _notify(
            f"🚀 [poly-compare] 開始（本輪預算至多"
            f"{(deadline_mono - start_mono) / 60:.0f}分鐘，"
            f"{DAY_CUTOFF_HOUR}:00 EST 前必收工）"
        )
        compare_stats = run_compare_phase(db, api, deadline_mono)
        summary = None
        if time.monotonic() < deadline_mono:
            summary = rebuild_global_summary(api)

        msg = f"📸 [poly-compare] 本輪結束\n核對：本輪{compare_stats['processed']}支"
        if summary:
            msg += f"\n全域彙總：累計已核對{summary['ticker_count']}支（見 mp_poly_compare_summary.json）"
        _log(msg)
        _notify(msg)
    finally:
        db.release_lock(COMPARE_LOCK_ID)

    if _now_est() < cutoff_today and _has_fetch_backlog(api):
        dispatch_workflow(FETCH_WORKFLOW_FILE)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["fetch", "compare"], required=True)
    args = parser.parse_args()

    if not MONGO_URI or not POLYGON_KEY or not HF_TOKEN_ENV:
        _log("❌ MONGO_URI/POLYGON_KEY/HF_TOKEN 未齊全，無法執行")
        return 1

    now = _now_est()
    cutoff_today = now.replace(hour=DAY_CUTOFF_HOUR, minute=0, second=0, microsecond=0)
    if now >= cutoff_today:
        _log(f"⏭️ 已過 {DAY_CUTOFF_HOUR}:00 EST，今日不再啟動新一輪（phase={args.phase}）")
        _notify(f"⏭️ [poly-{args.phase}] 已過{DAY_CUTOFF_HOUR}:00 EST，本次跳過，等下次觸發")
        return 0

    db = PolyDB()
    api = _hf_api()

    if args.phase == "fetch":
        return _run_fetch_cycle(db, api, cutoff_today)
    else:
        return _run_compare_cycle(db, api, cutoff_today)


if __name__ == "__main__":
    sys.exit(main())
