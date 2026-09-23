"""
snapshot_archive.py — SIGNAL_SNAPSHOTS 每日封存到 HF Dataset + 裁切 Mongo（只留最近2交易日）。

由 driver_live.py 工作鏈「最後一 phase」呼叫（見該檔 main() 末段）。整包 try 包住，
掛掉只 log、絕不影響 CFET judge / Key Level / Telegram 已完成的結果。

順序鐵律（用戶拍板）：先封存到 dataset、確認上傳成功，才 $pull 刪 Mongo。上傳失敗就不刪，
留待下次重試（下次 fanout / 00:00 EST 安全網）。

封存格式：signal_snapshots/{ticker}/{YYYY-MM-DD}.jsonl.gz
  - 每票每日一檔、gzip（實測壓縮率~4%），每行一筆事件 JSON（datetime → isoformat）
  - (ticker, date) 供 comm-hub /report/view fallback 精確定位（見該端 dataset 回讀）

保留窗：最近 2 交易日。cutoff = 第2近的已完成交易日（含）；created_at(UTC) < cutoff_utc 封存+刪。

marker：StockData.Archive_State {id:"snapshot_daily_archive", last_run_date}，每日只做一次
（用戶要求：獨立 collection、不塞 System_State）。

環境變數：HF_TOKEN（寫 dataset）、HF_REPO_ID（預設 zhujun0511-AI/ai-telegram-bot-dataset）。

Python 3.9 相容。
"""
import os
import json
import gzip
import shutil
import tempfile
from datetime import datetime, timedelta
from collections import defaultdict

import pytz

EST = pytz.timezone("US/Eastern")
UTC = pytz.UTC

HF_REPO_ID_DEFAULT = "zhujun0511-AI/ai-telegram-bot-dataset"
ARCHIVE_PREFIX = "signal_snapshots"
MARKER_ID = "snapshot_daily_archive"

# 與 db.py FALLBACK_HOLIDAYS 同源（跨檔各自維護一份，比照專案既有慣例）
FALLBACK_HOLIDAYS = {
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-04-18", "2025-05-26",
    "2025-06-19", "2025-07-04", "2025-09-01", "2025-11-27", "2025-12-25",
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03", "2026-05-25",
    "2026-09-07", "2026-11-26", "2026-12-25",
}


def _holidays(stock_db):
    try:
        doc = stock_db["Configs"].find_one({"type": "market_calendar"})
        if doc and doc.get("holidays"):
            return set(doc["holidays"])
    except Exception:
        pass
    return set(FALLBACK_HOLIDAYS)


def _is_trading_day(d, hols):
    return d.weekday() < 5 and d.strftime("%Y-%m-%d") not in hols


def _prev_trading_day(d, hols):
    d = d - timedelta(days=1)
    while not _is_trading_day(d, hols):
        d -= timedelta(days=1)
    return d


def _compute_cutoff(stock_db, now_est):
    """回傳 (cutoff_est_date, cutoff_utc_naive)。保留最近2交易日（最近已完成交易日+前一交易日）+當天。"""
    hols = _holidays(stock_db)
    today = now_est.date()
    recent = today if (_is_trading_day(today, hols) and now_est.hour >= 16) else _prev_trading_day(today, hols)
    cutoff_est_date = _prev_trading_day(recent, hols)
    cutoff_utc_naive = EST.localize(datetime(cutoff_est_date.year, cutoff_est_date.month,
                                             cutoff_est_date.day, 0, 0, 0)).astimezone(UTC).replace(tzinfo=None)
    return cutoff_est_date, cutoff_utc_naive


def _est_date_of(ca):
    """created_at → EST 日期字串。naive 視為 UTC（pymongo 預設回 naive UTC）。"""
    if not isinstance(ca, datetime):
        return None
    dt = ca if ca.tzinfo else UTC.localize(ca)
    return dt.astimezone(EST).strftime("%Y-%m-%d")


def _json_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)


def run_snapshot_archive(stock_db, hf_token, hf_repo=HF_REPO_ID_DEFAULT,
                         execute=True, respect_marker=True, log=print):
    """
    回傳 dict 摘要。execute=False 為 dry-run（不寫 dataset、不刪 Mongo）。
    stock_db: pymongo StockData handle（BC.p 傳 CfetLiveDB.stock_db）。
    """
    now_est = datetime.now(EST)
    today_str = now_est.strftime("%Y-%m-%d")

    marker_col = stock_db["Archive_State"]
    if respect_marker:
        m = marker_col.find_one({"id": MARKER_ID})
        if m and m.get("last_run_date") == today_str:
            log(f"[snapshot_archive] 今日({today_str})已封存過，跳過")
            return {"skipped": True, "reason": "already_ran_today"}

    if execute and not hf_token:
        log("[snapshot_archive] ⚠️ HF_TOKEN 未設定，跳過封存（不刪 Mongo，等設好再跑）")
        return {"skipped": True, "reason": "no_hf_token"}

    cutoff_est_date, cutoff_utc = _compute_cutoff(stock_db, now_est)
    log(f"[snapshot_archive] cutoff_est={cutoff_est_date}（保留此日期含以後的事件；更早的封存+刪）")

    bars = stock_db["Bars"]                            # 合併後：單一 Bars，一次撈所有 SIGNAL_SNAPSHOTS doc
    file_lines = defaultdict(list)
    tickers_touched = set()
    total_archive = 0

    for doc in bars.find({"period": "SIGNAL_SNAPSHOTS"}):
        ticker = (doc.get("ticker") or "").upper()
        if not ticker:
            continue
        for ev in doc.get("events", []):
            ca = ev.get("created_at")
            if not isinstance(ca, datetime):
                continue
            ca_naive = ca.replace(tzinfo=None) if ca.tzinfo else ca
            if ca_naive >= cutoff_utc:
                continue
            d = _est_date_of(ca)
            if not d:
                continue
            file_lines[(ticker, d)].append(json.dumps(ev, ensure_ascii=False, default=_json_default))
            tickers_touched.add(ticker)
            total_archive += 1

    summary = {"cutoff_est_date": str(cutoff_est_date), "archive_events": total_archive,
               "files": len(file_lines), "tickers": len(tickers_touched), "executed": False}

    if total_archive == 0:
        log("[snapshot_archive] 沒有需要封存的事件（保留窗內全部）")
        if execute and respect_marker:
            marker_col.update_one({"id": MARKER_ID},
                                  {"$set": {"id": MARKER_ID, "last_run_date": today_str,
                                            "last_archived": 0, "updated_at": now_est}}, upsert=True)
        return summary

    log(f"[snapshot_archive] 待封存 {total_archive} 筆 / {len(file_lines)} 檔 / {len(tickers_touched)} 票")

    if not execute:
        log("[snapshot_archive] DRY-RUN：不寫 dataset、不刪 Mongo")
        return summary

    # ── 1. 寫本地暫存（gzip jsonl）──
    staging = tempfile.mkdtemp(prefix="snap_archive_")
    try:
        for (ticker, d), lines in file_lines.items():
            tdir = os.path.join(staging, ARCHIVE_PREFIX, ticker)
            os.makedirs(tdir, exist_ok=True)
            raw = ("\n".join(lines) + "\n").encode("utf-8")
            with gzip.open(os.path.join(tdir, "{}.jsonl.gz".format(d)), "wb") as f:
                f.write(raw)

        # ── 2. upload_folder（SDK 內建 429 重試/分批），成功才往下刪 ──
        from huggingface_hub import HfApi
        api = HfApi(token=hf_token)
        log(f"[snapshot_archive] upload_folder → {hf_repo}（{len(file_lines)} 檔）...")
        api.upload_folder(
            folder_path=staging, repo_id=hf_repo, repo_type="dataset",
            commit_message="snapshot archive: {} events, {} files (<{})".format(
                total_archive, len(file_lines), cutoff_est_date),
        )
        log("[snapshot_archive] upload 成功")
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    # ── 3. 上傳成功才 $pull（原子、只刪 < cutoff，不碰並發新寫入）──
    res = bars.update_many(
        {"period": "SIGNAL_SNAPSHOTS"},
        {"$pull": {"events": {"created_at": {"$lt": cutoff_utc}}}},
    )
    deleted = res.modified_count
    log(f"[snapshot_archive] Mongo $pull 完成（裁切 {deleted} 個 ticker doc）")

    marker_col.update_one({"id": MARKER_ID},
                          {"$set": {"id": MARKER_ID, "last_run_date": today_str,
                                    "last_archived": total_archive, "updated_at": now_est}}, upsert=True)

    # ── 4. 更新 DATA_MAP（signal_snapshots 格 + provenance；2026-09-05 加）──
    #    written_by 必填、無法匿名寫；失敗只告警、不影響封存本體。見 data_map.py / 六-D。
    try:
        import data_map as _dm
        _dm.update_data_map(hf_repo, hf_token, "signal_snapshots", {
            "path_template": "signal_snapshots/{ticker}/{date}.jsonl.gz", "format": "jsonl.gz",
        }, "snapshot_archive.py", note="每日封存 %d 筆/%d 檔" % (total_archive, len(file_lines)))
        log("[snapshot_archive] DATA_MAP 已更新（signal_snapshots <- snapshot_archive.py）")
    except Exception as e:
        log("[snapshot_archive] ⚠️ DATA_MAP 更新失敗（不影響封存）: %s" % e)

    summary["executed"] = True
    summary["docs_trimmed"] = deleted
    return summary


if __name__ == "__main__":
    # 獨立每日 job 進入點（2026-09-13）：讓 snapshot_daily.yml 直接 `python snapshot_archive.py --execute`
    # 跑，不再只掛在 driver_live 工作鏈最後一 phase（cfet judge timeout 就到不了它、歸檔靜默停擺，
    # 見 handoff/DANGER_ZONES 2026-08-23、2026-09-13）。driver_live 那邊照舊呼叫，marker 保證每日
    # 只做一次、兩邊誰先跑都行（冪等、互不干擾）。env 走 secrets（比照 driver_live 的 os.getenv 慣例）。
    import sys
    import pymongo

    execute = "--execute" in sys.argv
    ignore_marker = "--ignore-marker" in sys.argv   # 補跑用：忽略今日 marker 強制重算

    uri = os.getenv("MONGO_URI", "").strip()
    if not uri:
        print("[snapshot_archive] 找不到 MONGO_URI"); sys.exit(1)
    hf_token = os.getenv("HF_TOKEN", "").strip()
    # yaml 會把未建的 secret 設成空字串，故用 or 兜底（不能靠 getenv 第2參數）
    hf_repo = os.getenv("HF_REPO_ID", "").strip() or HF_REPO_ID_DEFAULT

    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=15000)
    client.admin.command("ping")
    res = run_snapshot_archive(client["StockData"], hf_token, hf_repo,
                               execute=execute, respect_marker=not ignore_marker, log=print)
    print("[snapshot_archive] summary:", json.dumps(res, ensure_ascii=False, default=str))
    if not execute:
        print("[snapshot_archive] DRY-RUN；加 --execute 才寫 dataset + $pull 裁 Mongo。"
              "（--ignore-marker 忽略今日 marker，補跑用）")
