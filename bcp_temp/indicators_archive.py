"""
indicators_archive.py — StockData.Indicators 每日冷熱分層：全量歷史歸檔 HF Dataset + Mongo 只留最近 N 天。

背景（2026-09-13 查證，見 handoff 落差表）：
  Indicators = 每票每交易日一筆的技術指標時間序列（DC after_hours 寫、{ticker,date} 唯一）。
  全系統只讀「最新一筆」（AC $group $first、comm-hub/BC.p/DC 皆 find_one sort date -1）。
  DC database.py 的 get_indicators_history(days=60) 是**死代碼**（全 repo 無呼叫端）；
  dist_pct_rank_60d 實際在 indicator_logic.py 從 **D bars（df_d）現算**、**不讀本 collection 歷史**。
  → 沒有任何 live 讀者需要 Indicators 歷史；update_indicators 註釋「不刪除歷史/支持 60 日」是過時文件。

做法：全量歷史歸檔 Dataset（保住「不刪除歷史」原意、未來若接歷史走 dataset_gate 讀）+ Mongo 只留最近 N 天。
  歸檔格式：indicators/{YYYY-MM-DD}.jsonl.gz（每日一檔、gzip、每行一筆該日 doc；datetime→isoformat、去 _id）。
  保留窗：最近 N 個「有資料的日期」（預設 15，用戶 2026-09-13 拍板）。日期本身即交易日、無需假日曆。
  順序鐵律（比照 snapshot_archive）：先上傳 dataset 成功、才 delete_many 刪 Mongo。上傳失敗不刪、下次重試。
  marker：Archive_State {id:"indicators_archive", last_run_date}，每日只做一次。
env：HF_TOKEN（寫 dataset）、HF_REPO_ID（預設 zhujun0511-AI/ai-telegram-bot-dataset）。Python 3.9 相容。
"""
import os
import json
import gzip
import shutil
import tempfile
from datetime import datetime
from collections import defaultdict

import pytz

EST = pytz.timezone("US/Eastern")

HF_REPO_ID_DEFAULT = "zhujun0511-AI/ai-telegram-bot-dataset"
ARCHIVE_PREFIX = "indicators"
MARKER_ID = "indicators_archive"
DEFAULT_KEEP_DAYS = 15


def _json_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)


def run_indicators_archive(stock_db, hf_token, hf_repo=HF_REPO_ID_DEFAULT,
                           keep_days=DEFAULT_KEEP_DAYS, execute=True,
                           respect_marker=True, log=print):
    """
    回傳 dict 摘要。execute=False 為 dry-run（不寫 dataset、不刪 Mongo）。
    stock_db: pymongo StockData handle。
    """
    now_est = datetime.now(EST)
    today_str = now_est.strftime("%Y-%m-%d")

    marker_col = stock_db["Archive_State"]
    if respect_marker:
        m = marker_col.find_one({"id": MARKER_ID})
        if m and m.get("last_run_date") == today_str:
            log("[indicators_archive] 今日(%s)已歸檔過，跳過" % today_str)
            return {"skipped": True, "reason": "already_ran_today"}

    ind = stock_db["Indicators"]

    # 有資料的日期（字串 YYYY-MM-DD），降序；保留最近 keep_days 個、其餘歸檔+刪
    all_dates = sorted((d for d in ind.distinct("date") if isinstance(d, str)), reverse=True)
    keep_dates = set(all_dates[:keep_days])
    old_dates = [d for d in all_dates if d not in keep_dates]

    summary = {"total_dates": len(all_dates), "keep_days": keep_days,
               "keep_dates": len(keep_dates), "old_dates": len(old_dates), "executed": False}

    if not old_dates:
        log("[indicators_archive] 沒有需要歸檔的日期（共 %d 個日期 ≤ 保留窗 %d）" % (len(all_dates), keep_days))
        if execute and respect_marker:
            marker_col.update_one({"id": MARKER_ID},
                                  {"$set": {"id": MARKER_ID, "last_run_date": today_str,
                                            "last_archived": 0, "updated_at": now_est}}, upsert=True)
        return summary

    log("[indicators_archive] 共 %d 日期；保留最近 %d（%s ~ %s）；歸檔+刪 %d 個舊日期（%s ~ %s）"
        % (len(all_dates), len(keep_dates), min(keep_dates), max(keep_dates),
           len(old_dates), old_dates[-1], old_dates[0]))

    if not execute:
        # dry-run：只數各舊日期的 doc 數，不下載內容
        n = ind.count_documents({"date": {"$in": old_dates}})
        summary["archive_docs"] = n
        log("[indicators_archive] DRY-RUN：待歸檔 %d 筆 doc / %d 個日期（不寫不刪）" % (n, len(old_dates)))
        return summary

    if not hf_token:
        log("[indicators_archive] ⚠️ HF_TOKEN 未設定，跳過歸檔（不刪 Mongo，等設好再跑）")
        return {"skipped": True, "reason": "no_hf_token"}

    # ── 1. 撈舊日期全部 doc、依日期分檔寫本地 gzip jsonl ──
    file_lines = defaultdict(list)
    total = 0
    for doc in ind.find({"date": {"$in": old_dates}}):
        d = doc.get("date")
        if not isinstance(d, str):
            continue
        doc.pop("_id", None)   # Mongo 內部 id、不入 dataset
        file_lines[d].append(json.dumps(doc, ensure_ascii=False, default=_json_default))
        total += 1

    if total == 0:
        log("[indicators_archive] 舊日期查無 doc（異常，跳過刪除）")
        return summary

    staging = tempfile.mkdtemp(prefix="ind_archive_")
    try:
        adir = os.path.join(staging, ARCHIVE_PREFIX)
        os.makedirs(adir, exist_ok=True)
        for d, lines in file_lines.items():
            raw = ("\n".join(lines) + "\n").encode("utf-8")
            with gzip.open(os.path.join(adir, "{}.jsonl.gz".format(d)), "wb") as f:
                f.write(raw)

        # ── 2. upload_folder（SDK 內建重試/分批），成功才往下刪 ──
        from huggingface_hub import HfApi
        api = HfApi(token=hf_token)
        log("[indicators_archive] upload_folder → %s（%d 檔 / %d 筆）..." % (hf_repo, len(file_lines), total))
        api.upload_folder(
            folder_path=staging, repo_id=hf_repo, repo_type="dataset",
            commit_message="indicators archive: {} docs, {} dates (keep {})".format(
                total, len(file_lines), keep_days),
        )
        log("[indicators_archive] upload 成功")
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    # ── 3. 上傳成功才 delete_many（只刪已歸檔的舊日期）──
    res = ind.delete_many({"date": {"$in": old_dates}})
    deleted = res.deleted_count
    log("[indicators_archive] Mongo delete_many 完成（刪 %d 筆）" % deleted)

    marker_col.update_one({"id": MARKER_ID},
                          {"$set": {"id": MARKER_ID, "last_run_date": today_str,
                                    "last_archived": total, "last_deleted": deleted,
                                    "updated_at": now_est}}, upsert=True)

    # ── 4. 更新 DATA_MAP（indicators 格 + provenance；失敗只告警）──
    try:
        import data_map as _dm
        _dm.update_data_map(hf_repo, hf_token, "indicators", {
            "path_template": "indicators/{date}.jsonl.gz", "format": "jsonl.gz",
            "source": "StockData.Indicators（每票每日技術指標）", "scope": "全量歷史",
        }, "indicators_archive.py", note="每日歸檔 %d 筆 / %d 日期（Mongo 留最近 %d 天）" % (
            total, len(file_lines), keep_days))
        log("[indicators_archive] DATA_MAP 已更新（indicators <- indicators_archive.py）")
    except Exception as e:
        log("[indicators_archive] ⚠️ DATA_MAP 更新失敗（不影響歸檔）: %s" % e)

    summary["executed"] = True
    summary["archive_docs"] = total
    summary["docs_deleted"] = deleted
    return summary


if __name__ == "__main__":
    import sys
    import pymongo

    execute = "--execute" in sys.argv
    ignore_marker = "--ignore-marker" in sys.argv
    keep_days = DEFAULT_KEEP_DAYS
    if "--keep-days" in sys.argv:
        try:
            keep_days = int(sys.argv[sys.argv.index("--keep-days") + 1])
        except Exception:
            print("⚠️ --keep-days 需接整數，沿用預設 %d" % DEFAULT_KEEP_DAYS)

    uri = os.getenv("MONGO_URI", "").strip()
    if not uri:
        print("[indicators_archive] 找不到 MONGO_URI"); sys.exit(1)
    hf_token = os.getenv("HF_TOKEN", "").strip()
    hf_repo = os.getenv("HF_REPO_ID", "").strip() or HF_REPO_ID_DEFAULT

    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=15000)
    client.admin.command("ping")
    res = run_indicators_archive(client["StockData"], hf_token, hf_repo,
                                 keep_days=keep_days, execute=execute,
                                 respect_marker=not ignore_marker, log=print)
    print("[indicators_archive] summary:", json.dumps(res, ensure_ascii=False, default=str))
    if not execute:
        print("[indicators_archive] DRY-RUN；加 --execute 才寫 dataset + 刪 Mongo。"
              "（--keep-days N 改保留天數、--ignore-marker 補跑）")
