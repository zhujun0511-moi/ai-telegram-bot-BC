"""
bars_archive.py — H(30分) 不復權 append 續寫進 dataset：mp_data/ticker/{ticker}/h.csv

⚠️⚠️ 2026-09-06 改為 append（推翻先前「鏡像/覆蓋不累加」）：
  h＝不復權、當天篩選用、要**長歷史**（用戶首次已用 Yahoo 補完 4 lists 小時不復權長歷史）。
  本腳本走 Mongo 路徑時**必須 append 續寫、絕不整檔覆蓋**——否則會拿 Mongo 短窗蓋掉長歷史。
  做法：逐票**讀現有 h.csv → 只補現有沒有的 datetime（existing 優先、不覆蓋）→ 排序寫回**。
  ⚠️ existing 有內容卻解析不到列（疑格式異常）＝**跳過不覆蓋**（保守，寧可不寫也不毀資料）。

（歷史：2026-09-05 曾把 H 鏡像成 h.csv〔覆蓋〕；2026-09-06 用戶定案 h 要 append 長歷史。
  見 handoff_subsystem_dataset_map.md 二.5 + DANGER_ZONES 2026-09-06。）

寫入模式（地圖 write_modes）：前復權 d/w/m＝overwrite（本地 Yahoo）；不復權 d_raw/w_raw/m_raw + h＝append。
只寫 dataset、Mongo 不動；只碰各票 h.csv，不動 d/w/m/_raw。
cadence：**每日** bars_daily.yml（盤後）或手動。archive 成功後把「已確認全量在 dataset」的票 Atlas H
  裁到最新 keep(=800) 根（順序鐵律：archive→驗證上傳成功→才 trim；跳過/失敗的票絕不裁）；dataset 保全量、
  Atlas 只留熱層 800。（2026-09-12：月排程→每日 + 加 trim，冷熱分層。keep=0/trim=False 則只 archive 不裁。）
env：MONGO_URI、HF_TOKEN、HF_REPO_ID（預設 zhujun0511-AI/ai-telegram-bot-dataset）。Python 3.9 相容。
"""
import os
import sys
import csv
import io
import json
import shutil
import tempfile
from datetime import datetime

import pytz

import data_map as dm   # 同 repo（BC.p）vendored canonical；hook 用

EST = pytz.timezone("US/Eastern")
HF_REPO_ID_DEFAULT = "zhujun0511-AI/ai-telegram-bot-dataset"
MARKER_ID = "bars_backup"
WRITTEN_BY = "bars_archive.py"

# 更新地圖只宣告 h 格（merge_product_entry 逐 period 取代、不清 local_yahoo 的 d/w/m/d_raw…）。
# ⚠️ h 完整 metadata（adjust/purpose/scope/write_mode）要帶齊——merge 是「整個 period 取代」，帶不齊
#    會把 consolidation 寫進地圖的欄位洗掉（h＝不復權、篩選、append 續寫，別誤對齊前復權/誤標覆蓋）。
TICKER_BARS_H_DESC = {
    "root": "mp_data/ticker",
    "path_template": "mp_data/ticker/{ticker}/{period}.csv",
    "periods": {"h": {"file": "h.csv", "source": "yahoo(首次補充)→mongo_h(未來)",
                      "adjust": "unadjusted", "purpose": "screen", "scope": "watchlist",
                      "cols": ["datetime", "open", "high", "low", "close", "volume"],
                      "write_mode": "append(續寫不覆蓋)", "coverage": "long_history(首次 Yahoo 已補、之後只 append)"}},
}


def _full_set(stock_db):
    """Configs.ticker_lists 的 full_set（追蹤宇宙）。缺則回空 set（呼叫端 fail-safe 退回全 H）。"""
    cfg = stock_db["Configs"].find_one({"type": "ticker_lists"}) or {}
    fs = cfg.get("full_set") or (cfg.get("lists", {}) or {}).get("full_set") or []
    return set(str(t).upper() for t in fs)


def _parse_h_csv(text):
    """現有 h.csv → dict{datetime_str: [6 欄字串]}（**保留原字串、不重格式化**，避免動到既有精度）。"""
    rows = {}
    for ln in (text or "").splitlines():
        ln = ln.strip()
        if not ln or ln.lower().startswith("datetime"):
            continue
        parts = ln.split(",")
        if len(parts) >= 6 and parts[0]:
            rows[parts[0]] = parts[:6]
    return rows


def _merge_h_append(existing_text, mongo_bars):
    """append 續寫、不覆蓋：existing 為權威，只補現有沒有的 datetime。
    回 (csv_text 或 None, added, existing_count)。chronological 舊在前、LF。"""
    existing_rows = _parse_h_csv(existing_text)
    existing_count = len(existing_rows)
    rows = dict(existing_rows)
    added = 0
    for b in mongo_bars:
        t = b.get("t")
        dt = str(t) if t is not None else ""
        if dt and dt not in rows:      # existing 優先＝不覆蓋；只加現有沒有的 datetime
            rows[dt] = [dt, b.get("o"), b.get("h"), b.get("l"), b.get("c"), b.get("v")]
            added += 1
    if not rows:
        return None, 0, existing_count
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["datetime", "open", "high", "low", "close", "volume"])
    for dt in sorted(rows.keys()):
        w.writerow(rows[dt])
    return buf.getvalue(), added, existing_count


def _download_h_text(repo, token, ticker):
    """讀 dataset 上 mp_data/ticker/{t}/h.csv；不存在（404）回 None；其他錯往上拋（呼叫端跳過不覆蓋）。"""
    from huggingface_hub import hf_hub_download
    try:
        local = hf_hub_download(repo_id=repo, filename="mp_data/ticker/%s/h.csv" % ticker,
                                repo_type="dataset", token=token)
    except Exception as e:
        if type(e).__name__ in ("EntryNotFoundError", "RepositoryNotFoundError") or "404" in str(e):
            return None
        raise
    with open(local, "r", encoding="utf-8") as f:
        return f.read()


def _trim_h_atlas(bars_col, tickers, keep, log):
    """把「已確認全量在 dataset」的票的 Atlas H doc 裁到最新 keep 根。
    **伺服器端** `$push($each:[]/$slice:keep)`＝保留陣列前 keep 個＝最新 keep 根
    （H bars 是 newest-first 不變式，全系統一致：DC push_bars / data_core merge、
    indicator_logic「新在前」、實測 NVDA/XLK b[0]=最新）。**不載入 bar 到本地**——
    這是舊版逐票 find_one 載 ~1700 根×數百票、trim 慢到 13 分撞 timeout 的根因。
    ≤keep 的 doc：$slice 保留全部＝no-op（modified_count=0、不計入 trimmed）。
    安全網：呼叫端只傳 archive 已確認 dataset⊇Atlas 的票；萬一某票序反，dataset 仍有
    全量可復原（archive 先於 trim）。回傳實際被裁短的票數（modified_count 計）。"""
    trimmed = 0
    for t in sorted(tickers):
        r = bars_col.update_one(
            {"ticker": t, "period": "H"},
            {"$push": {"bars": {"$each": [], "$slice": keep}}},
        )
        if getattr(r, "modified_count", 0):
            trimmed += 1
    if trimmed:
        log("[bars_archive] Atlas H 已裁到最新 %d 根：%d 支（伺服器端 $slice、dataset 保全量）" % (keep, trimmed))
    return trimmed


def trim_15m_atlas(stock_db, keep=400, execute=True, log=print):
    """把所有 period=15m doc 裁到最新 keep 根（伺服器端 $push($each:[]/$slice:keep)、不載入 bar）。

    15m 是**盤中衍生緩衝、非源資料**（DC 盤中讀 15m→合成 30m，只用最新那根；深歷史走 period=H）——
    無任何深歷史讀者，h.csv(30分) 已是保存下來的盤中歷史，故 15m **直接 cap、不歸檔到 dataset**
    （用戶 2026-09-13 拍板；yfinance 本來也只能回抓 ~60 天 15m、深歷史留著無法重建）。與 H trim 不同：
    ①不需 HF_TOKEN、不依賴 dataset ②對**所有** period=15m（含合成籃、不限 full_set）一律 cap
    （順帶解 DANGER_ZONES 2026-09-13「合成籃 15m 不受 bars_daily trim」的洞）。
    ⚠️ 安全關鍵：15m bars 必須 **newest-first**（實測 SPY/AAPL/NVDA bars[0]=最新 09-11、bars[-1]=最舊）——
    $slice 保留陣列前 keep 個＝最新 keep 根。冪等：≤keep 的 doc no-op（modified_count=0）。回傳摘要 dict。"""
    bars = stock_db["Bars"]
    if not execute:
        pipe = [{"$match": {"period": "15m"}},
                {"$project": {"n": {"$size": {"$ifNull": ["$bars", []]}}}},
                {"$match": {"n": {"$gt": keep}}}, {"$count": "c"}]
        r = list(bars.aggregate(pipe, allowDiskUse=True))
        over = r[0]["c"] if r else 0
        log("[bars_archive] DRY-RUN 15m：%d 支超過 %d 根待裁（不裁）" % (over, keep))
        return {"trim_15m_over": over, "keep_15m": keep, "executed": False}
    tickers = sorted(str(t).upper() for t in bars.distinct("ticker", {"period": "15m"}))
    trimmed = 0
    for t in tickers:
        r = bars.update_one({"ticker": t, "period": "15m"},
                            {"$push": {"bars": {"$each": [], "$slice": keep}}})
        if getattr(r, "modified_count", 0):
            trimmed += 1
    log("[bars_archive] Atlas 15m 已裁到最新 %d 根：%d 支被裁短（共 %d 支 15m；伺服器端 $slice、不歸檔）"
        % (keep, trimmed, len(tickers)))
    return {"trim_15m": trimmed, "tickers_15m": len(tickers), "keep_15m": keep, "executed": True}


def run_bars_backup(stock_db, hf_token, hf_repo=HF_REPO_ID_DEFAULT,
                    execute=True, respect_marker=True, keep=800, trim=True, log=print):
    """把有 H 的追蹤票 **append 續寫** 進 mp_data/ticker/{ticker}/h.csv（existing 優先不覆蓋）+ 更新地圖，
    archive 成功後把「已確認在 dataset」的票 Atlas H 裁到最新 keep 根（冷熱分層：dataset 全量、Atlas 熱層）。
    順序鐵律：archive→驗證上傳成功→才 trim；跳過/上傳失敗的票絕不裁。keep=0 或 trim=False → 只 archive。
    回傳摘要 dict。"""
    now_est = datetime.now(EST)
    today_str = now_est.strftime("%Y-%m-%d")

    marker_col = stock_db["Archive_State"]
    if respect_marker:
        m = marker_col.find_one({"id": MARKER_ID})
        if m and m.get("last_run_date") == today_str:
            log("[bars_archive] 今日(%s)已跑過，跳過" % today_str)
            return {"skipped": True, "reason": "already_ran_today"}

    if execute and not hf_token:
        log("[bars_archive] ⚠️ HF_TOKEN 未設定，跳過")
        return {"skipped": True, "reason": "no_hf_token"}

    bars_col = stock_db["Bars"]                       # 合併後：單一 Bars，用 distinct 取有 H 的票
    has_h = set(str(t).upper() for t in bars_col.distinct("ticker", {"period": "H"}))
    fs = _full_set(stock_db)
    # 限縮 full_set∩有H（免為 Mongo 殭屍在 mp_data 生 h.csv）；full_set 缺則 fail-safe 退回全 H
    tickers = sorted(has_h & fs) if fs else sorted(has_h)
    log("[bars_archive] append H → mp_data/ticker/{t}/h.csv：%d 支（full_set∩有H；Mongo 有H=%d）"
        % (len(tickers), len(has_h)))

    summary = {"tickers": len(tickers), "written": 0, "added": 0, "skipped": 0, "executed": False}
    if not execute:
        log("[bars_archive] DRY-RUN：不讀不寫 dataset")
        return summary

    staging = tempfile.mkdtemp(prefix="bars_h_")
    n = 0
    total_added = 0
    skipped = 0
    written_set = set()    # 本輪 append 上傳成功後可裁的票（added>0）
    complete_set = set()   # added==0：Atlas 全部 bar 早已在 dataset → 可安全裁
    try:
        for t in tickers:
            doc = bars_col.find_one({"ticker": t, "period": "H"}, {"bars": 1})
            new_bars = (doc or {}).get("bars") or []
            # append：讀現有 h.csv（讀失敗＝跳過不覆蓋）
            try:
                existing = _download_h_text(hf_repo, hf_token, t)
            except Exception as e:
                log("[bars_archive] ⚠️ 讀 %s/h.csv 失敗、跳過（不覆蓋）: %s" % (t, e))
                skipped += 1
                continue
            merged, added, existing_count = _merge_h_append(existing, new_bars)
            # 保守守門：existing 有內容卻解析不到列＝疑格式異常 → 跳過不覆蓋
            if existing is not None and existing.strip() and existing_count == 0:
                log("[bars_archive] ⚠️ %s/h.csv 存在但解析 0 列（疑格式異常）、跳過不覆蓋" % t)
                skipped += 1
                continue
            if merged is None:
                continue                       # 無資料
            if existing_count > 0 and added == 0:
                complete_set.add(t)            # Atlas 全部 bar 已在 dataset（added==0）→ 可安全 trim
                continue                       # 無新 bar 可 append、不重寫（省 churn）
            tdir = os.path.join(staging, "mp_data", "ticker", t)
            os.makedirs(tdir, exist_ok=True)
            with open(os.path.join(tdir, "h.csv"), "w", encoding="utf-8", newline="") as f:
                f.write(merged)
            n += 1
            total_added += added
            written_set.add(t)                 # 上傳成功後可裁（見下方 trim）
        if n == 0:
            log("[bars_archive] 無新 bar 可 append（無資料/已同步；跳過 %d）；仍檢查 trim（complete=%d）"
                % (skipped, len(complete_set)))
        else:
            from huggingface_hub import HfApi
            api = HfApi(token=hf_token)
            log("[bars_archive] upload_folder（append-merged h.csv、existing 優先不覆蓋）→ %s（%d 檔 / +%d bar / 跳過 %d）..."
                % (hf_repo, n, total_added, skipped))
            api.upload_folder(
                folder_path=staging, repo_id=hf_repo, repo_type="dataset",
                commit_message="bars_archive: append H -> h.csv (%d files, +%d bars)" % (n, total_added),
            )
            log("[bars_archive] upload 成功（Mongo 未動、d/w/m 未動、existing h 未覆蓋）")

            # 更新 DATA_MAP（h 格 + provenance；失敗只告警、不影響資料）
            try:
                dm.update_data_map(hf_repo, hf_token, "ticker_bars", TICKER_BARS_H_DESC, WRITTEN_BY,
                                   note="每日 append H (%d 檔 / +%d bar)" % (n, total_added))
                log("[bars_archive] DATA_MAP 已更新（ticker_bars.h <- bars_archive.py）")
            except Exception as e:
                log("[bars_archive] ⚠️ DATA_MAP 更新失敗（不影響資料）: %s" % e)
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    # ── trim（順序鐵律：archive 成功後才裁；只裁「已確認全量在 dataset」的票）──
    #   written_set＝本輪 append 上傳成功的票；complete_set＝added==0（Atlas bar 早已全在 dataset）。
    #   兩者都已確認 dataset ⊇ Atlas → 裁到最新 keep 根安全、不丟。跳過/失敗的票不在此集、不裁。
    #   （若上方 upload_folder 拋錯，函數在此之前已中止＝fail-safe 不裁，留待下次。）
    trimmed = 0
    trim_targets = (written_set | complete_set) if (trim and keep) else set()
    if trim_targets:
        trimmed = _trim_h_atlas(bars_col, trim_targets, keep, log)

    if respect_marker:
        marker_col.update_one({"id": MARKER_ID},
                              {"$set": {"id": MARKER_ID, "last_run_date": today_str,
                                        "last_written": n, "last_added": total_added,
                                        "last_trimmed": trimmed, "updated_at": now_est}},
                              upsert=True)
    summary["written"] = n
    summary["added"] = total_added
    summary["skipped"] = skipped
    summary["trimmed"] = trimmed
    summary["executed"] = True
    return summary


# ── 月排程入口（BC.p bars_backup_monthly.yml 跑 python bars_archive.py --execute）+ 手動 ──
def _load_env_file(path=r"C:\Users\zhuju\pass.env.txt"):
    out = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    out[k.strip()] = v.strip()
    except Exception:
        pass
    return out


if __name__ == "__main__":
    import pymongo
    execute = "--execute" in sys.argv
    trim = "--no-trim" not in sys.argv        # 預設會裁；--no-trim 只 archive 不裁（首日觀察用）
    keep = 800
    if "--keep" in sys.argv:
        try:
            keep = int(sys.argv[sys.argv.index("--keep") + 1])
        except Exception:
            print("⚠️ --keep 需接整數，沿用預設 800")
    # 15m 冷熱分層（2026-09-13，P2）：獨立 cap、不歸檔（見 trim_15m_atlas）。--trim-15m 才啟用；
    # 預設 400 根（~2.5 週、遠大於盤中 5 天工作窗）。獨立於 H archive、不需 HF_TOKEN。
    do_trim_15m = "--trim-15m" in sys.argv
    keep_15m = 400
    if "--keep-15m" in sys.argv:
        try:
            keep_15m = int(sys.argv[sys.argv.index("--keep-15m") + 1])
        except Exception:
            print("⚠️ --keep-15m 需接整數，沿用預設 400")
    env = _load_env_file()
    uri = os.getenv("MONGO_URI") or env.get("MONGO_URI", "")
    token = os.getenv("HF_TOKEN") or env.get("HF_TOKEN", "")
    repo = os.getenv("HF_REPO_ID", "").strip() or env.get("HF_REPO_ID", "") or HF_REPO_ID_DEFAULT
    if not uri:
        print("找不到 MONGO_URI"); sys.exit(1)
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=15000)
    client.admin.command("ping")
    res = run_bars_backup(client["StockData"], hf_token=token, hf_repo=repo,
                          execute=execute, keep=keep, trim=trim)
    print("摘要:", json.dumps(res, ensure_ascii=False))
    # 15m trim（獨立於 H archive：即使上面因 marker/無 HF_TOKEN 跳過，15m 仍照裁；不歸檔）
    if do_trim_15m:
        res15 = trim_15m_atlas(client["StockData"], keep=keep_15m, execute=execute, log=print)
        print("15m trim 摘要:", json.dumps(res15, ensure_ascii=False))
    if not execute:
        print("DRY-RUN；加 --execute 才寫 dataset + 裁 Atlas（--no-trim 只 archive 不裁、--keep N 改保留根數、"
              "--trim-15m 啟用 15m cap、--keep-15m N 改 15m 保留根數）。")
