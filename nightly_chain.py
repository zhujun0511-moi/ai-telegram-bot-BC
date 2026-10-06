"""
nightly_chain.py — 夜間收尾鏈（2026-10-05 用戶同意「GitHub 工作鏈式化」）

起因：每晚的歸檔／寫入各自靠 GitHub 排程時鐘，10-05 實測 GitHub 排程延遲最多 8 小時（Raw Writer
排 12:30 UTC 實際 20:38），且各時鐘彼此不知道 DC 盤後是否已寫完。改成「上一步完成才叫下一步」：

  DC 盤後 ALL_DONE ──dispatch──→ 本 workflow（trigger=dc）
      依序 dispatch 並等待完成：
        BC.p bars_daily → BC.p snapshot_daily → BC.p indicators_daily → BC.p commdata_daily → BC raw_writer_daily
      任何一步失敗就停（後面各步自己的原排程仍是保底，且各有「今天跑過就跳過」的 marker）
  凌晨保底排程（trigger=schedule）：DC 已完成、本鏈對該交易日卻沒跑完 → 自己啟動；否則直接結束

指揮放在公開 BC（Actions 免費）：等待期間只花 BC 的時間，不花 BC.p（私有、計費）的分鐘數。
跨倉庫 dispatch／查詢用 BC 既有 GH_TOKEN（CFET fan-out 同一把）；同倉庫用內建 GITHUB_TOKEN（yml 需 actions: write）。
結果寫 StockData.System_State{id:"nightly_chain"}（MongoDB_Standard_v3 已登記）；BC 每日健康報告「夜間鏈」段讀。
試跑（--dry-run）：把各工作的試跑參數一路傳下去（BC.p mode=dry_run、raw_writer write=0），整條鏈演練不寫資料。
⛔ 不印 token。Python 3.9 相容。
"""
import argparse
import datetime as dt
import os
import re
import sys
import time

import pymongo
import pytz
import requests

from tasks.outbound import dispatch_workflow

ET = pytz.timezone("US/Eastern")
API = "https://api.github.com"
POLL_S = 30
FIND_RUN_S = 180                      # dispatch 後最多等幾秒找到對應的 run
# (名稱, 倉庫別名, workflow 檔, 正式參數, 試跑參數, 單步上限分鐘)
STEPS = [
    ("bars_daily",       "BCP", "bars_daily.yml",       {"mode": "full"}, {"mode": "dry_run"}, 90),
    ("snapshot_daily",   "BCP", "snapshot_daily.yml",   {"mode": "full"}, {"mode": "dry_run"}, 75),
    ("indicators_daily", "BCP", "indicators_daily.yml", {"mode": "full"}, {"mode": "dry_run"}, 45),
    ("commdata_daily",   "BCP", "commdata_daily.yml",   {"mode": "full"}, {"mode": "dry_run"}, 30),
    ("raw_writer_daily", "BC",  "raw_writer_daily.yml", {"write": "1"},   {"write": "0"},      165),
]


def _hm():
    return dt.datetime.now(ET).strftime("%H:%M")


def _log(m):
    print(f"[{dt.datetime.now(ET).strftime('%H:%M:%S ET')}] {m}", flush=True)


def _repos():
    return {"BCP": os.getenv("CFET_REPO", "").strip(), "BC": os.getenv("GITHUB_REPOSITORY", "").strip()}


def _token(alias):
    # 跨倉庫（BC.p）用 PAT；同倉庫用內建 GITHUB_TOKEN
    return os.getenv("GH_TOKEN", "").strip() if alias == "BCP" else os.getenv("GITHUB_TOKEN", "").strip()


def _hdr(token):
    return {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"}


def latest_done_date(st_col):
    """DC 盤後最近一次完成的交易日（System_State{id:global} 的 after_hours_done_{date}=True 中最大者）。"""
    g = st_col.find_one({"id": "global"}) or {}
    dates = [k[len("after_hours_done_"):] for k, v in g.items()
             if k.startswith("after_hours_done_") and v and re.fullmatch(r"\d{4}-\d{2}-\d{2}", k[len("after_hours_done_"):])]
    return max(dates) if dates else None


def find_run(repo, wf, token, since_utc):
    """找 dispatch 之後建立的那個 run（同 workflow、event=workflow_dispatch、created_at ≥ since）。"""
    deadline = time.time() + FIND_RUN_S
    while time.time() < deadline:
        try:
            r = requests.get(f"{API}/repos/{repo}/actions/workflows/{wf}/runs",
                             params={"event": "workflow_dispatch", "per_page": 5}, headers=_hdr(token), timeout=20)
            for run in r.json().get("workflow_runs", []):
                created = dt.datetime.strptime(run["created_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
                if created >= since_utc:
                    return run["id"]
        except Exception as e:
            _log(f"查 run 失敗（重試）：{type(e).__name__}")
        time.sleep(10)
    return None


def wait_run(repo, run_id, token, limit_min):
    """等 run 結束，回 conclusion（success/failure/cancelled/timed_out…；逾時回 chain_timeout）。"""
    end = time.time() + limit_min * 60
    while time.time() < end:
        try:
            r = requests.get(f"{API}/repos/{repo}/actions/runs/{run_id}", headers=_hdr(token), timeout=20)
            j = r.json()
            if j.get("status") == "completed":
                return j.get("conclusion") or "unknown"
        except Exception as e:
            _log(f"查狀態失敗（重試）：{type(e).__name__}")
        time.sleep(POLL_S)
    return "chain_timeout"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trigger", default="manual")         # dc／schedule／manual
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--if-needed", action="store_true")    # 保底排程用：DC 未完成或本鏈已跑完就結束
    a = ap.parse_args()

    st_col = pymongo.MongoClient(os.environ["MONGO_URI"])["StockData"]["System_State"]
    date = latest_done_date(st_col)
    prev = st_col.find_one({"id": "nightly_chain"}) or {}
    if a.if_needed:
        if not date:
            _log("DC 盤後沒有任何完成紀錄 → 結束")
            return 0
        if prev.get("date") == date and prev.get("finished") == "done" and not prev.get("dry_run"):
            _log(f"{date} 的夜間鏈已完成（{prev.get('finished_at')}）→ 保底排程結束")
            return 0
        if prev.get("date") == date and prev.get("finished") is None and not prev.get("dry_run"):
            _log(f"{date} 的夜間鏈正在跑（{prev.get('trigger')} 觸發）→ 保底排程不重複啟動")
            return 0

    repos = _repos()
    if not repos["BCP"] or not _token("BCP") or not _token("BC"):
        _log("❌ 缺 CFET_REPO／GH_TOKEN／GITHUB_TOKEN，無法執行")
        return 1

    rec = {"id": "nightly_chain", "date": date, "trigger": a.trigger, "dry_run": a.dry_run,
           "started_at": dt.datetime.now(dt.timezone.utc), "finished": None, "finished_at": None,
           "steps": [{"name": n, "repo": "BC.p" if al == "BCP" else "BC", "run_id": None, "start": None,
                      "end": None, "conclusion": "not_started"} for n, al, *_ in STEPS]}

    def save():
        st_col.update_one({"id": "nightly_chain"}, {"$set": rec}, upsert=True)

    save()
    _log(f"夜間鏈開始：交易日 {date}｜觸發 {a.trigger}｜{'試跑' if a.dry_run else '正式'}")
    for i, (name, alias, wf, inp_real, inp_dry, limit) in enumerate(STEPS):
        step = rec["steps"][i]
        repo, token = repos[alias], _token(alias)
        since = dt.datetime.now(dt.timezone.utc).replace(microsecond=0) - dt.timedelta(seconds=5)
        step["start"] = _hm()
        ok = dispatch_workflow(wf, token=token, repo=repo, ref="main", inputs=inp_dry if a.dry_run else inp_real)
        if not ok:
            step["conclusion"] = "dispatch_failed"
        else:
            run_id = find_run(repo, wf, token, since)
            step["run_id"] = run_id
            save()
            step["conclusion"] = wait_run(repo, run_id, token, limit) if run_id else "run_not_found"
        step["end"] = _hm()
        save()
        _log(f"{name}：{step['conclusion']}（{step['start']}→{step['end']}）")
        if step["conclusion"] != "success":
            rec["finished"], rec["finished_at"] = "failed", _hm()
            save()
            _log(f"⛔ 在 {name} 停下；後面各步由各自原排程保底")
            return 1
    rec["finished"], rec["finished_at"] = "done", _hm()
    save()
    _log("✅ 夜間鏈全部完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
