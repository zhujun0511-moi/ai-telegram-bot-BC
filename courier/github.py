# -*- coding: utf-8 -*-
"""
courier/github.py — BC 對 GitHub API 的讀取（2026-10-16「唯一的門」）

來源（逐字搬來，行為不變）：
  actions_minutes_watch.py：try_billing_api、_iter_runs（用 GH_TOKEN）
  nightly_chain.py：find_run、wait_run、跨倉庫權杖選擇（BC.p 用 GH_TOKEN、同倉庫用 GITHUB_TOKEN）
觸發 workflow_dispatch 仍在 courier/outbound.dispatch_workflow。Python 3.9 相容。
"""
import datetime as dt
import os
import time

import requests

GH_API = "https://api.github.com"


# ── actions_minutes_watch 用（GH_TOKEN）──

def has_token() -> bool:
    return bool(os.getenv("GH_TOKEN", "").strip())


def _token() -> str:
    return os.getenv("GH_TOKEN", "").strip()


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {_token()}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def try_billing_api(owner: str):
    """帳號 Actions billing。回 (used, included, paid) 或 None（無權限/失敗→降級）。"""
    try:
        r = requests.get(f"{GH_API}/users/{owner}/settings/billing/actions",
                         headers=_headers(), timeout=15)
        if r.status_code != 200:
            print(f"[billing] HTTP {r.status_code}（多半是 token 無 user scope）→ 降級估算")
            return None
        j = r.json()
        return (j.get("total_minutes_used"), j.get("included_minutes"),
                j.get("total_paid_minutes_used"))
    except Exception as e:
        print(f"[billing] 例外 {e} → 降級估算")
        return None


def iter_runs(repo: str, since_date: dt.date):
    """yield repo 在 since_date(含)之後建立的 workflow run（分頁）。"""
    page = 1
    since_iso = since_date.isoformat()
    while page <= 20:   # 安全上限
        try:
            r = requests.get(f"{GH_API}/repos/{repo}/actions/runs",
                             headers=_headers(),
                             params={"per_page": 100, "page": page,
                                     "created": f">={since_iso}"},
                             timeout=20)
        except Exception as e:
            print(f"[runs] {repo} 例外 {e}")
            return
        if r.status_code != 200:
            print(f"[runs] {repo} HTTP {r.status_code}")
            return
        runs = r.json().get("workflow_runs", [])
        if not runs:
            return
        for run in runs:
            yield run
        if len(runs) < 100:
            return
        page += 1


# ── nightly_chain 用（依倉庫別名選權杖）──

def chain_repos():
    return {"BCP": os.getenv("CFET_REPO", "").strip(), "BC": os.getenv("GITHUB_REPOSITORY", "").strip()}


def chain_token(alias):
    # 跨倉庫（BC.p）用 PAT；同倉庫用內建 GITHUB_TOKEN
    return os.getenv("GH_TOKEN", "").strip() if alias == "BCP" else os.getenv("GITHUB_TOKEN", "").strip()


def chain_has_token(alias) -> bool:
    return bool(chain_token(alias))


def _hdr(token):
    return {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"}


def find_run(repo, wf, alias, since_utc, find_run_s, log):
    """找 dispatch 之後建立的那個 run（同 workflow、event=workflow_dispatch、created_at ≥ since）。"""
    token = chain_token(alias)
    deadline = time.time() + find_run_s
    while time.time() < deadline:
        try:
            r = requests.get(f"{GH_API}/repos/{repo}/actions/workflows/{wf}/runs",
                             params={"event": "workflow_dispatch", "per_page": 5}, headers=_hdr(token), timeout=20)
            for run in r.json().get("workflow_runs", []):
                created = dt.datetime.strptime(run["created_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
                if created >= since_utc:
                    return run["id"]
        except Exception as e:
            log(f"查 run 失敗（重試）：{type(e).__name__}")
        time.sleep(10)
    return None


def wait_run(repo, run_id, alias, limit_min, poll_s, log):
    """等 run 結束，回 conclusion（success/failure/cancelled/timed_out…；逾時回 chain_timeout）。"""
    token = chain_token(alias)
    end = time.time() + limit_min * 60
    while time.time() < end:
        try:
            r = requests.get(f"{GH_API}/repos/{repo}/actions/runs/{run_id}", headers=_hdr(token), timeout=20)
            j = r.json()
            if j.get("status") == "completed":
                return j.get("conclusion") or "unknown"
        except Exception as e:
            log(f"查狀態失敗（重試）：{type(e).__name__}")
        time.sleep(poll_s)
    return "chain_timeout"


def dispatch_chain_step(wf, alias, repo, inputs, ref="main"):
    """跨倉庫／鏈步 dispatch（權杖在門內依別名選）。nightly_chain 原呼叫帶 ref="main"；
    run_cfet_fanout 原呼叫不帶 ref（＝GITHUB_REF_NAME 或 main）→ 傳 ref=None。"""
    from courier.outbound import dispatch_workflow
    return dispatch_workflow(wf, token=chain_token(alias), repo=repo, ref=ref, inputs=inputs)
