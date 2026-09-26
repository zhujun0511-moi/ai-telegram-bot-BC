"""
actions_minutes_watch.py — GitHub Actions 私有 repo 分鐘用量監測（2026-09-23 新增）

背景：BC.p(私有 repo)的 Actions 吃計費分鐘，2026-09-23 因本月額度用盡被 GitHub
擋下（job "was not started"）、**事前完全無預警**、整批歸檔 job 靜默失敗。此腳本
每日在 BC(公開/免費)跑一次，估算受監測私有 repo 本計費週期已用分鐘，接近上限就
Telegram 告警，避免再度無聲撞牆。

資料來源（雙保險，自動降級）：
  1. 帳號 billing API（GH_TOKEN 有 user/billing scope 時）＝權威值（含 included/已用/付費）
  2. 退回：加總各私有 repo 的 run 時長（每 run round up 到整分鐘，近似 GitHub 計費）

env：
  GH_TOKEN          讀 run/billing 的 PAT（BC 現有 secret；跨 repo 讀 BC.p 需 repo scope）
  COMM_HUB_URL      告警出口（BC 現有；經 tasks/outbound.notify）
  MINUTES_LIMIT     免費額度（Free 2000 / Pro 3000；預設 2000。billing API 可用時以其 included 為準）
  WARN_PCT          警戒百分比（預設 0.75）
  BILLING_RESET_DAY 帳單週期重置日 1-28（預設 1；僅退回估算用，不知道就填 1）
  WATCH_REPOS       監測的私有 repo，逗號分（預設 zhujun0511-moi/ai-telegram-bot-BC.p）
  ALWAYS_NOTIFY     "1"=每次都推當日用量（預設只在超警戒/疑似被擋才推）

Python 3.9 兼容。
"""
import os
import sys
import math
import datetime as dt

import requests

try:                       # Windows GBK 主控台印 emoji 會崩；GitHub runner 本就 UTF-8
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

GH_API = "https://api.github.com"


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


def cycle_start(reset_day: int) -> dt.date:
    """本計費週期起點(UTC date)。今天日>=reset_day→本月reset_day；否則上月reset_day。"""
    today = dt.datetime.now(dt.timezone.utc).date()
    rd = max(1, min(28, reset_day))
    if today.day >= rd:
        return today.replace(day=rd)
    first = today.replace(day=1)
    prev_last = first - dt.timedelta(days=1)
    return prev_last.replace(day=rd)


def _iter_runs(repo: str, since_date: dt.date):
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


def sum_run_minutes(repo: str, since_date: dt.date):
    """加總 repo 自 since_date 的 run 計費分鐘（每 run round up 整分鐘）。回 (minutes, n_runs)。"""
    total, n = 0, 0
    for run in _iter_runs(repo, since_date):
        started = run.get("run_started_at") or run.get("created_at")
        updated = run.get("updated_at")
        if not (started and updated):
            continue
        try:
            t0 = dt.datetime.fromisoformat(started.replace("Z", "+00:00"))
            t1 = dt.datetime.fromisoformat(updated.replace("Z", "+00:00"))
        except Exception:
            continue
        secs = max(0, (t1 - t0).total_seconds())
        total += int(math.ceil(secs / 60.0))   # GitHub 按整分鐘計費、每 job round up
        n += 1
    return total, n


def detect_block(repo: str):
    """疑似已被擋：最近 15 個 run 裡，有 >=2 個 failure 且時長 <90s（job 根本沒起）。
    回 (suspected: bool, sample: list[str])。"""
    since = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=3)
    fast_fail = []
    cnt = 0
    for run in _iter_runs(repo, since):
        cnt += 1
        if cnt > 15:
            break
        if run.get("conclusion") != "failure":
            continue
        started = run.get("run_started_at") or run.get("created_at")
        updated = run.get("updated_at")
        try:
            t0 = dt.datetime.fromisoformat(started.replace("Z", "+00:00"))
            t1 = dt.datetime.fromisoformat(updated.replace("Z", "+00:00"))
            secs = (t1 - t0).total_seconds()
        except Exception:
            continue
        if secs < 90:
            fast_fail.append(f"{run.get('name','?')}({int(secs)}s)")
    return (len(fast_fail) >= 2, fast_fail[:5])


def main() -> int:
    if not _token():
        print("❌ GH_TOKEN 未設定，無法查詢")
        return 1

    limit   = int(os.getenv("MINUTES_LIMIT", "2000"))
    warn    = float(os.getenv("WARN_PCT", "0.75"))
    reset_d = int(os.getenv("BILLING_RESET_DAY", "1"))
    repos   = [r.strip() for r in os.getenv(
        "WATCH_REPOS", "zhujun0511-moi/ai-telegram-bot-BC.p").split(",") if r.strip()]
    always  = os.getenv("ALWAYS_NOTIFY", "0") == "1"
    owner   = repos[0].split("/")[0]

    lines = []

    # ── 已用分鐘（billing API 優先、否則加總 run）──
    bill = try_billing_api(owner)
    if bill and bill[0] is not None:
        used, included, paid = bill
        if included:
            limit = included
        lines.append(f"帳號（billing API）：已用 {used}/{included} 分，付費額外 {paid}")
    else:
        start = cycle_start(reset_d)
        used = 0
        for repo in repos:
            m, n = sum_run_minutes(repo, start)
            used += m
            lines.append(f"{repo}：~{m} 分 / {n} runs（自 {start} 起）")
        lines.append(f"本週期合計 ~{used} 分（run 時長估算，週期起 {start}）")

    pct = (used / limit) if limit else 0.0

    # ── 近 30 天滾動＝月用量 pace 投影 ──
    start30 = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=30)
    roll = 0
    for repo in repos:
        m, _ = sum_run_minutes(repo, start30)
        roll += m
    lines.append(f"近 30 天滾動 ~{roll} 分（≈ 月投影 pace）")

    # ── BC.p setup_scan 上次自計時（從 FractalRadar.System_State 讀，供「job 跑多久」時間報告）──
    _uri = os.getenv("MONGO_URI", "").strip()
    if _uri:
        try:
            import pymongo
            _st = pymongo.MongoClient(_uri, serverSelectionTimeoutMS=8000)[
                "FractalRadar"]["System_State"].find_one({"_id": "state"})
            if _st and _st.get("setup_scan_at"):
                lines.append(f"CFET setup 上次跑：{_st.get('setup_scan_runtime_sec')}s、"
                             f"watch {_st.get('setup_scan_watch_count')} 支（{_st.get('setup_scan_at')}）")
        except Exception as _e:
            print(f"[setup_runtime] 讀取略過: {_e}")

    # ── 疑似已被擋（快速失敗）──
    blocked_any, samples = False, []
    for repo in repos:
        b, s = detect_block(repo)
        if b:
            blocked_any = True
            samples += [f"{repo}: {', '.join(s)}"]

    over = (pct >= warn) or (roll >= limit) or blocked_any
    if blocked_any:
        head = "🔴🔴 GitHub Actions 疑似已被擋（額度/付款）"
    elif over:
        head = "🔴 GitHub Actions 分鐘接近上限"
    else:
        head = "🟢 GitHub Actions 分鐘用量正常"

    body = "\n".join(lines)
    tail = f"上限 {limit}｜已用 {pct*100:.0f}%｜警戒 {int(warn*100)}%"
    if blocked_any:
        tail += "\n⚠️ 近日多個 run <90s 失敗＝job 沒起，去 GitHub Billing 看付款/額度"
        if samples:
            tail += "\n" + "\n".join(samples)
    msg = f"{head}\n{body}\n{tail}"
    print(msg)

    if over or always:
        try:
            from tasks.outbound import notify
            ok = notify(msg, report_type="bc_backtest")
            print("[notify] 已推送" if ok else "[notify] 未推送（COMM_HUB_URL 未設或推送失敗）")
        except Exception as e:
            print(f"[notify] 失敗（不影響監測本體）: {e}")
    else:
        print("[notify] 未超警戒，不推送")

    return 0


if __name__ == "__main__":
    sys.exit(main())
