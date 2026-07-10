"""
tasks/outbound.py — BC 統一對外出口（Telegram通知 + GitHub workflow dispatch
+ 跨中心webhook觸發），2026-07-10新增。

背景：比照 DC 2026-07-09 那次「統一對外出口」修復（DC過去4個檔案各自獨立
寫requests.post，只有一處有備援保護，其餘是未受保護的暴露面）。這次盤點
BC發現同一種問題：run_refresh_models.py/run_verify_weekend.py/
run_backtest_daily.py/tasks/phase_calculator.py/run_cfet_fanout.py 五個
檔案各自寫了幾乎一樣的「推送Telegram/觸發下一個workflow」邏輯，收斂到這裡。

⚠️ 範圍界定（用戶2026-07-10拍板）：只收斂「Telegram通知 + workflow
dispatch/跨中心webhook觸發」這一類。HF Dataset存取（run_mp_*.py/
tasks/phase_calculator.py的HF上傳下載）、OpenRouter AI呼叫、MP API下載
（run_mp_fetch.py）性質完全不同（資料管道I/O，不是「對外發信號」），
不強行塞進同一個模組。MP相關腳本目前主動暫停中，本次也刻意不動。

Python 3.9 相容。
"""

import os
import time

import requests


def notify(msg: str, report_type: str = "bc_backtest", retries: int = 1, delay: int = 2) -> bool:
    """
    推送到通訊中心 /comm/send。

    retries=1（預設）對應原本 run_refresh_models.py/run_verify_weekend.py/
    run_backtest_daily.py 的行為（失敗只print，不重試）；
    tasks/phase_calculator.py 原本的 _send_telegram_alert() 有3次重試+2秒
    間隔的較強韌版本，呼叫時傳 retries=3 保留原行為，不強迫統一成同一種
    重試策略（那是連續失敗告警用的，值得更韌一點）。
    """
    comm_hub_url   = os.getenv("COMM_HUB_URL", "").strip()
    webhook_secret = os.getenv("WEBHOOK_SECRET", "").strip()
    if not comm_hub_url:
        print(f"[notify] COMM_HUB_URL 未設定，跳過推送: {msg[:80]}")
        return False

    payload = {"content": msg, "report_type": report_type}
    headers = {"x-webhook-secret": webhook_secret, "Content-Type": "application/json"}

    for attempt in range(retries):
        try:
            resp = requests.post(comm_hub_url, json=payload, headers=headers, timeout=10)
            print(f"[notify] 推送: {resp.status_code}")
            if resp.status_code == 200:
                return True
        except Exception as e:
            print(f"[notify] 推送失敗（第{attempt + 1}次）: {e}")
        if attempt < retries - 1:
            time.sleep(delay)
    return False


def dispatch_workflow(workflow_file: str, token: str = None, repo: str = None,
                       ref: str = None) -> bool:
    """
    透過 GitHub API 觸發 workflow_dispatch。

    預設同repo（不傳token/repo時，用GHA內建的GITHUB_TOKEN/GITHUB_REPOSITORY/
    GITHUB_REF_NAME，yml需permissions: actions: write）——對應
    run_refresh_models.py.dispatch_workflow()/run_backtest_daily.py與
    run_verify_weekend.py的_dispatch_next_workflow()原本的行為。

    傳token/repo時做跨repo dispatch——對應run_cfet_fanout.py用GH_TOKEN
    （PAT）+CFET_REPO 跨repo dispatch到 ai-telegram-bot-BC.p 的場景。
    """
    token = token or os.getenv("GITHUB_TOKEN", "").strip()
    repo  = repo or os.getenv("GITHUB_REPOSITORY", "").strip()
    ref   = ref or os.getenv("GITHUB_REF_NAME", "main").strip()
    if not (token and repo):
        print(f"❌ [dispatch] 缺 token/repo，無法 dispatch {workflow_file}")
        return False
    try:
        resp = requests.post(
            f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/dispatches",
            json={"ref": ref},
            headers={"Authorization": f"Bearer {token}",
                     "Accept": "application/vnd.github+json"},
            timeout=10,
        )
        print(f"→ dispatch {repo}/{workflow_file}: {resp.status_code}")
        return resp.status_code == 204
    except Exception as e:
        print(f"❌ [dispatch] {workflow_file} 異常: {e}")
        return False


def dispatch_next_workflow():
    """
    workflow接力：讀 NEXT_WORKFLOW env var，同repo dispatch。沿用
    run_backtest_daily.py/run_verify_weekend.py既有的_dispatch_next_workflow()
    行為（NEXT_WORKFLOW空值=不接力，不印錯誤，安靜跳過）。
    """
    wf = os.getenv("NEXT_WORKFLOW", "").strip()
    if not wf:
        return
    dispatch_workflow(wf)


def trigger_ac_webhook(path: str, payload: dict) -> bool:
    """
    直接觸發AC某個webhook端點（例如/webhook/weekly，見
    run_verify_weekend.py._trigger_weekly_report()）。ANALYSIS_HUB_URL只存
    根網址，這裡自己拼接路徑（2026-07-07拍板的新慣例）；header用
    WEBHOOK_SECRET（AC是FastAPI，見DANGER_ZONES密鑰header三次拍板，不受DC
    Flask那個底線header被反向代理層丟棄的限制）。
    """
    base           = os.getenv("ANALYSIS_HUB_URL", "").strip()
    webhook_secret = os.getenv("WEBHOOK_SECRET", "").strip()
    if not base:
        print(f"[trigger_ac_webhook] ANALYSIS_HUB_URL 未設定，跳過: {path}")
        return False
    try:
        resp = requests.post(
            f"{base.rstrip('/')}{path}",
            json=payload,
            headers={"WEBHOOK_SECRET": webhook_secret, "Content-Type": "application/json"},
            timeout=10,
        )
        print(f"[trigger_ac_webhook] {path}: {resp.status_code}")
        return resp.status_code < 400
    except Exception as e:
        print(f"[trigger_ac_webhook] {path} 失敗: {e}")
        return False
