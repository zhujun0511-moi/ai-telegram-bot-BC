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

2026-07-10（同一天，另一輪整頓）：COMM_HUB_URL 統一為根網址慣例，
notify() 原本直接把它當完整 /comm/send URL 打，現在改成拼路徑，比照
DC/AC同一輪的root-URL統一（trigger_ac_webhook()的ANALYSIS_HUB_URL本來
就是07-07就定下的根網址寫法，不受影響）。

Python 3.9 相容。
"""

import os
import time

import requests

# 2026-10-16「唯一的門」：本檔由 tasks/outbound.py 搬進 courier/outbound.py。
from courier.keys import scrub


def notify(msg: str, report_type: str = "bc_backtest", retries: int = 1, delay: int = 2) -> bool:
    """
    推送到通訊中心 /comm/send。

    retries=1（預設）對應原本 run_refresh_models.py/run_verify_weekend.py/
    run_backtest_daily.py 的行為（失敗只print，不重試）；
    tasks/phase_calculator.py 原本的 _send_telegram_alert() 有3次重試+2秒
    間隔的較強韌版本，呼叫時傳 retries=3 保留原行為，不強迫統一成同一種
    重試策略（那是連續失敗告警用的，值得更韌一點）。
    """
    # 2026-08-06 全域密鑰遮蔽（防禦縱深）：msg 可能帶呼叫端傳進的例外訊息，例外的
    # str(e) 可能含密鑰 URL；在唯一對外 Telegram 出口統一遮蔽，保護所有 notify 呼叫端。
    msg = scrub(msg)

    # 2026-10-16 Jun 拍板「發 Telegram 的人自己備份」：送出前先寫 CommData.Telegram_Outbound
    # （欄位比照 AC courier/sender._save_outbound）；備份失敗或沒有 MONGO_URI 只印出、不擋發送。
    _save_outbound(msg, report_type)

    base           = os.getenv("COMM_HUB_URL", "").strip().rstrip("/")
    webhook_secret = os.getenv("WEBHOOK_SECRET", "").strip()
    if not base:
        print(f"[notify] COMM_HUB_URL 未設定，跳過推送: {msg[:80]}")
        return False
    url = f"{base}/comm/send"

    payload = {"content": msg, "report_type": report_type}
    headers = {"x-webhook-secret": webhook_secret, "Content-Type": "application/json"}

    for attempt in range(retries):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=10)
            print(f"[notify] 推送: {resp.status_code}")
            if resp.status_code == 200:
                return True
        except Exception as e:
            print(f"[notify] 推送失敗（第{attempt + 1}次）: {scrub(e)}")
        if attempt < retries - 1:
            time.sleep(delay)
    return False


def dispatch_workflow(workflow_file: str, token: str = None, repo: str = None,
                       ref: str = None, inputs: dict = None) -> bool:
    """
    透過 GitHub API 觸發 workflow_dispatch。

    預設同repo（不傳token/repo時，用GHA內建的GITHUB_TOKEN/GITHUB_REPOSITORY/
    GITHUB_REF_NAME，yml需permissions: actions: write）——對應
    run_refresh_models.py.dispatch_workflow()/run_backtest_daily.py與
    run_verify_weekend.py的_dispatch_next_workflow()原本的行為。

    傳token/repo時做跨repo dispatch——對應run_cfet_fanout.py用GH_TOKEN
    （PAT）+CFET_REPO 跨repo dispatch到 ai-telegram-bot-BC.p 的場景。

    inputs（2026-07-19新增，可選）：透傳給 GitHub workflow_dispatch API 的
    inputs 欄位（目標 workflow 的 yml 必須先宣告對應的 workflow_dispatch.inputs，
    否則 GitHub API 會拒絕）——例如 run_cfet_fanout.py 用它帶
    {"trigger_source": "after_hours"} 告訴 ai-telegram-bot-BC.p 的
    cfet_judge.yml 這次是 after_hours 觸發的。不傳時沿用舊行為（body 只有 ref）。
    """
    token = token or os.getenv("GITHUB_TOKEN", "").strip()
    repo  = repo or os.getenv("GITHUB_REPOSITORY", "").strip()
    ref   = ref or os.getenv("GITHUB_REF_NAME", "main").strip()
    if not (token and repo):
        print(f"❌ [dispatch] 缺 token/repo，無法 dispatch {workflow_file}")
        return False
    body = {"ref": ref}
    if inputs:
        body["inputs"] = inputs
    try:
        resp = requests.post(
            f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/dispatches",
            json=body,
            headers={"Authorization": f"Bearer {token}",
                     "Accept": "application/vnd.github+json"},
            timeout=10,
        )
        print(f"→ dispatch {repo}/{workflow_file}: {resp.status_code}")
        return resp.status_code == 204
    except Exception as e:
        print(f"❌ [dispatch] {workflow_file} 異常: {scrub(e)}")
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
        print(f"[trigger_ac_webhook] {path} 失敗: {scrub(e)}")
        return False


def _save_outbound(content: str, report_type: str, source: str = "BC"):
    """發送前備份到 CommData.Telegram_Outbound（2026-10-16 新增；失敗不阻塞發送）。"""
    try:
        from datetime import datetime
        import pytz
        from courier import mongo
        if not os.getenv("MONGO_URI", ""):
            print("[notify] 無 MONGO_URI，略過 Telegram_Outbound 備份")
            return
        mongo.insert_one("CommData", "Telegram_Outbound", {
            "type":       report_type,
            "content":    content,
            "source":     source,
            "created_at": datetime.now(pytz.timezone("US/Eastern")),
        })
    except Exception as e:
        print(f"[notify] Telegram_Outbound 備份失敗（不影響發送）: {scrub(e)}")


# ─────────────────────────────────────────────
# 以下 2026-10-16 由各腳本原樣搬進門（呼叫端只呼叫函數，鑰匙與網址不離開門）
# ─────────────────────────────────────────────

_UA = {"User-Agent": "python-requests/2.32.3"}


def dc_v3_update(log, dry_run: bool) -> bool:
    """原 td_ready_gate.trigger_dc：觸發 DC /v3-update（HF 剛好重啟可能失敗 → 重試 3 次）。"""
    dc_url = os.getenv("DC_URL", "").strip().rstrip("/") or "https://zhujun0511-ai-ai-telegram-bot-dc.hf.space"
    secret = os.environ.get("WEBHOOK_SECRET", "")
    if dry_run:
        log("（試跑）略過觸發 DC")
        return True
    for i in range(3):
        try:
            r = requests.get(f"{dc_url}/v3-update", params={"mode": "incremental"},
                             headers={**_UA, "x-webhook-secret": secret}, timeout=60)
            log(f"→ DC /v3-update：HTTP {r.status_code} {r.text[:80]}")
            if r.status_code == 200:
                return True
        except Exception as e:
            log(f"⚠️ 觸發 DC 異常 {type(e).__name__}")
        time.sleep(30)
    return False


def send_signal_snapshot_backtest(ticker: str):
    """原 tasks/cfet_backtest._send_signal_snapshot（逐字）：回測用途的中樞神經 Signal Snapshot 落地呼叫，
    deliver_to_telegram 不傳（通訊中心 bc_backtest 預設 False），失敗只印、不影響回測主流程。"""
    base           = os.getenv("COMM_HUB_URL", "").strip().rstrip("/")
    webhook_secret = os.getenv("WEBHOOK_SECRET", "").strip()
    if not base or not webhook_secret:
        return
    try:
        requests.post(
            f"{base}/signal/snapshot",
            json={"ticker": ticker, "triggered_by": "bc_backtest"},
            headers={"WEBHOOK_SECRET": webhook_secret, "Content-Type": "application/json"},
            timeout=10,
        )
    except Exception as e:
        print(f"⚠️ signal_snapshot 呼叫失敗（不影響回測主流程）: {e}")


def probe_comm_hub():
    """原 actions_minutes_watch.gcp_section 的外部探測：GET comm-hub 根網址，回狀態碼或例外名稱。"""
    try:
        return requests.get("https://comm-hub.zhujun.date/", timeout=15).status_code
    except Exception as e:
        return type(e).__name__


def ac_refresh_free_models():
    """原 run_refresh_models.refresh_free_models 的請求部分（逐字：timeout 180）。
    回 None＝ANALYSIS_HUB_URL／WEBHOOK_SECRET 未設定；否則回 requests 回應（例外原樣往上拋）。"""
    base   = os.getenv("ANALYSIS_HUB_URL", "").strip()
    secret = os.getenv("WEBHOOK_SECRET", "").strip()
    if not base or not secret:
        return None
    return requests.get(
        f"{base.rstrip('/')}/admin/refresh-free-models",
        headers={"x-webhook-secret": secret},
        # 2026-09-21：新 refresh 要逐個「金融題實測」~13 模型（併發4、每個上限20s）→ 總耗時可達 ~90s，
        # 30s 會假逾時（AC 其實照樣跑完存+自發告警，不影響功能，但 BC 日誌難看、拿不到結果）。放寬到 180s。
        timeout=180,
    )
