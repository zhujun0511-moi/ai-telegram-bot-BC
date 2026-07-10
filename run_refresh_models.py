"""
run_refresh_models.py — 免費模型刷新 + BC 排程鏈唯一入口（v1.0，2026-07-09新增）

背景：免費模型清單（CommData.Configs.free_models_registry）過去完全沒有
自動排程觸發，只能靠人工手動呼叫 AC /admin/refresh-free-models，實測發現
上次刷新是5天前——不是 phase_calc/backtest 沒在跑導致的，是壓根沒人排程
去打這個端點。

順便把 BC 原本分散在 bc_backtest_daily.yml / bc_verify_weekend.yml 各自的
獨立 schedule cron 收斂成一個：這支腳本是整條 BC 排程鏈唯一的自動觸發
起點，每天固定跑一次，先刷新模型，再依序 dispatch 下游 workflow。

鏈路：
  bc_refresh_models（本腳本，每日cron，唯一自動觸發源）
    → 刷新 AC 免費模型清單（失敗不阻塞後續 dispatch，只發告警）
    → dispatch bc_backtest_daily.yml（每天都跑，內部自己判斷有沒有欠課）
    → 若今天是週六或週日：額外 dispatch bc_verify_weekend.yml
      （沿用原本「週六本跑+週日兜底」的雙保險設計，verdict 冪等，
      重複觸發不會有副作用）

下游兩個 workflow 保留各自的 workflow_dispatch 觸發（含既有的 chain_next
接力機制），只是拿掉了各自的 schedule。DC 事件驅動觸發 bc_backtest_daily
也是走 workflow_dispatch，不受影響。

Python 3.9 兼容。
"""

import os
import sys
from datetime import datetime

import pytz
import requests

from tasks.outbound import notify as _notify_shared
from tasks.outbound import dispatch_workflow as _dispatch_workflow_shared

EST_TZ = pytz.timezone("US/Eastern")

ANALYSIS_HUB_URL  = os.getenv("ANALYSIS_HUB_URL", "").strip()
WEBHOOK_SECRET    = os.getenv("WEBHOOK_SECRET", "").strip()


def _now_est() -> datetime:
    return datetime.now(EST_TZ)


def _log(msg: str):
    print(f"[{_now_est().strftime('%Y-%m-%d %H:%M:%S EST')}] {msg}", flush=True)


def _notify(msg: str):
    """2026-07-10改用 tasks/outbound.py 統一出口，report_type/行為不變（bc_backtest，失敗只print不重試）。"""
    _notify_shared(msg, report_type="bc_backtest")


def refresh_free_models() -> bool:
    """
    呼叫 AC /admin/refresh-free-models。
    刷新失敗不影響後續 dispatch——模型清單刷新跟排程鏈是兩件獨立的事，
    模型清單一時抓不到新的，不該連帶讓當天的 backtest/verify 都不跑。
    """
    if not ANALYSIS_HUB_URL or not WEBHOOK_SECRET:
        _log("❌ ANALYSIS_HUB_URL/WEBHOOK_SECRET 未設定，無法刷新模型")
        return False
    try:
        resp = requests.get(
            f"{ANALYSIS_HUB_URL.rstrip('/')}/admin/refresh-free-models",
            headers={"x-webhook-secret": WEBHOOK_SECRET},
            timeout=30,
        )
        if resp.status_code == 200:
            body = resp.json()
            _log(
                f"✅ 模型刷新成功: after_filter={body.get('after_filter')} "
                f"missing_active_roles={body.get('missing_active_roles')}"
            )
            missing = body.get("missing_active_roles") or []
            if missing:
                _notify(
                    f"⚠️ [模型刷新] 以下角色刷新後無 active 模型，"
                    f"正在使用寫死保底值：{', '.join(missing)}"
                )
            return True
        _log(f"❌ 模型刷新失敗 {resp.status_code}: {resp.text[:200]}")
        _notify(f"❌ [模型刷新] AC 回應 {resp.status_code}，排程鏈仍會繼續往下走")
        return False
    except Exception as e:
        _log(f"❌ 模型刷新異常: {e}")
        _notify(f"❌ [模型刷新] 呼叫異常: {e}，排程鏈仍會繼續往下走")
        return False


def dispatch_workflow(workflow_file: str) -> bool:
    """2026-07-10改用 tasks/outbound.py 統一出口，同repo dispatch行為不變。"""
    return _dispatch_workflow_shared(workflow_file)


def main() -> int:
    _log("=== BC 排程鏈起點：刷新免費模型 ===")
    refresh_free_models()

    now        = _now_est()
    is_weekend = now.weekday() >= 5   # 5=Sat, 6=Sun

    dispatch_workflow("bc_backtest_daily.yml")

    if is_weekend:
        day_name = "週六" if now.weekday() == 5 else "週日"
        _log(f"今天是{day_name}，額外 dispatch bc_verify_weekend.yml")
        dispatch_workflow("bc_verify_weekend.yml")
    else:
        _log("平日，不觸發 bc_verify_weekend.yml")

    return 0


if __name__ == "__main__":
    sys.exit(main())
