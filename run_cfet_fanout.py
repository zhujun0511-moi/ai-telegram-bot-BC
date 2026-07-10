"""
run_cfet_fanout.py — BC 作為單一fan-out點，把 workflow_dispatch 轉發給
新的 private repo ai-telegram-bot-BC.p 的 cfet_judge.yml。

背景：DC after_hours ALL_DONE 現有的 run_cfet_scan(db)（寫舊
StockData.CFET_States）保留不動；DC 新增 dispatch_bc_cfet_fanout()
會額外呼叫這個repo（BC）既有的 GH_TOKEN/GH_REPO dispatch機制觸發
bc_cfet_fanout.yml，本腳本收到後再往下dispatch BC.p，讓DC不用同時直接
管兩個下游repo的dispatch關係。

複用既有 GH_TOKEN secret（原本用於run_mp_reorganize_wm.py同repo dispatch
mp_nightly.yml，這次是跨repo dispatch，同一組token）；CFET_REPO是新增的
純字串secret，值為目標repo全名。

2026-07-10改用 tasks/outbound.py 統一出口（BC盤點發現11個腳本各自
requests.post，這是其中一個），行為不變。

Python 3.9 相容。
"""

import os
import sys

from tasks.outbound import dispatch_workflow


def main() -> int:
    token = os.getenv("GH_TOKEN", "").strip()
    cfet_repo = os.getenv("CFET_REPO", "").strip()

    if not token or not cfet_repo:
        print("❌ GH_TOKEN 或 CFET_REPO 未設定，無法 fan-out 到 CFET judge repo")
        return 1

    ok = dispatch_workflow("cfet_judge.yml", token=token, repo=cfet_repo)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
