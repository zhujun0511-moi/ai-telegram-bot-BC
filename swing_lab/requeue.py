# -*- coding: utf-8 -*-
"""
requeue.py — 擺動點考試佇列消化器的「自我觸發」（2026-10-18 Jun：一次一份卷、考完重啟讓 6 小時計時重來）

swing_exam_queue.yml 最後一步（不論考試成敗都執行）：讀考官留下的旗標
`_code/exam/_queue_next.txt`（1＝佇列還有待考卷）→ 經門 courier/outbound.dispatch_workflow 觸發
swing_exam_queue.yml 自己（同倉庫、GITHUB_TOKEN；workflow_dispatch 是 GitHub 允許自我觸發的例外）。
旗標不存在（考試步驟中途被關）＝不觸發，交給 cron-job.org 每 2 小時備援。佇列內容本身在私有 dataset，本檔看不到。
Python 3.9 相容。
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
FLAG = os.path.join(HERE, "_code", "exam", "_queue_next.txt")
WORKFLOW = "swing_exam_queue.yml"


def main():
    if not os.path.exists(FLAG):
        print("佇列旗標不存在（考試步驟可能中途被關）→ 不自我觸發，等 cron-job.org 備援")
        return
    with open(FLAG, encoding="utf-8") as f:
        more = f.read().strip() == "1"
    if not more:
        print("佇列已空 → 不再觸發")
        return
    sys.path.insert(0, os.path.dirname(HERE))
    from courier.outbound import dispatch_workflow
    ok = dispatch_workflow(WORKFLOW)
    print("還有待考的卷 → 自我觸發 %s：%s" % (WORKFLOW, "成功" if ok else "失敗（等 cron-job.org 備援）"))


if __name__ == "__main__":
    main()
