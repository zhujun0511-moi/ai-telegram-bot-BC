"""
poly_release_lock.py — 一次性工具，強制釋放卡住的鎖（2026-07-17新增）

背景：GHA job被人工cancel（或平台強制砍job）時，Python行程被直接殺掉，
run_poly_backfill_compare.py 的 `finally: db.release_lock(...)` 不會有
機會執行，鎖會卡在Mongo裡（is_running=True）直到staleness threshold
（poly_fetch_lock 6.5h / poly_compare_lock 1.5h）才會被下一輪搶鎖時
判定孤兒自動接管。2026-07-17 13:02 EST 手動cancel了一輪fetch（run
29597576713，處理到CCB才被砍），poly_fetch_lock因此卡住，導致後續
立刻想重觸發的fetch直接「搶不到鎖」跳過——要等到staleness threshold
才會自動解除，但那已經過了當天16:00 cutoff，等於當天完全用不到。

用法：LOCK_ID環境變數指定要釋放哪個鎖（poly_fetch_lock或poly_compare_
lock），直接把該鎖文件的is_running設為False。不比對token——跟正常
release_lock()不同（正常流程比對token只釋放自己持有的鎖），這支工具
是人工介入場景，前提是已經確認這個鎖不是被合法在跑的job持有、是被
cancel/kill留下的孤兒鎖，才手動觸發這個腳本。

Python 3.9 相容。
"""

import os
import sys

import pymongo

MONGO_URI = os.getenv("MONGO_URI", "").strip()
LOCK_ID   = os.getenv("LOCK_ID", "").strip()


def main() -> int:
    if not MONGO_URI or not LOCK_ID:
        print("❌ MONGO_URI/LOCK_ID 未設定")
        return 1

    client = pymongo.MongoClient(MONGO_URI)
    col = client["StockData"]["System_State"]

    before = col.find_one({"id": LOCK_ID})
    print(f"釋放前狀態：{before}")

    if not before:
        print(f"⚠️ 找不到 id={LOCK_ID} 的文件，無需釋放")
        return 0

    result = col.update_one({"id": LOCK_ID}, {"$set": {"is_running": False}})
    print(f"matched={result.matched_count}, modified={result.modified_count}")

    after = col.find_one({"id": LOCK_ID})
    print(f"釋放後狀態：{after}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
