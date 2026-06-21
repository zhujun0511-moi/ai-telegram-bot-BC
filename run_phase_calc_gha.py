"""
run_phase_calc_gha.py — phase_calc GitHub Actions 入口 v1.1

職責：
  取代原 HF Space app.py 的 /webhook/phase-calc 端點 + 自激發機制。
  GitHub Actions 沒有「自己」可以打 HTTP 自激發，改為單次 job 內部
  while 迴圈連續跑多個 batch，靠 schedule（每小時一次）觸發下一個 job
  接續未完成的輪次。

與原 HF Space 版本的差異：
  - 進程內鎖（_phase_calc_lock）→ MongoDB 層級鎖（跨進程有效）
  - HTTP 自激發（_self_trigger_phase_calc）→ 本檔案內 while 迴圈
  - 5.5 小時主動逾時收尾（GitHub Actions job 上限 6 小時，留 30 分鐘緩衝）

v1.1 修復（2026-06-21）：
  搶鎖驗證邏輯改用唯一 token 比對，取代原本的時間戳差距比對。
  根因：pymongo 讀回的 datetime 預設不帶時區資訊（且數值為 UTC），
  與寫入時的 EST-aware datetime 比較會產生約 4-5 小時的時區誤判，
  導致搶鎖實際成功卻被誤判為失敗，鎖留在 is_running=True 無人釋放，
  阻塞後續所有排程觸發。已在生產 MongoDB 上實際發生過一次
  （2026-06-21 03:55 UTC 手動觸發測試時發現）。

不改動的部分：
  - run_phase_calc_batch() 內部邏輯完全沿用 tasks/phase_calculator.py，
    本檔案只負責「怎麼呼叫它、呼叫幾次、什麼時候停」
  - PhaseCalcDB 的 Phase_History / Ticker_Sector_Map 等讀寫邏輯不變

Python 3.9 兼容。
"""

import os
import sys
import time
import uuid
from datetime import datetime, timedelta

import pytz

from tasks.phase_calculator import run_phase_calc_batch, PhaseCalcDB

EST_TZ = pytz.timezone("US/Eastern")

# 單次 job 最長運行時間（秒）。GitHub Actions 硬上限 6 小時，
# 留 30 分鐘緩衝，避免被強殺在 HF commit 寫到一半的中間態。
MAX_JOB_SECONDS = 5.5 * 3600

# 鎖佔用視為「失效」的時間門檻（秒）。
# 正常情況 finally 區塊一定會釋放鎖；這個門檻只在 job 異常被砍、
# finally 沒機會執行時，讓下一次排程能夠自行解除死鎖。
# 設為略大於 MAX_JOB_SECONDS，確保不會誤判一個仍在合法運行中的 job。
LOCK_STALE_SECONDS = MAX_JOB_SECONDS + 15 * 60


def _now_est() -> str:
    return datetime.now(EST_TZ).strftime("%Y-%m-%d %H:%M:%S EST")


def _acquire_lock(db: PhaseCalcDB) -> bool:
    """
    嘗試搶佔 MongoDB 層級鎖。

    搶鎖邏輯（原子操作，find_one_and_update）：
      條件：is_running 不存在，或為 False，或鎖已過期（stale）
      動作：設 is_running=True，記錄 lock_acquired_at（人類可讀，僅供查閱）、
            lock_token（本次運行的唯一識別碼，用於後續驗證）

    v1.1 修復：原先用「比對 lock_acquired_at 時間戳差距」判斷是否搶鎖成功，
    但 pymongo 讀回的 datetime 預設不帶時區資訊（且數值為 UTC），與寫入時的
    EST-aware datetime 比較會產生約 4-5 小時的時區誤判，導致搶鎖實際成功
    但被誤判為失敗（鎖留在 True 卻無人釋放）。
    改為唯一 token 比對，不涉及任何時間運算，徹底避開時區陷阱。

    返回 True = 搶鎖成功，可以開始運算
    返回 False = 上一個 job 仍在合法運行中，本次直接退出
    """
    col = db.stock_db["Phase_Calc_Progress"]
    now = datetime.now(EST_TZ)
    stale_before = now - timedelta(seconds=LOCK_STALE_SECONDS)
    token = uuid.uuid4().hex

    col.find_one_and_update(
        {
            "run_id": db.RUN_ID,
            "$or": [
                {"is_running": {"$exists": False}},
                {"is_running": False},
                {"lock_acquired_at": {"$lt": stale_before}},
            ],
        },
        {
            "$set": {
                "is_running": True,
                "lock_acquired_at": now,
                "lock_token": token,
            }
        },
        upsert=True,
    )
    # find_one_and_update 在條件不匹配時不會執行 $set，
    # 重新讀取後比對 lock_token 是否為本次寫入的值，
    # 是 = 搶鎖成功（無論原本是 upsert 新建還是更新既有文檔）；
    # 否 = 條件不匹配，鎖被別人（或仍在合法運行中的舊狀態）持有，搶鎖失敗。
    doc = col.find_one({"run_id": db.RUN_ID})
    return bool(doc) and doc.get("lock_token") == token


def _release_lock(db: PhaseCalcDB):
    """釋放 MongoDB 層級鎖。無論本次運算成功或失敗都必須呼叫（finally）。"""
    col = db.stock_db["Phase_Calc_Progress"]
    col.update_one(
        {"run_id": db.RUN_ID},
        {"$set": {"is_running": False}},
    )


def main() -> int:
    """
    主入口。返回值作為 process exit code：
      0 = 正常結束（含 ALL_ROUNDS_DONE / ALREADY_DONE / 正常逾時收尾）
      1 = 異常（連線失敗、未預期例外）
    GitHub Actions 不需要靠 exit code 判斷是否要重試，排程本來就是
    每小時固定觸發，這裡的 exit code 只影響 workflow run 的成功/失敗標記，
    方便在 GitHub Actions 介面上肉眼看出歷史執行是否正常。
    """
    try:
        db = PhaseCalcDB()
    except Exception as e:
        print(f"[{_now_est()}] ❌ 初始化 PhaseCalcDB 失敗: {e}")
        return 1

    if db.is_done():
        print(f"[{_now_est()}] ✅ 全部輪次已完成（ALL_ROUNDS_DONE），無需執行")
        return 0

    if not _acquire_lock(db):
        print(f"[{_now_est()}] ⏸️ 搶鎖失敗，上一個 job 仍在合法運行中，本次跳過")
        return 0

    print(f"[{_now_est()}] 🔒 搶鎖成功，開始執行 phase_calc")

    start_time = time.monotonic()
    batch_count = 0
    last_status = "UNKNOWN"

    try:
        while True:
            elapsed = time.monotonic() - start_time
            if elapsed >= MAX_JOB_SECONDS:
                print(f"[{_now_est()}] ⏰ 已運行 {elapsed/3600:.2f} 小時，"
                      f"達到主動逾時門檻（{MAX_JOB_SECONDS/3600:.1f} 小時），"
                      f"收尾結束，等待下次排程接續")
                last_status = "TIMEOUT_GRACEFUL_STOP"
                break

            result = run_phase_calc_batch(db)
            status = result.get("status", "UNKNOWN")
            last_status = status
            batch_count += 1

            round_num = result.get("round", "?")
            completed = result.get("completed_count", "?")
            total = result.get("total_tickers", "?")
            print(f"[{_now_est()}] batch #{batch_count} | {status} | "
                  f"R{round_num} | {completed}/{total}")

            if status == "BATCH_DONE":
                # 原 HF 版本這裡會 sleep(2.0) 再自激發；GitHub Actions 是
                # 單進程內迴圈，不需要這個延遲（沒有 HTTP round-trip 開銷），
                # 直接進下一輪迴圈即可。HF commit 速率限制仍由
                # _hf_batch_commit 內部的 429 重試邏輯處理，不需要在這裡額外等待。
                continue

            elif status == "ALL_DONE":
                next_round = result.get("next_round", "?")
                print(f"[{_now_est()}] ✅ 第 {round_num} 輪完成，"
                      f"進入第 {next_round} 輪")
                continue

            elif status == "ALL_ROUNDS_DONE":
                print(f"[{_now_est()}] 🏁 全部輪次相位計算完成（共 {round_num} 輪），停止")
                break

            elif status == "ALREADY_DONE":
                print(f"[{_now_est()}] 相位計算所有輪次已完成，跳過")
                break

            elif status == "COMMIT_FAILED":
                print(f"[{_now_est()}] ⚠️ HF commit 失敗，本次 job 結束，"
                      f"進度未推進，等待下次排程重試")
                break

            elif status == "SCAN_ERROR":
                print(f"[{_now_est()}] ⚠️ HF 目錄掃描失敗，本次 job 結束，"
                      f"等待下次排程重試")
                break

            else:
                print(f"[{_now_est()}] ⚠️ 未預期狀態 {status}，本次 job 結束")
                break

    except Exception as e:
        print(f"[{_now_est()}] ❌ phase_calc 執行異常: {e}")
        last_status = "EXCEPTION"
        return 1

    finally:
        _release_lock(db)
        elapsed_total = time.monotonic() - start_time
        print(f"[{_now_est()}] 🔓 鎖已釋放 | 本次 job 共執行 {batch_count} 個 batch | "
              f"耗時 {elapsed_total/3600:.2f} 小時 | 最終狀態: {last_status}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
