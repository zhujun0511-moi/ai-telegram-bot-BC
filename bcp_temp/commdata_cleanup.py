"""
commdata_cleanup.py — CommData 保留窗清理（Telegram_Outbound / Cross_Center_Relay / AI_Narrative_Failures）。

背景（2026-09-13 查證）：這三個都是「純寫入 log / 已處理殘留」、只增不減、無深讀者：
  - Telegram_Outbound：發送記錄（telegram_sender insert + 回填送達），無 find 讀者。報告內容本身
    另存於 Conversations/Reports，這只是送出 log。→ 留最近 N 天、更舊直接刪（用戶拍板：不歸檔）。
  - Cross_Center_Relay：跨中心備援佇列。poll-relay 只認領 processed=False；processed=True＝已處理完
    的殘留（實測 2105 筆全 processed=True、0 待處理），從不刪 → 累積。→ 刪 processed=True 且過保留窗。
  - AI_Narrative_Failures：AI 綜合失敗的純觀察 log（不做自動重試、無讀者）。→ 留最近 N 天。

⛔ 不碰 Conversations（用戶明確：保留全部對話）。comm-hub 的 cleanup_old_records 是死代碼（從沒被
   呼叫、且不含這三個 collection），本腳本不依賴它、獨立在 BC.p 跑（不碰 comm-hub、失敗隔離）。

刪除用保留窗（不歸檔，用戶拍板：純 log、內容另有留存）。cutoff 用 naive UTC——created_at/processed_at
皆由 comm-hub 以 datetime.now(EST) 寫入（tz-aware）→ pymongo 轉存 UTC → 讀回 naive UTC。delete_many
按 cutoff 天生冪等（重跑刪同一集合、第二次刪 0），故不需 marker。env：MONGO_URI。Python 3.9 相容。
"""
import os
from datetime import datetime, timedelta


def run_commdata_cleanup(comm_db, telegram_keep_days=30, failures_keep_days=30,
                         relay_keep_days=3, execute=True, log=print):
    """回傳 dict 摘要。execute=False 為 dry-run（只 count、不刪）。comm_db: pymongo CommData handle。"""
    now = datetime.utcnow()   # created_at/processed_at 存為 naive UTC，故用 utcnow 比對
    summary = {"executed": execute}

    jobs = [
        ("Telegram_Outbound", {"created_at": {"$lt": now - timedelta(days=telegram_keep_days)}},
         "telegram_outbound", telegram_keep_days),
        ("AI_Narrative_Failures", {"created_at": {"$lt": now - timedelta(days=failures_keep_days)}},
         "ai_narrative_failures", failures_keep_days),
        # Relay：只刪「已處理且過保留窗」的；processed=False（待處理）與近期已處理一律保留
        ("Cross_Center_Relay", {"processed": True, "processed_at": {"$lt": now - timedelta(days=relay_keep_days)}},
         "cross_center_relay", relay_keep_days),
    ]

    for coll, query, key, keep_days in jobs:
        try:
            if execute:
                r = comm_db[coll].delete_many(query)
                n = r.deleted_count
                log("[commdata_cleanup] %s：刪 %d 筆（保留窗 %d 天）" % (coll, n, keep_days))
                summary[key + "_deleted"] = n
            else:
                n = comm_db[coll].count_documents(query)
                log("[commdata_cleanup] DRY-RUN %s：%d 筆待刪（保留窗 %d 天）" % (coll, n, keep_days))
                summary[key + "_would_delete"] = n
        except Exception as e:
            log("[commdata_cleanup] ⚠️ %s 清理異常（跳過）: %s" % (coll, e))
            summary[key + "_error"] = str(e)

    return summary


if __name__ == "__main__":
    import sys
    import json
    import pymongo

    execute = "--execute" in sys.argv

    def _arg_int(flag, default):
        if flag in sys.argv:
            try:
                return int(sys.argv[sys.argv.index(flag) + 1])
            except Exception:
                print("⚠️ %s 需接整數，沿用預設 %d" % (flag, default))
        return default

    tg = _arg_int("--telegram-keep-days", 30)
    fa = _arg_int("--failures-keep-days", 30)
    re_ = _arg_int("--relay-keep-days", 3)

    uri = os.getenv("MONGO_URI", "").strip()
    if not uri:
        print("[commdata_cleanup] 找不到 MONGO_URI"); sys.exit(1)
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=15000)
    client.admin.command("ping")
    res = run_commdata_cleanup(client["CommData"], telegram_keep_days=tg,
                               failures_keep_days=fa, relay_keep_days=re_,
                               execute=execute, log=print)
    print("[commdata_cleanup] summary:", json.dumps(res, ensure_ascii=False, default=str))
    if not execute:
        print("[commdata_cleanup] DRY-RUN；加 --execute 才刪。"
              "（--telegram-keep-days / --failures-keep-days / --relay-keep-days）")
