"""
poly_cleanup_stale.py — 一次性清理腳本（2026-07-17新增，用完即刪，非常駐邏輯）

背景：Poly長尾backfill+MP稽核的Polygon fetch原本用adjusted=true，實測對
頻繁反向分割的仙股（如ADTX）會產生adjustment假影——2年前的舊日期價格被
換算成「以現在股數結構計算的等值價格」，數字誇張到脫離現實（$649,252,800），
跟MP記錄的原始未調整成交價（$14.79）完全不是同一種基準，比較出來的巨大
差距是假影不是MP資料真的壞。已改用adjusted=false重寫 run_poly_backfill_
compare.py 的 _fetch_polygon_daily()。完整根因見 DANGER_ZONES_master.md
「Polygon adjusted=true 對頻繁反向分割的仙股產生的adjustment假影」章節。

本腳本目的：清空舊的、用adjusted=true抓的錯誤基準資料，讓下一輪fetch/
compare重新抓取。fetch/compare的斷點續傳邏輯是靠HF Dataset檔案存在性
判斷「已完成」（見run_poly_backfill_compare.py._list_tickers_with_file()），
舊的錯誤檔案不清掉，新代碼永遠會誤判成「已經做過」而跳過，不會用新參數
重抓。

清理範圍（三類）：
  mp_data/ticker/{ticker}/d-p.csv              （Polygon抓取結果，錯誤基準）
  mp_data/ticker/{ticker}/compare_report.json   （基於錯誤d-p.csv算出的比對）
  mp_poly_compare_summary.json                  （全域彙總，根目錄）
不動 mp_data/ticker/{ticker}/d.csv（MP原始資料）、w.csv/m.csv（MP週月線）——
這些不受影響，本次事故完全侷限在Poly backfill這條線自己新增的檔案。

批次delete：用 create_commit + CommitOperationDelete，每批
DELETE_BATCH_SIZE個操作一個commit，不是逐檔案delete_file（那樣~2,600次
delete會直接撞HF Dataset commit速率上限128次/小時，見DANGER_ZONES
「HF Dataset commit速率」章節既有教訓）。

執行方式：本地跑（需要HF_TOKEN環境變數，讀寫權限）或包成一次性GHA
workflow用既有BC repo secret跑。跑完一次、確認乾淨後這支腳本應該被刪除，
不是留在repo裡常駐的東西。

Python 3.9 相容。
"""

import os
import sys
import time

from huggingface_hub import HfApi
from huggingface_hub import CommitOperationDelete

HF_TOKEN_ENV = os.getenv("HF_TOKEN", "").strip()
HF_REPO_ID   = os.getenv("HF_REPO_ID", "zhujun0511-AI/ai-telegram-bot-dataset").strip()
HF_TICKER_DIR = "mp_data/ticker"
SUMMARY_FILE  = "mp_poly_compare_summary.json"

DELETE_BATCH_SIZE = 200   # 每個commit最多刪幾個檔案，遠低於128次/小時的commit數上限
DRY_RUN = os.getenv("DRY_RUN", "1").strip() != "0"   # 預設乾跑，只列不刪，需明確設DRY_RUN=0才真的刪


def _find_stale_files(api: HfApi) -> list:
    files = api.list_repo_files(repo_id=HF_REPO_ID, repo_type="dataset")
    prefix = HF_TICKER_DIR + "/"
    targets = []
    for f in files:
        if f.startswith(prefix) and (f.endswith("/d-p.csv") or f.endswith("/compare_report.json")):
            targets.append(f)
    if SUMMARY_FILE in files:
        targets.append(SUMMARY_FILE)
    return sorted(targets)


def main() -> int:
    if not HF_TOKEN_ENV:
        print("❌ HF_TOKEN 未設定，無法執行")
        return 1

    api = HfApi(token=HF_TOKEN_ENV)
    targets = _find_stale_files(api)

    dp_count      = sum(1 for f in targets if f.endswith("/d-p.csv"))
    compare_count = sum(1 for f in targets if f.endswith("/compare_report.json"))
    summary_count = sum(1 for f in targets if f == SUMMARY_FILE)

    print(f"🔍 清理目標：共 {len(targets)} 個檔案")
    print(f"   d-p.csv: {dp_count} / compare_report.json: {compare_count} / 全域彙總: {summary_count}")

    if DRY_RUN:
        print("⏭️ DRY_RUN=1（預設），只列出目標不實際刪除。要真的刪除請設 DRY_RUN=0 重跑。")
        for f in targets[:20]:
            print(f"   {f}")
        if len(targets) > 20:
            print(f"   ...（其餘 {len(targets) - 20} 個省略）")
        return 0

    deleted = 0
    for i in range(0, len(targets), DELETE_BATCH_SIZE):
        batch = targets[i:i + DELETE_BATCH_SIZE]
        ops = [CommitOperationDelete(path_in_repo=f) for f in batch]
        api.create_commit(
            repo_id=HF_REPO_ID,
            repo_type="dataset",
            operations=ops,
            commit_message=f"poly cleanup: remove adjusted=true stale data (batch {i // DELETE_BATCH_SIZE + 1})",
        )
        deleted += len(batch)
        print(f"✅ 已刪除 {deleted}/{len(targets)}")
        if i + DELETE_BATCH_SIZE < len(targets):
            time.sleep(2)   # 批次之間小間隔，避免commit過於密集

    print(f"📸 清理完成，共刪除 {deleted} 個檔案")
    return 0


if __name__ == "__main__":
    sys.exit(main())
