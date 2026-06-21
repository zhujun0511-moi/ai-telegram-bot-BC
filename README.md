# ai-telegram-bot-BC

OpenClaw 批次運算中心（Batch Center）。

## 這是什麼

OpenClaw 是一套自動化美股分析告警系統，主體跨四個部署單元：
數據中心（DC）、分析中心（AC）、學習中心（LC）三個 HuggingFace Space，
加上本 repo——專門承接**批次性、CPU 密集、不需常駐對外服務**的運算任務，
透過 GitHub Actions 執行。

HF Space 免費帳戶的 CPU 配額是**全帳號共享**（不是 Space 級別），任何一個
高強度運算任務都可能拖垮帳號下所有 Space。本 repo 的存在就是為了把這類
任務搬離那個共享配額池，讓 DC/AC/LC 維持輕量、常駐、低延遲的服務性質。

完整背景與決策過程見主線項目的
`handoff_phase_calc_migration_v1.md`（子課題 handoff 文件）。

## 目前承接的任務

### phase_calc（相位計算）

學習中心原本的 `/webhook/phase-calc` 端點 + HTTP 自激發機制，現改由本
repo 的 GitHub Actions workflow 驅動：

- `tasks/phase_calculator.py` — 運算邏輯本體（從 LC 完整複製，邏輯未變）
- `run_phase_calc_gha.py` — GitHub Actions 專用入口：MongoDB 層級鎖、
  while 迴圈跑多個 batch、5.5 小時主動逾時收尾
- `.github/workflows/phase_calc.yml` — 每小時整點觸發一次

**一輪 phase_calc 跨多次 job 接續完成是預期行為，不是異常。** 真正的
瓶頸是 HF Dataset commit 速率限制（128次/小時），不是 GitHub Actions
本身的執行時長或額度——8,349 個 ticker 全部跑完一輪本來就需要約
7.5-10 小時，這個耗時跟執行平台無關。

進度與運算結果寫入跟 LC 共用的同一個 MongoDB Atlas（`StockData` 資料庫）
與 HF Dataset，資料完全連續。LC 端 `/status` 端點仍可透過精簡版
`PhaseCalcDB`（`tasks/phase_calc_db.py`，唯讀查詢）查看進度。

## 環境變量 / GitHub Secrets

| 名稱 | 性質 | 用途 |
|---|---|---|
| `MONGO_URI` | Secret | 連接 MongoDB Atlas |
| `HF_TOKEN` | Secret | 讀寫 HuggingFace Dataset |
| `OPENROUTER_API_KEY` | Secret | phase_calc Round 2 起的板塊映射查詢（免費模型，非關鍵依賴，未設定時優雅降級為跳過） |
| `HF_REPO_ID` | 非密鑰 | 直接寫在 workflow yml，預設 `zhujun0511-AI/ai-telegram-bot-dataset` |

## 設計原則（延續主線項目規範）

- **中心獨立部署**：本 repo 不 import DC/AC/LC 任何代碼，共用邏輯一律
  複製，不建立跨 repo 依賴
- **GitHub Actions 沒有對外 HTTP 端點**：所有任務必須是「觸發一次、跑一段、
  寫回 MongoDB/HF、結束」的批次模式，不能假設有常駐服務可以互相呼叫
- **MongoDB 層級鎖**：跨進程/跨 job 的互斥必須透過 MongoDB 條件更新實現，
  Python 進程內鎖（`threading.Lock()`）在這裡完全無效
- **未來擴展**：本 repo 預期會承接更多批次任務（不只 phase_calc），
  新任務應比照 phase_calc 的結構（獨立 workflow + 獨立入口腳本 +
  獨立 MongoDB 鎖文檔），保持任務之間互不干擾

## 不要做的事

- 不要把本 repo 當成常駐服務使用（沒有對外 HTTP 端點，GitHub Actions
  跑完即銷毀容器）
- 不要在運算邏輯（`tasks/` 內各文件）裡引入跨中心 import
- 不要縮短 phase_calc 的 `PHASE_CALC_BATCH_SIZE` 或嘗試突破 HF commit
  128次/小時限制來「加速」——這是刻意維持與原 HF Space 版本一致的節奏，
  詳見 `handoff_phase_calc_migration_v1.md` 的決策記錄
