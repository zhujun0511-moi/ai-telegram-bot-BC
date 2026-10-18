"""
courier/ — BC 唯一的門（快遞員）。2026-10-16 Jun 拍板「唯一的門」嚴格版：
所有對外（Mongo、GitHub API、TwelveData、Polygon、HF dataset、comm-hub／AC／DC、鑰匙）只在這裡；
門外腳本拿不到連線物件與鑰匙值。防退化：tools/door_audit.py（DC／AC／BC 同一份），結果必須 0。

  keys.py        鑰匙讀取、scrub()（原 bc_secutil.py）、本機備援鑰匙檔
  mongo.py       通用 Mongo 操作（腳本把查詢條件交給門執行；連線選項 configure）
  verify_db.py   週末核查 VerifyDB（原 run_verify_weekend）
  backtest_db.py 每日回測 BacktestDB／StatsDB（原 tasks/cfet_backtest、tasks/backtest_stats）
  data_core.py   Bars 純核心（五中心 vendored 副本之一，逐字）
  data_map.py    dataset 地圖純核心（vendored，逐字）
  dataset.py     HF dataset 讀寫（raw_writer、dataset 健檢、考試啟動器）
  github.py      GitHub API 讀取（健康報告、夜間鏈）
  twelvedata.py  TwelveData（定稿閘門、raw_writer）
  polygon.py     Polygon（raw_writer、週末核查）
  public_data.py 公開資料唯讀（維基百科 S&P 500 歷史版本、GitHub 成分股 CSV、SEC EDGAR；歷史板塊歸屬用，2026-10-18）
  outbound.py    通知（發送前自己備份 Telegram_Outbound，Jun 10-16 拍板）、dispatch、觸發 DC／AC、外部探測
"""
