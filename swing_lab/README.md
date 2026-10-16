# swing_lab — 擺動點考官（只有啟動器）

保密設計（2026-10-16）：引擎含擺動點規則、考官含進出場邏輯，**都不放在這個公開倉庫**。

- `bootstrap.py`：從私有 dataset 下載程式包 `swing_ai/code/{sha256}.zip` → 驗指紋 → 解壓到 `_code/` → 執行考官。
- 配方、打包數據、成績也都在私有 dataset（`swing_ai/strategies/`、`swing_ai/packs/`、`swing_ai/results/`）。
- 鎖箱考試只准在 GitHub Actions 裡開，開箱先記帳（每份卷只考一次）。
- GitHub 日誌只印筆數，不印任何盈虧。
- 觸發：`.github/workflows/swing_exam.yml`（只有手動觸發）。
