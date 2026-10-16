# -*- coding: utf-8 -*-
"""
dataset_health_check.py — dataset 資料地圖稽核 + Yahoo(mp_data) vs TD(Mongo) 週對帳（2026-09-05）

設計：handoff_subsystem_dataset_map.md（五、六）。補「Yahoo mp_data 無 repo 側驗證」缺口（紅隊 #6）
      + 把地圖稽核做到「資料級、非只路徑」（紅隊 #8）。取代已刪的 mp_poly_compare_summary.json。

兩塊：
  [audit] 讀 DATA_MAP.json + _map/{product}.json，比對 dataset 實況：
          ① 每 product 的 root / path_template（抽樣 ticker）能不能解到真的存在的檔；
          ② _retired 前綴是否真的沒了（還在＝告警）；
          ③ 未宣告寫入方：provenance.last_written_by ∉ expected_writers → 告警（只提示、不擋）。
  [crosscheck] full_set 對 mp_data/ticker/{t}/d.csv(Yahoo) vs Mongo Bars period=D(TD)：
          取 Yahoo 最新日期、在 Mongo D 找同日 close，差 > 門檻 → 告警。預設抽樣，--full 全掃。

env：MONGO_URI、HF_TOKEN、HF_REPO_ID（預設 zhujun0511-AI/ai-telegram-bot-dataset）。
用法：python dataset_health_check.py [--full] [--sample N] [--tol 0.01]
exit 0=無告警 / 1=有告警。Python 3.9 相容。
"""
import os
import sys
import json
import random

# 2026-10-16「唯一的門」：HF dataset、Mongo、鑰匙（含本機備援鑰匙檔）一律經 courier。
from courier import mongo as M
from courier import keys
from courier.dataset import HealthDataset

HF_REPO_DEFAULT = "zhujun0511-AI/ai-telegram-bot-dataset"
SAMPLE_TICKER = "AAPL"   # audit 抽樣解路徑用（full_set 幾乎必有）


# _load_env／_dl_json／_last_csv_row 已逐字搬進門（2026-10-16）：courier/keys.load_local_env、
# courier/dataset.HealthDataset.dl_json／last_csv_row（token 留在門內）。


def run_audit(ds, have_paths):
    warns = []
    root_idx = ds.dl_json("DATA_MAP.json")
    if not root_idx:
        return ["[audit] ❌ DATA_MAP.json 不存在"]
    products = root_idx.get("products", {})
    print("[audit] products=%s｜_retired=%s" % (list(products.keys()), root_idx.get("_retired")))
    # ③ _retired 真的沒了
    for pref in root_idx.get("_retired", []):
        hit = any(p == pref or p.startswith(pref + "/") for p in have_paths)
        if hit:
            warns.append("[audit] ⚠️ _retired 前綴仍有檔存在: %s" % pref)
    # 逐 product
    for name, mpath in products.items():
        doc = ds.dl_json(mpath)
        if not doc:
            warns.append("[audit] ⚠️ product 小檔缺: %s" % mpath); continue
        tmpl = doc.get("path_template")
        if tmpl:
            try:
                sample = tmpl.format(ticker=SAMPLE_TICKER, period="d")
            except Exception:
                sample = tmpl.replace("{ticker}", SAMPLE_TICKER).replace("{period}", "d")
            # 只有 ticker_bars 這種帶 {ticker} 的才好抽樣驗；signal_snapshots/prompts 抽樣較難，驗 root/前綴存在
            if "{" not in sample:
                if sample not in have_paths:
                    # 對 ticker_bars：sample d 可能真不存在（該票沒 d），放寬成「root 下有檔」
                    root = doc.get("root") or sample.split("/")[0]
                    if not any(p.startswith(root + "/") for p in have_paths):
                        warns.append("[audit] ⚠️ %s：root/路徑解不到任何檔（%s）" % (name, root))
        # ③ 未宣告寫入方
        prov = doc.get("provenance", {})
        lwb = prov.get("last_written_by")
        exp = doc.get("expected_writers", [])
        # 豁免一次性遷移/bootstrap（consolidation_*）——遷移本來就合法碰任何格
        if lwb and exp and not lwb.startswith("consolidation") \
           and not any(lwb.split("(")[0] in e for e in exp):
            warns.append("[audit] ⚠️ %s：last_written_by=%s 不在 expected_writers=%s（未宣告寫入方）"
                         % (name, lwb, exp))
    return warns


def run_crosscheck(ds, full, sample_n, tol):
    warns = []
    cfg = M.find_one("StockData", "Configs", {"type": "ticker_lists"}) or {}
    full_set = sorted({str(t).upper() for t in
                       (cfg.get("full_set") or (cfg.get("lists", {}) or {}).get("full_set") or [])})
    tickers = full_set if full else random.sample(full_set, min(sample_n, len(full_set)))
    print("[crosscheck] Yahoo(mp_data d.csv) vs TD(Mongo D)｜%d 支（tol=%.3f%%）" % (len(tickers), tol * 100))
    checked = flagged = skipped = 0
    for t in sorted(tickers):
        row = ds.last_csv_row("mp_data/ticker/%s/d.csv" % t)
        if not row:
            skipped += 1; continue
        parts = row.split(",")
        if len(parts) < 5:
            skipped += 1; continue
        ydate = parts[0][:10]
        try:
            yclose = float(parts[4])
        except Exception:
            skipped += 1; continue
        doc = M.find_one("StockData", "Bars", {"ticker": t, "period": "D"}, {"bars": 1})
        if not doc or not doc.get("bars"):
            skipped += 1; continue
        mclose = None
        for b in doc["bars"]:
            if str(b.get("t", ""))[:10] == ydate:
                try:
                    mclose = float(b.get("c"))
                except Exception:
                    mclose = None
                break
        if mclose is None or mclose == 0:
            skipped += 1; continue
        diff = abs(yclose - mclose) / mclose
        checked += 1
        if diff > tol:
            flagged += 1
            warns.append("[crosscheck] ⚠️ %s @%s: Yahoo=%.4f vs TD=%.4f 差 %.2f%%"
                         % (t, ydate, yclose, mclose, diff * 100))
    print("[crosscheck] 對到 %d｜告警 %d｜跳過 %d（無共同日/缺檔）" % (checked, flagged, skipped))
    return warns


def main():
    full = "--full" in sys.argv
    tol = 0.01
    sample_n = 40
    if "--tol" in sys.argv:
        tol = float(sys.argv[sys.argv.index("--tol") + 1])
    if "--sample" in sys.argv:
        sample_n = int(sys.argv[sys.argv.index("--sample") + 1])
    # 原 _load_env()：本機備援鑰匙檔（GitHub 上不存在＝空）——2026-10-16 起只在門內讀，這裡只問「有沒有」
    uri_ok = bool(keys.has_raw("MONGO_URI") or keys.local_env_has("MONGO_URI"))
    repo = keys.env_strip("HF_REPO_ID") or keys.local_env_repo_id() or HF_REPO_DEFAULT
    ds = HealthDataset(repo)
    if not ds.ready() or not uri_ok:
        print("缺 HF_TOKEN 或 MONGO_URI"); sys.exit(2)

    have_paths = ds.list_paths()
    M.configure(local_fallback=True, serverSelectionTimeoutMS=20000)   # 原 MongoClient(uri, 20000)；uri＝環境變數或本機備援檔
    M.ping()

    warns = []
    warns += run_audit(ds, have_paths)
    warns += run_crosscheck(ds, full, sample_n, tol)
    M.close_all()

    print("\n" + "=" * 60)
    if warns:
        print("HEALTH CHECK: %d 告警" % len(warns))
        for w in warns:
            print("  " + w)
        sys.exit(1)
    print("HEALTH CHECK: ✅ 無告警")
    sys.exit(0)


if __name__ == "__main__":
    main()
