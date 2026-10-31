# -*- coding: utf-8 -*-
"""
tools/upload_series_check.py — 把本地端的序列正本價格核對結果上傳私有 dataset（2026-10-30 Jun 拍板範圍）

  D:\\data\\Shared\\附件_MPQ序列正本表價格核對v2_20261030.csv  →  _catalog/_src/series_check/v<N>/{check.csv, manifest.json}
規矩（MongoDB_Standard 10-30 登記）：
- 只含比值、日期、天數，不含價格；動手前先核欄位，有價格欄就停。⛔ 只放私有 dataset。
- 去掉 BOM、換行統一成 LF 後上傳；manifest 同時記來源檔 sha256。
- v<N> 已存在不覆蓋；csv 與 manifest 同一次提交（manifest 排最後）。
本機執行（需本機 HF 憑證，經 courier/）：
    python tools/upload_series_check.py --candidates-version 2 [--src ...] [--dry-run]
"""
import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

REPO = "zhujun0511-AI/ai-telegram-bot-dataset"
DST = "_catalog/_src/series_check"
COLS = ["pair_id", "entity_id", "asset", "sym_a", "sym_b", "check", "common_days", "same_ratio", "junction_ratio",
        "price_basis", "verdict", "median_ratio_b_over_a", "share_days_within_1pct_of_median", "a_date", "b_date"]


def normalize(raw):
    """回 (上傳內容 bytes, 列數)；欄位不對 SystemExit。"""
    text = raw.decode("utf-8-sig").replace("\r\n", "\n")
    rd = csv.DictReader(io.StringIO(text))
    if rd.fieldnames != COLS:
        raise SystemExit("⛔ 欄位不對：%s" % rd.fieldnames)
    return text.encode("utf-8"), sum(1 for _ in rd)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default=r"D:\data\Shared\附件_MPQ序列正本表價格核對v2_20261030.csv")
    ap.add_argument("--candidates-version", type=int, required=True)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    with open(a.src, "rb") as f:
        raw = f.read()
    body, n = normalize(raw)
    from courier.dataset import HealthDataset, upload_local
    files = HealthDataset(REPO).list_paths()
    ver = 1
    while any(p.startswith("%s/v%d/" % (DST, ver)) for p in files):
        ver += 1
    man = {"version": ver, "created": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "candidates_version": a.candidates_version, "source_file": os.path.basename(a.src),
           "source_sha256": hashlib.sha256(raw).hexdigest(),
           "files": {"check.csv": {"sha256": hashlib.sha256(body).hexdigest(), "rows": n}}}
    print("核對通過：v%d、%d 列" % (ver, n), flush=True)
    if a.dry_run:
        print("試跑：不上傳", flush=True)
        return 0
    base = "%s/v%d" % (DST, ver)
    k = upload_local(REPO, [(base + "/check.csv", body),
                            (base + "/manifest.json", json.dumps(man, ensure_ascii=False, indent=1).encode("utf-8"))],
                     "series_check v%d" % ver, "upload_series_check")
    print("寫出 %d 個檔（%s）" % (k, base), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
