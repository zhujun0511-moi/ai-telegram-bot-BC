# -*- coding: utf-8 -*-
"""
tools/upload_mpq_symbols.py — 把本地端交付的 MPQ 代號摘要原樣上傳私有 dataset（2026-10-25 Jun：身份表 v2 先上傳 _src 輸入）

  D:\\data\\Shared\\mpq_symbols\\v<N>\\{mpq_symbols.csv, manifest.json}  →  _catalog/_src/mpq_symbols/v<N>/
規矩（MongoDB_Standard §六-D 10-25 登記）：
- 上傳前核 manifest 的 sha256／列數；不一致就停。位元組原樣上傳（sha256 不變）。
- v<N> 已存在不覆蓋；csv 與 manifest 同一次提交（manifest 排最後）。
- 只含代號與日期，不含價量；日誌只印筆數。⛔ 只放私有 dataset。
本機執行（需本機 HF 憑證，經 courier/）：
    python tools/upload_mpq_symbols.py [--src D:\\data\\Shared\\mpq_symbols\\v1] [--dry-run]
"""
import argparse
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

REPO = "zhujun0511-AI/ai-telegram-bot-dataset"
DST = "_catalog/_src/mpq_symbols"
CSV_NAME = "mpq_symbols.csv"


def check(src):
    """回 (版本號, csv 位元組, manifest 位元組)；核不過 SystemExit。"""
    with open(os.path.join(src, "manifest.json"), "rb") as f:
        man_b = f.read()
    with open(os.path.join(src, CSV_NAME), "rb") as f:
        csv_b = f.read()
    man = json.loads(man_b.decode("utf-8"))
    want = man["files"][CSV_NAME]
    if hashlib.sha256(csv_b).hexdigest() != want["sha256"]:
        raise SystemExit("⛔ sha256 與 manifest 不一致")
    rows = csv_b.decode("utf-8").count("\n") - 1
    if rows != int(want["rows"]):
        raise SystemExit("⛔ 列數 %d 與 manifest %s 不一致" % (rows, want["rows"]))
    return int(man["version"]), csv_b, man_b


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default=r"D:\data\Shared\mpq_symbols\v1")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    ver, csv_b, man_b = check(a.src)
    from courier.dataset import HealthDataset, upload_local
    base = "%s/v%d" % (DST, ver)
    files = HealthDataset(REPO).list_paths()
    if any(p.startswith(base + "/") for p in files):
        raise SystemExit("⛔ %s 已存在，不覆蓋" % base)
    print("核對通過：v%d、%d 列" % (ver, csv_b.count(b"\n") - 1), flush=True)
    if a.dry_run:
        print("試跑：不上傳", flush=True)
        return 0
    n = upload_local(REPO, [(base + "/" + CSV_NAME, csv_b), (base + "/manifest.json", man_b)],
                     "mpq_symbols v%d" % ver, "upload_mpq_symbols")
    print("寫出 %d 個檔（%s）" % (n, base), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
