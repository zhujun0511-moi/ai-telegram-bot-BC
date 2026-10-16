# -*- coding: utf-8 -*-
"""
bootstrap.py — 擺動點考官的「啟動器」（公開倉庫裡唯一的程式）

保密（2026-10-16 Jun：涉及 BC 的部分注意保密）：引擎含 Jun 的擺動點規則、考官含進出場邏輯，
都不放公開倉庫。它們打成程式包放在私有 dataset `swing_ai/code/{sha256}.zip`；
本程式只負責：下載程式包 → 驗指紋（檔名＝內容 sha256）→ 解壓 → 執行考官。指紋不符＝不執行。
用法：python bootstrap.py <exam.py 的參數…>（例：train topic01_base --fetch-pack latest --procs 4）
可用環境變數 SWING_CODE_BUNDLE 指定程式包指紋（留空＝latest.json 指向的那一包）。
⛔ 不印任何 token。Python 3.9 相容。
"""
import hashlib
import json
import os
import subprocess
import sys
import zipfile

REPO = "zhujun0511-AI/ai-telegram-bot-dataset"
CODE_DIR = "swing_ai/code"
HERE = os.path.dirname(os.path.abspath(__file__))
DEST = os.path.join(HERE, "_code")


def main():
    # 2026-10-16「唯一的門」：下載經 BC 的門（courier/dataset.download_code_bundle，HF_TOKEN 在門內讀）。
    # 本檔在 swing_lab/ 底下執行（working-directory: swing_lab），門在上一層。
    sys.path.insert(0, os.path.dirname(HERE))
    from courier.dataset import download_code_bundle
    want = os.environ.get("SWING_CODE_BUNDLE", "").strip()
    zp, want = download_code_bundle(REPO, CODE_DIR, want)
    h = hashlib.sha256()
    with open(zp, "rb") as f:
        for blk in iter(lambda: f.read(1 << 20), b""):
            h.update(blk)
    if h.hexdigest() != want:
        raise SystemExit("程式包指紋不符，不執行")
    with zipfile.ZipFile(zp) as z:
        z.extractall(DEST)
    print("程式包 %s 已驗證並解壓" % want[:12], flush=True)
    env = dict(os.environ, SWING_CODE_BUNDLE=want)
    r = subprocess.run([sys.executable, "exam.py"] + sys.argv[1:], cwd=os.path.join(DEST, "exam"), env=env)
    sys.exit(r.returncode)


if __name__ == "__main__":
    main()
