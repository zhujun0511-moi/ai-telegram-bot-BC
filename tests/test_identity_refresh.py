# -*- coding: utf-8 -*-
"""
test_identity_refresh.py — 身份表重建（identity_mpq.plan_refresh）與「不產生中間版」守衛的離線測試，不連網。
（另以真實數據核對過，2026-10-28：①把 v2 藏起來重建，7 個檔與 v2 逐位元組相同；②以重建版為基底再跑 identity_mpq.plan，
 entities 與 v2 相同（MPQ 公司沒重複）、identity 除版本號外相同。）
用法：python tests/test_identity_refresh.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import catalog_publish as CP   # noqa: E402
import identity_mpq as IM      # noqa: E402

PASS, FAIL = [], []


def check(name, ok, info=""):
    (PASS if ok else FAIL).append(name)
    print(("✅ " if ok else "❌ ") + name + ("" if ok else "  " + str(info)))


ID_CSV = "entity,ticker,start,end,name,active,type,source\nFIGI:A,AAA,,,Alpha,True,CS,polygon\n"


def store(extra):
    d = {"swing_ai/identity/2026-10-18_v2/identity.csv": ID_CSV,
         "_catalog/identity/v1/manifest.json": json.dumps({"version": 1, "source": "swing_ai/identity/2026-10-18_v2"}),
         "_catalog/identity/v2/manifest.json": json.dumps({"version": 2, "polygon_base_version": 1,
                                                           "mpq_symbols": "_catalog/_src/mpq_symbols/v1"})}
    d.update(extra)
    return d


# ── poly_source_of ──
S = store({})
check("v2 經 polygon_base_version 找到 Polygon 來源",
      IM.poly_source_of(S.get, json.loads(S["_catalog/identity/v2/manifest.json"])) == "swing_ai/identity/2026-10-18_v2")
check("refresh 版直接用 polygon_source",
      IM.poly_source_of(S.get, {"polygon_source": "swing_ai/identity/2026-10-28", "polygon_base_version": 3}) == "swing_ai/identity/2026-10-28")
check("v1 用 source", IM.poly_source_of(S.get, json.loads(S["_catalog/identity/v1/manifest.json"])) == "swing_ai/identity/2026-10-18_v2")
check("沒 manifest 回 None", IM.poly_source_of(S.get, None) is None)

# ── catalog_publish 守衛 ──
adds, summ = CP.plan(list(S), S.get, "2026-10")
check("已含 MPQ、Polygon 來源未變 → 沿用 v2、不發身份表", not any("_catalog/identity/" in p for p, _ in adds)
      and "沿用 v2" in summ["identity"], summ)

S2 = store({"swing_ai/identity/2026-10-28/identity.csv": ID_CSV})
try:
    CP.plan(list(S2), S2.get, "2026-10")
    check("已含 MPQ、Polygon 來源是新的 → 拒絕單獨發 Polygon 版", False, "沒擋下")
except SystemExit as e:
    check("已含 MPQ、Polygon 來源是新的 → 拒絕單獨發 Polygon 版", "identity_refresh" in str(e), e)

S3 = {"swing_ai/identity/2026-10-18_v2/identity.csv": ID_CSV,
      "swing_ai/identity/2026-10-28/identity.csv": ID_CSV,
      "_catalog/identity/v1/manifest.json": json.dumps({"version": 1, "source": "swing_ai/identity/2026-10-18_v2"})}
adds, summ = CP.plan(list(S3), S3.get, "2026-10")
check("還沒含 MPQ（只有 v1）時照舊可發 Polygon 版", any(p.startswith("_catalog/identity/v2/") for p, _ in adds), summ)

print("\n通過 %d／失敗 %d" % (len(PASS), len(FAIL)))
sys.exit(1 if FAIL else 0)
