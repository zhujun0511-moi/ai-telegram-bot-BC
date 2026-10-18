# -*- coding: utf-8 -*-
"""
test_catalog_publish.py — 規則表發布（catalog_publish.py：身份表、板塊表 → _catalog/…/v<N>/）離線測試：
假的 dataset（記憶體裡的檔），不連網。用法：python tests/test_catalog_publish.py
"""
import csv
import hashlib
import io
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import catalog_publish as CP   # noqa: E402

PASS, FAIL = [], []


def check(name, ok, info=""):
    (PASS if ok else FAIL).append(name)
    print(("✅ " if ok else "❌ ") + name + ("" if ok else "  " + str(info)))


def rows(text):
    return list(csv.DictReader(io.StringIO(text)))


IDV = "2026-10-18_v2"
ident = ("entity,ticker,start,end,name,active,type,source\n"
         "FIGI:META,FB,,2022-06-09,Facebook,False,CS,polygon\n"
         "FIGI:META,META,,,Meta Platforms,True,CS,polygon\n"
         "CIK:WAMU,WM,,2008-09-26,Washington Mutual,False,CS,polygon\n"
         "CIK:WASTE,WM,,,Waste Management,True,CS,polygon\n"
         "FIGI:AAA,AAA,,,Aaa Corp,True,CS,polygon\n")
reuse = ("ticker,boundary,entity_before,name_before,entity_after,name_after\n"
         "WM,2008-09-26,CIK:WAMU,Washington Mutual,CIK:WASTE,Waste Management\n")
renames = "entity,old_ticker,new_ticker,date,old_name,new_name\nFIGI:META,FB,META,2022-06-09,Facebook,Meta\n"
sector = ("ticker,start,end,xl,source,confidence\n"
          "META,2007-01,2008-08,XLK,sic_map,high\n"
          "META,2008-09,2026-10,XLC,gics,high\n"
          "WM,2007-01,2026-10,XLI,sic_map,high\n"
          "AAA,2007-01,2026-10,,,\n"
          "ZZZ,2010-01,2010-03,XLE,sic_map,medium\n")
store = {
    "swing_ai/identity/2026-10-18/identity.csv": "entity,ticker,start,end,name,active,type,source\n",
    "swing_ai/identity/%s/identity.csv" % IDV: ident,
    "swing_ai/identity/%s/reuse.csv" % IDV: reuse,
    "swing_ai/identity/%s/renames.csv" % IDV: renames,
    "swing_ai/identity/%s/problems.csv" % IDV: "ticker,reason\n",
    "swing_ai/sector_history/2026-10-18_v3/sector_history.csv": sector,
    "swing_ai/sector_history/2026-10-18_v3/manifest.json": json.dumps({"identity_version": IDV}),
    "swing_ai/sector_history/2026-10-18_v3/problems.csv": "ticker,reason\nZZZ,x\n",
}
adds, summ = CP.plan(set(store), store.get, "2026-10")
out = dict(adds)
paths = [p for p, _ in adds]

check("版本排序：_v10 排在 _v2 之後", CP.src_key("2026-10-18_v10") > CP.src_key("2026-10-18_v2"))
check("身份表、板塊表都發布成 v1", "_catalog/identity/v1/manifest.json" in out and "_catalog/sector/v1/manifest.json" in out,
      summ)
check("每版 manifest 排在該版所有檔之後",
      all(paths.index("_catalog/%s/v1/manifest.json" % d) > max(i for i, p in enumerate(paths)
          if p.startswith("_catalog/%s/v1/" % d) and not p.endswith("manifest.json")) for d in ("identity", "sector")))
man = json.loads(out["_catalog/identity/v1/manifest.json"])
check("manifest 的 sha256 與檔案內容一致",
      all(hashlib.sha256(out["_catalog/identity/v1/" + k].encode("utf-8")).hexdigest() == v["sha256"]
          for k, v in man["files"].items()))
check("manifest 記來源版本（第 2 版，不是第 1 版）", man["source"] == "swing_ai/identity/" + IDV, man["source"])

idr = rows(out["_catalog/identity/v1/identity.csv"])
check("身份長表：v1 只有 source=polygon", {r["source"] for r in idr} == {"polygon"})
fb = [r for r in idr if r["symbol"] == "FB"][0]
check("FB 與 META 同一家公司", fb["entity_id"] == [r for r in idr if r["symbol"] == "META"][0]["entity_id"])
check("from 空白＝最早可得（不亂填）", fb["from"] == "")
wm = [r for r in idr if r["symbol"] == "WM"]
check("WM 兩段：舊公司 to=2008-09-26、新公司現役", {(r["entity_id"], r["to"]) for r in wm} ==
      {("CIK:WAMU", "2008-09-26"), ("CIK:WASTE", "")}, wm)
check("WM 舊段的 note 寫明易主給 Waste Management", "易主給 Waste Management" in
      [r for r in wm if r["entity_id"] == "CIK:WAMU"][0]["note"])
check("table_version 欄＝1", {r["table_version"] for r in idr} == {"1"})
check("renames.csv 欄名 entity 改成 entity_id", out["_catalog/identity/v1/renames.csv"].startswith("entity_id,"))
ents = {r["entity_id"]: r for r in rows(out["_catalog/identity/v1/entities.csv"])}
check("entities：META 名稱用最近一段、現役；Washington Mutual 已下市",
      ents["FIGI:META"]["name"] == "Meta Platforms" and ents["FIGI:META"]["active"] == "1" and ents["CIK:WAMU"]["active"] == "0")

sec = rows(out["_catalog/sector/v1/sector.csv"])
meta = [r for r in sec if r["entity_id"] == "FIGI:META"]
check("改名不算易主：FB 年代的板塊照常掛 Meta、不標低信心", meta and all(r["confidence"] == "high" for r in meta), meta)
wmo = [r for r in sec if r["entity_id"] == "CIK:WAMU"]
check("WM 易主前（2007-01～2008-09；易主 09-26 在月中，按每月 1 日判斷整月歸舊公司）掛到 Washington Mutual，保留但標 low",
      len(wmo) == 1 and wmo[0]["from"] == "2007-01" and wmo[0]["to"] == "2008-09" and wmo[0]["confidence"] == "low"
      and wmo[0]["note"] == CP.REUSE_NOTE, wmo)
wmn = [r for r in sec if r["entity_id"] == "CIK:WASTE"]
check("WM 易主後（2008-10 起）掛 Waste Management、信心不變",
      len(wmn) == 1 and wmn[0]["from"] == "2008-10" and wmn[0]["confidence"] == "high", wmn)
check("沒有板塊的段不寫進去（AAA）", not any(r["entity_id"] == "FIGI:AAA" for r in sec))
unm = rows(out["_catalog/sector/v1/unmapped.csv"])
check("身份表查無的代號進 unmapped（ZZZ）", [u["ticker"] for u in unm] == ["ZZZ"], unm)
sman = json.loads(out["_catalog/sector/v1/manifest.json"])
check("板塊 manifest 記 identity_version=1、易主低信心段數、略過段數",
      sman["identity_version"] == 1 and sman["low_reuse_segments"] == 1 and sman["skipped_empty_segments"] == 1, sman)

# 第二次：來源沒變 → 不發新版
store2 = dict(store, **out)
adds2, summ2 = CP.plan(set(store2), store2.get, "2026-10")
check("來源沒變就不發新版", adds2 == [] and "沿用 v1" in summ2["identity"] and "沿用 v1" in summ2["sector"], summ2)

# 新的身份來源 → 身份 v2；板塊來源仍用舊身份 → 板塊跳過（不一致）
store3 = dict(store2)
store3["swing_ai/identity/2026-10-19/identity.csv"] = ident
adds3, summ3 = CP.plan(set(store3), store3.get, "2026-10")
p3 = [p for p, _ in adds3]
check("新身份來源 → 發 v2、不動 v1", "_catalog/identity/v2/manifest.json" in p3 and
      not any(p.startswith("_catalog/identity/v1/") for p in p3), p3)
check("板塊用的身份表跟最新身份不一致 → 不發板塊", not any(p.startswith("_catalog/sector/") for p in p3)
      and "不一致" in summ3["sector"], summ3)


class FakeHF:
    def __init__(self, files):
        self.files, self.added = files, []

    def list_files(self):
        return set(self.files)

    def read(self, p):
        return self.files.get(p)

    def add(self, p, t):
        self.added.append(p)

    def flush(self, out, note):
        return len(self.added)


class A:
    out, dry_run = "_x", True


bad = dict(store)
bad["_catalog/identity/v1/identity.csv"] = "x"        # 有殘檔但沒 manifest（未完成的版本）
try:
    CP.main_publish(A(), FakeHF(bad))
    check("目標檔已存在 → 拒絕覆蓋", False, "沒擋")
except SystemExit as e:
    check("目標檔已存在 → 拒絕覆蓋", "不覆蓋" in str(e), e)

print("\n%d 通過，%d 失敗" % (len(PASS), len(FAIL)))
sys.exit(1 if FAIL else 0)
