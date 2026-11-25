# -*- coding: utf-8 -*-
"""catalog_roster.py 離線測試（假 Polygon、假 dataset；不連網）。用法：python tests/test_catalog_roster.py"""
import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import catalog_roster as CR      # noqa: E402
from catalog_publish import to_csv   # noqa: E402

PASS, FAIL = [], []


def check(name, ok, info=""):
    (PASS if ok else FAIL).append(name)
    print(("✅ " if ok else "❌ ") + name + ("" if ok else "  ← %s" % (info,)))


IDC = CR.ID_COLS


def idr(e, src, sym, frm="", to="", asset="stock"):
    return {"entity_id": e, "source": src, "symbol": sym, "from": frm, "to": to, "evidence": "", "note": "",
            "table_version": "6", "asset": asset if src == "mpq" else ""}


V6 = [idr("FIGI:B1", "mpq", "BKH", "2000-01-03"), idr("FIGI:B1", "polygon", "BKH"),
      idr("MPQ:ABR.D", "mpq", "ABR.D", "2021-06-07"),
      idr("FIGI:L1", "mpq", "LMPX", "2019-12-05"),
      idr("FIGI:O1", "mpq", "OLD-DELISTED", "2010-01-04", "2015-03-02"),
      idr("MPQ:NOPE", "mpq", "NOPE", "2020-01-02"),
      idr("FIGI:X1", "mpq", "XETF", "2018-01-02", asset="etf")]
P1 = [dict(idr("FIGI:B1", "mpq", "BKH", "2000-01-03", "2026-11-16"), action="set_to"),
      dict(idr("FIGI:B1", "polygon", "BKH", "", "2026-11-16"), action="set_to"),
      dict(idr("FIGI:B1", "polygon", "HZN", "2026-11-16"), action="add")]


def poly(t, figi="", cik="", typ="CS", ex="XNYS", market="stocks", delisted=None):
    r = {"ticker": t, "name": t + " Inc", "type": typ, "primary_exchange": ex, "cik": cik, "share_class_figi": figi,
         "composite_figi": "", "market": market}
    if delisted:
        r["delisted_utc"] = delisted + "T00:00:00Z"
    return r


STK = [poly("F%04d" % i, "BBF%04d" % i) for i in range(5000)] + [
    poly("HZN", "B1"), poly("ABRpD", "", "1253986", "PFD"), poly("XETF", "X1", typ="ETF", ex="ARCX")]
OTC = [poly("LMPX", "L1", ex="", market="otc")]
INA = [poly("LMPX", "L1", delisted="2022-08-25"), poly("OLD", "O1", delisted="2015-03-02")]


class FakeRaw:
    def __init__(self, fail=()):
        self.calls, self.fail = [], set(fail)

    def all_pages(self, path, params):
        self.calls.append(dict(params))
        if "market" in params:
            return {("stocks", "true"): STK, ("otc", "true"): OTC, ("stocks", "false"): INA}[(params["market"], params["active"])]
        t = params["ticker"]
        if t in self.fail:
            return None
        if t == "ABRpD" and params["active"] == "true":
            return [poly("ABRpD", "", "1253986", "PFD")]
        return []


class FakeHF:
    def __init__(self, files):
        self.files, self.pending, self.commits = dict(files), [], []

    def list_files(self):
        return set(self.files)

    def read(self, p):
        return self.files.get(p)

    def add(self, p, t):
        self.pending.append((p, t))

    def flush(self, out, note):
        for p, t in self.pending:
            self.files[p] = t
        self.commits.append(note)
        n, self.pending = len(self.pending), []
        return n


def store():
    b, s = "_catalog/identity/v6", "_catalog/splits/v11"
    return {b + "/identity.csv": to_csv(V6, IDC),
            b + "/patches/p1/manifest.json": "{}", b + "/patches/p1/identity_patch.csv": to_csv(P1, ["action"] + IDC),
            s + "/patches/p1/manifest.json": "{}",
            s + "/patches/p1/splits_patch.csv": "action,entity_id,symbol,ex_date\nupdate,FIGI:L1,LMPX,2020-01-01\n",
            "swing_ai/listing_events/events/2026-11-21.csv":
                "kind,symbol,entity,name,type,effective\nnew_listing,ANT,FIGI:A1,Ant,CS,2026-11-21\nnew_listing,XETF,FIGI:X1,X,ETF,2026-11-21\n",
            "_catalog/symbols/v2/manifest.json": json.dumps({"content_sha256": "old"}),
            "_catalog/symbols/v2/mpq.csv": "mpq_symbol,asset,entity_id\nBKH,stock,FIGI:B1\n"}


# 附件套用
rows = CR.apply_identity(V6, [("p1", P1)])
bkh = [r for r in rows if r["symbol"] == "BKH" and r["source"] == "mpq"][0]
check("附件 set_to 改結束日並記附件號", bkh["to"] == "2026-11-16" and bkh["_patch"] == "p1", bkh)
check("附件 add 新增並標 _added", any(r["symbol"] == "HZN" and r["_added"] for r in rows))
try:
    CR.apply_identity(V6, [("p9", [dict(idr("FIGI:Z", "mpq", "ZZZ", "1999-01-01"), action="set_to")])])
    check("set_to 對不到要整份停", False)
except ValueError:
    check("set_to 對不到要整份停", True)

# 一次完整執行（bulk＋逐支查＋發布）
hf = FakeHF(store())
raw = FakeRaw()
now = dt.datetime(2026, 11, 24, 2, 0, tzinfo=dt.timezone.utc)
rc = CR.run(raw, hf, now=now, busy=lambda: None, publish=True, log=lambda *a: None)
check("執行成功", rc == 0)
man = json.loads(hf.files["_catalog/symbols/v3/manifest.json"])
R = {(r["mpq_symbol"], r["origin"]): r for r in CR.parse(hf.files["_catalog/symbols/v3/mpq.csv"])}
check("發 v3、manifest 最後", hf.commits[-1].startswith("發布 v3") and man["identity_version"] == 6, hf.commits)
check("BKH 段結束、實體改用 HZN＝renamed_away", R[("BKH", "mpq")]["market"] == "renamed_away"
      and R[("BKH", "mpq")]["identity_patch"] == "p1", R[("BKH", "mpq")])
check("HZN 以 renamed 一列出現、交易所現役", R[("HZN", "renamed")]["market"] == "exchange", R.get(("HZN", "renamed")))
check("LMPX：OTC，進入日＝最後交易所下市日；拆股附件記 p1",
      R[("LMPX", "mpq")]["market"] == "otc" and R[("LMPX", "mpq")]["market_since"] == "2022-08-25"
      and R[("LMPX", "mpq")]["splits_patches"] == "p1", R[("LMPX", "mpq")])
check("ABR.D：逐支查用 ABRpD 查到，記 Polygon 寫法", R[("ABR.D", "mpq")]["polygon_symbol"] == "ABRpD"
      and R[("ABR.D", "mpq")]["market"] == "exchange" and R[("ABR.D", "mpq")]["check_status"] == "done", R[("ABR.D", "mpq")])
check("OLD-DELISTED：段已結束、已下市", R[("OLD-DELISTED", "mpq")]["market"] == "delisted", R[("OLD-DELISTED", "mpq")])
check("NOPE：查無＝unknown 並寫原因", R[("NOPE", "mpq")]["market"] == "unknown" and "查無" in R[("NOPE", "mpq")]["issue"])
check("新上市 ANT 以 listing 一列出現；已在名冊的 XETF 不重複",
      ("ANT", "listing") in R and ("XETF", "listing") not in R and R[("XETF", "mpq")]["asset"] == "etf")
check("保留 v2 欄位（listing_events._scope 照讀）", all(c in CR.parse(hf.files["_catalog/symbols/v3/mpq.csv"])[0]
                                                   for c in ("mpq_symbol", "asset", "entity_id", "delisted")))
check("上一版是舊格式＝全部算 added", man["changes"] == len(R))
check("manifest 記 Polygon 總表抓取日與筆數（日後可一次找出同批來源）",
      man["polygon_bulk_at"] == "2026-11-24" and man["polygon_rows"] == {"stocks": len(STK), "otc": len(OTC), "inactive": len(INA)}
      and man["inputs"]["identity"] == "v6+p1" and man["inputs"]["splits"] == "v11+p1", man)
check("不含價格欄", not any(c in CR.ROSTER_COLS for c in ("close", "price", "volume")))
st = json.loads(hf.files[CR.WORK + "/state.json"])
check("工作區有進度與統計", st["bulk_at"] == "2026-11-24" and st["stats"]["status_done"] == len(R), st)

# 再跑一次：不重做 bulk、沒有待查、內容沒變不發
n_calls = len(raw.calls)
CR.run(raw, hf, now=now, busy=lambda: None, publish=True, log=lambda *a: None)
check("7 天內不重做 bulk、沒有待查就不打 Polygon", len(raw.calls) == n_calls, raw.calls[n_calls:])
check("內容沒變不發新版", "_catalog/symbols/v4/manifest.json" not in hf.files)

# 讓路：一開始就忙＝什麼都不做
hf2 = FakeHF(store())
check("正式作業在跑＝讓路不寫", CR.run(FakeRaw(), hf2, now=now, busy=lambda: "raw_writer_daily.yml", log=lambda *a: None) == 0
      and not hf2.commits)

# 逐支查失敗：記 failed，不發布（auto 模式、非週日、還有待查）
hf3 = FakeHF(store())
CR.run(FakeRaw(fail={"ABR.D", "NOPE"}), hf3, now=now, busy=lambda: None, log=lambda *a: None)
w = {r["mpq_symbol"]: r for r in CR.parse(hf3.files[CR.WORK + "/roster.csv"])}
check("查詢失敗記 failed、下次重查", w["ABR.D"]["check_status"] == "failed" and w["NOPE"]["check_status"] == "failed", w["ABR.D"])
check("還有待查且非週日＝不發布", not any(p.startswith("_catalog/symbols/v3/") for p in hf3.files))

# 中途讓路：第 20 支前後檢查
many = [idr("MPQ:Q%02d" % i, "mpq", "Q%02d" % i, "2020-01-02") for i in range(45)]
s4 = store()
s4["_catalog/identity/v6/identity.csv"] = to_csv(V6 + many, IDC)
hf4, calls = FakeHF(s4), {"n": 0}


def busy_later():
    calls["n"] += 1
    return "nightly_chain.yml" if calls["n"] >= 2 else None


CR.run(FakeRaw(), hf4, now=now, busy=busy_later, log=lambda *a: None)
w4 = CR.parse(hf4.files[CR.WORK + "/roster.csv"])
left = sum(1 for r in w4 if r["check_status"] == "pending")
check("中途讓路：已查的 %d 支寫回、其餘留 pending" % CR.YIELD_EVERY, left == 45 + 3 - CR.YIELD_EVERY, left)

# 接力狀態（status）與判斷
st = {}
CR.run(FakeRaw(), FakeHF(s4), now=now, busy=lambda: None, budget_min=0, log=lambda *a: None, status=st)
check("到時間上限：status 記 budget_out、left>0、沒讓路", st["budget_out"] and st["left"] > 0 and not st["yielded"], st)
st2 = {}
CR.run(FakeRaw(), FakeHF(store()), now=now, busy=lambda: "nightly_chain.yml", log=lambda *a: None, status=st2)
check("一開始就讓路：status 記 yielded", st2["yielded"] and st2["left"] is None, st2)
st3 = {}
CR.run(FakeRaw(), FakeHF(store()), now=now, busy=lambda: None, publish=True, log=lambda *a: None, status=st3)
check("查完發布：status 記 published、left=0", st3["left"] == 0 and st3["published"].startswith("發布 v3"), st3)

D = CR.chain_decision
check("還有待查、沒讓路、chain>0＝接下一次", D({"left": 9, "yielded": False}, "3", 0) == ("continue", 2))
check("chain 用完＝收尾（開回排程）", D({"left": 9, "yielded": False}, "0", 0) == ("finish", 0))
check("查完＝收尾", D({"left": 0}, 4, 0) == ("finish", 0))
check("讓路＝收尾（不跟正式作業搶）", D({"left": 9, "yielded": True}, 4, 0) == ("finish", 0))
check("失敗（rc≠0）＝收尾，正式排程不會一直停著", D({"left": 9}, 4, 1) == ("finish", 0))
check("沒有狀態檔（腳本中途掛掉）＝收尾", D({}, 4, 0) == ("finish", 0))
check("chain 上限 6", D({"left": 9}, "99", 0) == ("continue", 5))
check("chain 不是數字＝0＝收尾", D({"left": 9}, "abc", 0) == ("finish", 0))
try:
    D({"left": 0}, 0, 0, then_run="raw_writer_daily.yml")
    check("then_run 只收 nightly_chain.yml", False)
except ValueError:
    check("then_run 只收 nightly_chain.yml", True)
check("restore 名單：去空白、去重複", CR.parse_restore(" nightly_chain.yml, bc_tickets.yml,nightly_chain.yml ,")
      == ["nightly_chain.yml", "bc_tickets.yml"])
check("restore 空＝不開任何排程", CR.parse_restore("") == [])
for bad in ("swing_exam_queue.yml", "bc_backtest_daily.yml", "raw_writer_daily.yml;rm", "../x.yml"):
    try:
        CR.parse_restore("bc_tickets.yml," + bad)
        check("restore 拒絕名單外：%s" % bad, False)
    except ValueError:
        check("restore 拒絕名單外：%s" % bad, True)

print("\n%d 通過，%d 失敗" % (len(PASS), len(FAIL)))
sys.exit(1 if FAIL else 0)
