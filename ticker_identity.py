# -*- coding: utf-8 -*-
"""
ticker_identity.py — 身份時間表（2026-10-18 Jun：預先處理「公司改名」與「代號易主」，例 FB→META、SPCX 前後不同公司）

以「股份」為主鍵，不以代號（10-18 第 2 版）：entity＝FIGI:{share_class_figi}（優先；同一股份跨改名不變、
  不同股份類別不同，例 GOOG／GOOGL 分開）→ 沒有 FIGI 但該 CIK 只有一種股份＝借用那個 FIGI → CIK:{10 碼} → NAME:{名稱}。
  權證、權利、單位、優先股（WARRANT／RIGHT／UNIT／PFD）不進時間表（第 1 版把 SPAC 的 ABC／ABCU／ABCW 算成改名）。
  - 同一 entity 用過多個代號＝改名（FB 已下市紀錄的 CIK＝META 現役的 CIK）
  - 同一代號屬於多個 entity＝易主（舊公司在 delisted_utc 那天結束，之後才是新公司）
登記：MongoDB_Standard_v3 §六-D `swing_ai/identity/{日期}/`。來源：Polygon 現役＋已下市總表、SEC 代號→CIK（經門 courier/）。
讀取方：sector_history.py（板塊歸屬按公司接）；大腦「易主切開」⏳ Jun：先不接。
日誌只印筆數。Python 3.9 相容。
"""
import csv
import datetime as dt
import io
import json
from collections import Counter, defaultdict

REPO = "zhujun0511-AI/ai-telegram-bot-dataset"
OUT_PREFIX = "swing_ai/identity"
FAR = "9999-12-31"


def norm(t):
    return (t or "").strip().upper().replace(".", "-")


SKIP_TYPES = ("WARRANT", "RIGHT", "UNIT", "PFD")


def _cik(r):
    c = str(r.get("cik") or "").strip().lstrip("0")
    return int(c) if c.isdigit() else None


def entity_of(r, cik2figi=None):
    figi = r.get("share_class_figi")
    if figi:
        return "FIGI:" + figi
    c = _cik(r)
    if c is not None and cik2figi and len(cik2figi.get(c, ())) == 1:
        return "FIGI:" + next(iter(cik2figi[c]))       # 該公司只有一種股份 → 借用它的 FIGI（接得上改名）
    if c is not None:
        return "CIK:%010d" % c
    if r.get("composite_figi"):
        return "FIGI:" + r["composite_figi"]
    nm = (r.get("name") or "").strip().upper()
    return ("NAME:" + nm) if nm else None


def build(active_rows, inactive_rows, sec_t2c=None):
    """純函數：Polygon 現役＋已下市列 → (identity 列, renames, reuse, problems)。
    每筆 Polygon 列＝「某代號在某段時間屬於某公司」：已下市列的 end＝delisted_utc；現役列 end＝空（至今）。
    同一代號的多筆按 end 排序，前一筆的 end＝後一筆的 start（易主邊界）。"""
    recs, problems = [], []
    cik2figi = defaultdict(set)
    for r in list(active_rows) + list(inactive_rows):
        if r.get("type") not in SKIP_TYPES and r.get("share_class_figi") and _cik(r) is not None:
            cik2figi[_cik(r)].add(r["share_class_figi"])
    for active, rows in ((True, active_rows), (False, inactive_rows)):
        for r in rows:
            t = norm(r.get("ticker"))
            if not t or r.get("type") in SKIP_TYPES:
                continue
            ent = entity_of(r, cik2figi)
            if ent is None:
                problems.append({"ticker": t, "reason": "無 CIK、無 FIGI、無名稱"})
                continue
            end = None if active else ((r.get("delisted_utc") or "")[:10] or None)
            if not active and not end:
                problems.append({"ticker": t, "reason": "已下市紀錄沒有 delisted_utc"})
                end = "0000-00-00"
            recs.append({"entity": ent, "ticker": t, "end": end, "name": r.get("name"), "active": active,
                         "type": r.get("type"), "cik": _cik(r), "source": "polygon"})
            if active and sec_t2c and t in sec_t2c and _cik(r) is not None and _cik(r) != int(sec_t2c[t]):
                problems.append({"ticker": t, "reason": "Polygon CIK %010d 與 SEC CIK %010d 不一致" % (
                    _cik(r), int(sec_t2c[t]))})
    by_t = defaultdict(list)
    for x in recs:
        by_t[x["ticker"]].append(x)
    ident, reuse = [], []
    for t, xs in by_t.items():
        xs.sort(key=lambda x: x["end"] or FAR)
        merged = []                                    # 同一 entity 連續多筆（例重新上市）併成一段
        for x in xs:
            if merged and merged[-1]["entity"] == x["entity"]:
                merged[-1]["end"] = x["end"]
                merged[-1]["active"] = merged[-1]["active"] or x["active"]
                merged[-1]["name"] = x["name"] or merged[-1]["name"]
            else:
                merged.append(dict(x))
        start = None
        for k, x in enumerate(merged):
            x["start"] = start
            ident.append(x)
            if k + 1 < len(merged):
                y = merged[k + 1]
                reuse.append({"ticker": t, "boundary": x["end"], "entity_before": x["entity"], "name_before": x["name"],
                              "entity_after": y["entity"], "name_after": y["name"]})
            start = x["end"]
    by_e = defaultdict(list)
    for x in ident:
        by_e[x["entity"]].append(x)
    renames = []
    for e, xs in by_e.items():
        tick = sorted(xs, key=lambda x: x["end"] or FAR)
        for a, b in zip(tick, tick[1:]):
            if a["ticker"] != b["ticker"] and a.get("end"):   # 舊代號已結束、之後換新代號＝改名（同時現役不算）
                renames.append({"entity": e, "old_ticker": a["ticker"], "new_ticker": b["ticker"], "date": a["end"],
                                "old_name": a["name"], "new_name": b["name"]})
    return ident, renames, reuse, problems


class Resolver:
    """（代號, 日期）→ entity；entity → 現在（或最後）的代號。日期用 YYYY-MM 或 YYYY-MM-DD 字串比較。"""

    def __init__(self, ident_rows):
        self.by_t = defaultdict(list)
        self.by_e = defaultdict(list)
        for x in ident_rows:
            self.by_t[norm(x["ticker"])].append(x)
            self.by_e[x["entity"]].append(x)
        for v in self.by_t.values():
            v.sort(key=lambda x: x.get("end") or FAR)

    def entity(self, ticker, date):
        """某代號在某天屬於哪家公司：第一筆「end 晚於該天」的紀錄（end 當天已是新公司）。查無＝None。"""
        d = date if len(date) > 7 else date + "-01"
        for x in self.by_t.get(norm(ticker), []):
            if (x.get("end") or FAR) > d:
                return x["entity"]
        return None

    def is_current(self, ticker, entity):
        """這個代號現在仍是這家公司（股份）在用的現役代號嗎。"""
        return any(x["entity"] == entity and not x.get("end") for x in self.by_t.get(norm(ticker), []))

    def current_ticker(self, entity):
        """這家公司「現在」的代號；已下市（最後一筆有 end）＝None——不能回它最後用過的代號，
        否則已下市的舊公司（例 Washington Mutual 的 WM）會被接到現在用同代號的新公司（Waste Management）。"""
        xs = sorted(self.by_e.get(entity, []), key=lambda x: x.get("end") or FAR)
        if not xs or xs[-1].get("end"):
            return None
        return xs[-1]["ticker"]


def main():
    import argparse
    from courier import polygon as PG
    from courier import public_data as PD
    from courier.dataset import RawHF
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--out", default="_identity_out")
    a = ap.parse_args()
    act = PG.sector_fetch_reference_all(_log=print, active=True)
    ina = PG.sector_fetch_reference_all(_log=print, active=False, max_pages=150)
    if act is None or ina is None:
        raise SystemExit("⛔ Polygon 總表讀取失敗")
    t2c = PD.sec_company_tickers() if PD.has_sec_contact() else None
    ident, ren, reu, prob = build(act, ina, t2c)

    def to_csv(rows, cols):
        b = io.StringIO()
        w = csv.DictWriter(b, fieldnames=cols, lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
        return b.getvalue()

    today = dt.date.today().isoformat()
    base = "%s/%s" % (OUT_PREFIX, today)
    hf = RawHF(write=not a.dry_run, repo=REPO, written_by="ticker_identity")
    files = hf.list_files()
    k = 2
    while any(p.startswith(base + "/") for p in files):      # 同日重建不覆蓋舊版
        base = "%s/%s_v%d" % (OUT_PREFIX, today, k)
        k += 1
    hf.add(base + "/identity.csv", to_csv(ident, ["entity", "ticker", "start", "end", "name", "active", "type", "source"]))
    hf.add(base + "/renames.csv", to_csv(ren, ["entity", "old_ticker", "new_ticker", "date", "old_name", "new_name"]))
    hf.add(base + "/reuse.csv", to_csv(reu, ["ticker", "boundary", "entity_before", "name_before", "entity_after",
                                             "name_after"]))
    hf.add(base + "/problems.csv", to_csv(prob, ["ticker", "reason"]))
    man = {"created": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "active_rows": len(act),
           "inactive_rows": len(ina), "identity_rows": len(ident), "entities": len({x["entity"] for x in ident}),
           "renames": len(ren), "reuse": len(reu), "problems": len(prob),
           "entity_kinds": dict(Counter(x["entity"].split(":")[0] for x in ident)), "version": base.split("/")[-1],
           "build": "v2：股份 FIGI 主鍵、排除 WARRANT／RIGHT／UNIT／PFD、同時現役不算改名"}
    hf.add(base + "/manifest.json", json.dumps(man, ensure_ascii=False, indent=1))
    n = hf.flush(a.out, "ticker identity %s" % today)
    print("身份時間表：現役 %d、已下市 %d 筆 → %d 段、%d 家公司｜改名 %d｜易主 %d｜問題 %d｜寫出 %d 個檔（%s）" % (
        len(act), len(ina), len(ident), man["entities"], len(ren), len(reu), len(prob), n, base), flush=True)


if __name__ == "__main__":
    main()
