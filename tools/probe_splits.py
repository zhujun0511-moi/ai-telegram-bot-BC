# -*- coding: utf-8 -*-
"""
tools/probe_splits.py — Polygon 拆股記錄覆蓋率實測（2026-10-17，Jun：買 MarketParquet 前確認能否還原真實價格）

查三件事：
  ① 指定股票的拆股記錄（日期、比例）
  ② 已下市股票是否也查得到
  ③ 全市場 2000 年至今的拆股，按年份統計筆數與反向合股比例（看早年有沒有資料）
查詢走 BC 的門（courier/polygon.RawPolygon，金鑰在門內讀；免費方案每 13 秒一次）。
日誌只印拆股日期／比例（公開資訊）與統計數字；完整結果存私有 dataset swing_ai/_probe/。
用法：python tools/probe_splits.py [--tickers NVDA,AAPL,...] [--no-market] [--no-upload]
Python 3.9 相容。
"""
import argparse
import collections
import csv
import datetime as dt
import io
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from courier.polygon import RawPolygon          # noqa: E402
from courier.dataset import RawHF               # noqa: E402
from courier import keys                         # noqa: E402

DEFAULT_TICKERS = "NVDA,AAPL,TSLA,YHOO,SUNW,LEH,WM"   # 後四檔＝已下市且曾拆股（Yahoo 2017、Sun 2010、雷曼 2008、華盛頓互惠 2008）
REPO = keys.env_strip("HF_REPO_ID") or "zhujun0511-AI/ai-telegram-bot-dataset"


def _log(m):
    print(m, flush=True)


def _ratio(s):
    """split_from→split_to：to/from >1＝拆股（1 拆 N），<1＝反向合股。"""
    try:
        return float(s.get("split_to")) / float(s.get("split_from"))
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", default=DEFAULT_TICKERS)
    ap.add_argument("--no-market", action="store_true")
    ap.add_argument("--no-upload", action="store_true")
    a = ap.parse_args()
    pg = RawPolygon(_log)
    out = {"run_at": dt.datetime.utcnow().isoformat() + "Z", "tickers": {}, "market": None}

    # ① ② 指定股票
    for t in [x.strip().upper() for x in a.tickers.split(",") if x.strip()]:
        rows = pg.all_pages("/v3/reference/splits", {"ticker": t, "limit": 1000})
        if rows is None:
            _log("[%s] 查詢失敗" % t)
            out["tickers"][t] = None
            continue
        rows = sorted(rows, key=lambda r: r.get("execution_date", ""))
        out["tickers"][t] = rows
        desc = "、".join("%s %s→%s" % (r.get("execution_date"), r.get("split_from"), r.get("split_to")) for r in rows)
        _log("[%s] %d 筆：%s" % (t, len(rows), desc or "（無）"))

    # ③ 全市場
    if not a.no_market:
        rows = pg.all_pages("/v3/reference/splits", {"execution_date.gte": "2000-01-01", "limit": 1000,
                                                     "sort": "execution_date", "order": "asc"})
        if rows is None:
            _log("[全市場] 查詢失敗")
        else:
            by_year = collections.defaultdict(lambda: [0, 0])
            for r in rows:
                y = (r.get("execution_date") or "????")[:4]
                by_year[y][0] += 1
                q = _ratio(r)
                if q is not None and q < 1:
                    by_year[y][1] += 1
            tickers = {r.get("ticker") for r in rows}
            _log("[全市場] 2000 年至今共 %d 筆、%d 個代號" % (len(rows), len(tickers)))
            for y in sorted(by_year):
                _log("  %s：%d 筆（其中反向合股 %d）" % (y, by_year[y][0], by_year[y][1]))
            out["market"] = {"count": len(rows), "tickers": len(tickers), "by_year": dict(by_year)}
            out["market_rows"] = rows

    if a.no_upload:
        return 0
    hf = RawHF(True, REPO, "tools/probe_splits.py")
    day = dt.date.today().isoformat()
    hf.add("swing_ai/_probe/polygon_splits_%s.json" % day, json.dumps(out, ensure_ascii=False))
    if out.get("market_rows"):
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(["ticker", "execution_date", "split_from", "split_to"])
        for r in out["market_rows"]:
            w.writerow([r.get("ticker"), r.get("execution_date"), r.get("split_from"), r.get("split_to")])
        hf.add("swing_ai/_probe/polygon_splits_market_%s.csv" % day, buf.getvalue())
    n = hf.flush("", "Polygon 拆股記錄覆蓋率實測")
    _log("已上傳私有 dataset swing_ai/_probe/（%d 檔）" % n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
