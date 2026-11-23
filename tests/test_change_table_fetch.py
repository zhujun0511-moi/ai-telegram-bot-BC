# -*- coding: utf-8 -*-
"""tools/change_table_fetch.py 離線測試（假 Polygon、假 dataset；不連網）。用法：python tests/test_change_table_fetch.py"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools import change_table_fetch as CT      # noqa: E402


def row(t, figi, name="X", ex="XNYS", typ="CS"):
    return {"ticker": t, "name": name, "type": typ, "primary_exchange": ex, "cik": "", "composite_figi": "",
            "share_class_figi": figi, "list_date": ""}


FILL = [row("F%04d" % i, "BBGF%04d" % i) for i in range(5000)]
START = FILL + [row("BKH", "B1"), row("SHNY", "S1"), row("OLD", "O1"), row("GONE", "G1"), row("OUT", "Z1")]
END = FILL + [row("HZN", "B1"), row("GLLD", "S1"), row("NEWT", "O1"), row("IPO", "N1")]


class FakeRaw:
    def __init__(self):
        self.calls = []

    def all_pages(self, path, params):
        self.calls.append((path, dict(params)))
        if path == "/v3/reference/tickers" and "date" in params:
            return START if params["date"] == "2026-10-17" else END
        if path == "/v3/reference/tickers":
            if params["ticker"] == "GONE" and params["active"] == "false":
                return [{"ticker": "GONE", "delisted_utc": "2026-11-05T00:00:00Z"}]
            return []
        if path == "/v3/reference/splits":
            return [{"ticker": "BKH", "execution_date": "2026-11-02", "split_from": 1, "split_to": 2}]
        if path == "/v3/reference/dividends":
            return [{"ticker": "OLD", "ex_dividend_date": "2026-11-03", "dividend_type": "SC", "cash_amount": 9.9},
                    {"ticker": "OLD", "ex_dividend_date": "2026-11-04", "dividend_type": "CD", "cash_amount": 1.0}]
        raise AssertionError(path)

    def _get(self, url, params=None):
        self.calls.append((url, params))
        return {"results": {"composite_figi": "C", "events": [{"type": "ticker_change", "date": "2026-11-16",
                                                                 "ticker_change": {"ticker": "HZN"}}]}}


class FakeHF:
    def __init__(self):
        self.files, self.pending = {}, []

    def add(self, p, t):
        self.pending.append((p, t))

    def flush(self, out, note):
        for p, t in self.pending:
            self.files[p] = t
        n = len(self.pending)
        self.pending = []
        return n


def main():
    raw, hf = FakeRaw(), FakeHF()
    scope = ({"BKH", "SHNY", "OLD", "GONE"}, set(), set())        # OUT 不在清單
    rc = CT.run(raw, hf, scope, "2026-10-17", "2026-11-20", "2026-11-30", "t", log=lambda *a: None)
    assert rc == 0
    P = "swing_ai/_probe/change_table_t/"
    meta = json.loads(hf.files[P + "meta.json"])
    assert all(meta["date_param_check"].values()), meta["date_param_check"]
    ren = hf.files[P + "renamed.csv"]
    assert "BKH,HZN" in ren and "SHNY,GLLD" in ren and "OLD,NEWT" in ren, ren
    gone = hf.files[P + "gone_lookup.csv"]
    assert "GONE,delisted,2026-11-05" in gone and "OUT" not in gone, gone
    assert "HZN" in hf.files[P + "rename_events.csv"]
    divs = hf.files[P + "dividends_nonregular.csv"]
    assert "SC" in divs and "9.9" not in divs and "CD" not in divs.split("\n", 1)[1], divs
    assert "IPO" in hf.files[P + "appeared.csv"]
    # 消失代號不在清單內不逐支查
    assert not any(p == "/v3/reference/tickers" and q.get("ticker") == "OUT" for p, q in raw.calls if isinstance(q, dict))
    # 筆數異常不寫入
    hf2 = FakeHF()
    bad = FakeRaw()
    bad.all_pages = lambda path, params: [] if "date" in params else None
    assert CT.run(bad, hf2, scope, "2026-10-17", "2026-11-20", "2026-11-30", "t", log=lambda *a: None) == 1 and not hf2.files
    print("✅ test_change_table_fetch 全部通過")


if __name__ == "__main__":
    main()
