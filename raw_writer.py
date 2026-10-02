"""
raw_writer.py — dataset「列表內」實際價寫入（2026-10-02 新增，BC）

職責：為 Mongo `Configs.full_set`（扣 delisted）的每支票，在 HF dataset
`mp_data/ticker/{TICKER}/` 寫入：
  d_raw.csv / w_raw.csv / m_raw.csv  ＝實際價（拆股還原、不扣分紅），date,open,high,low,close,volume
  actions.csv                        ＝拆股/分紅事件，date,dividend,split（split＝split_to/split_from，10＝1拆10）
**列表外一律不碰**（歸本地端 local_yahoo_raw，ADR-049）；**h.csv 不碰**（歸 BC.p bars_archive）；
前復權 d/w/m 與其他所有檔不碰。只新增/覆蓋自己的檔，**絕不刪除、絕不 wipe**。

資料來源（2026-10-02 實測）：
  - TD 預設（DC Mongo D）＝拆股調整價（NVDA 2024-06-07 回 120.888）→ 不能直接當實際價的「歷史」
  - TD `adjust=none` ＝實際價（同日 1208.88／量 41,238,600）→ 用於首次補史與拆股後重抓
  - 每日追加：Mongo D 最新收盤 bar 必為實際價（它之後尚無拆股）→ 不額外耗 TD 額度
    防呆：上次寫入後有拆股（Polygon execution_date > last）→ 該段改用 TD adjust=none 重抓
  - actions：Polygon reference splits/dividends（免費、全史）。首次逐票全史；之後查全市場窗口事件

寫入規則：只寫 date ≤ 基準交易日的**前一個交易日**（最新一根盤後尚未定稿，隔天才寫）；只追加、既有行優先（不改已寫的行）；
w_raw 標籤＝該週週五、m_raw 標籤＝每月 1 日（比照 dataset 既有 w.csv/m.csv），**只寫已結束的週/月**。

用法：python raw_writer.py [--write] [--tickers A,B] [--limit N] [--out-dir DIR]
  無 --write＝試跑：照常抓取計算，檔案寫到 --out-dir、不上傳 HF、不改地圖、不寫 Mongo 狀態。

env：MONGO_URI、HF_TOKEN、HF_REPO_ID、TWELVE_KEY、POLYGON_KEY、RAW_SEED_CAP（預設 120）
Python 3.9 相容。設計見 handoff_subsystem_dataset_map.md 2026-10-02 節。
"""
import argparse
import calendar
import csv
import datetime as dt
import io
import os
import sys
import time

import pymongo
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

WRITTEN_BY   = "raw_writer.py"
ROOT         = "mp_data/ticker"
REPO         = os.getenv("HF_REPO_ID", "").strip() or "zhujun0511-AI/ai-telegram-bot-dataset"
SEED_CAP     = int(os.getenv("RAW_SEED_CAP", "120"))
COMMIT_CHUNK = 100
TD_URL       = "https://api.twelvedata.com/time_series"
PG_BASE      = "https://api.polygon.io"
TD_PAUSE     = 8.5      # TD 免費 8 credits/分鐘
PG_PAUSE     = 13.0     # Polygon 免費 5 次/分鐘
PG_GAP_DAYS  = 720      # Polygon 免費方案可查約 2 年內的日線（補 TD 缺日用）
EVENT_WINDOW_DAYS = 14  # 每日查全市場事件的回看窗口（最長拉到 120 天，見 run）
BAR_HEADER   = ["date", "open", "high", "low", "close", "volume"]
ACT_HEADER   = ["date", "dividend", "split"]
UA           = {"User-Agent": "python-requests/2.32.3"}


def _log(msg):
    print(msg, flush=True)


# ══ 純函數（可離線測試）══════════════════════════════════════════════

def fmt_num(x, nd=4):
    v = round(float(x), nd)
    s = repr(v)
    return s[:-2] if s.endswith(".0") else s


def parse_csv(text):
    """CSV 文字 → {key(第一欄): [原字串欄位...]}；保留原字串（既有行優先、不重格式化）。"""
    out = {}
    if not text:
        return out
    rd = csv.reader(io.StringIO(text))
    next(rd, None)
    for row in rd:
        if row and row[0]:
            out[row[0]] = row
    return out


def render_csv(header, rows):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    for k in sorted(rows):
        w.writerow(rows[k])
    return buf.getvalue()


def merge_keep_existing(existing, new):
    """既有行優先：只加入 existing 沒有的 key。回 (merged, 新增數)。"""
    merged = dict(new)
    merged.update(existing)
    return merged, len(set(new) - set(existing))


def bar_row(date, o, h, l, c, v):
    return [date, fmt_num(o), fmt_num(h), fmt_num(l), fmt_num(c), str(int(round(float(v or 0))))]


def td_rows(values, upto):
    """TD time_series values（新在前、adjust=none）→ {date: row}，只留 date ≤ upto。"""
    out = {}
    for v in values or []:
        d = str(v.get("datetime", ""))[:10]
        if not d or d > upto:
            continue
        try:
            out[d] = bar_row(d, v["open"], v["high"], v["low"], v["close"], v.get("volume"))
        except (KeyError, ValueError, TypeError):
            continue
    return out


def mongo_rows(bars, after, upto):
    """Mongo Bars D（{t,o,h,l,c,v}）→ {date: row}，只留 after < date ≤ upto。"""
    out = {}
    for b in bars or []:
        d = str(b.get("t", ""))[:10]
        if not d or d <= after or d > upto:
            continue
        try:
            out[d] = bar_row(d, b["o"], b["h"], b["l"], b["c"], b.get("v"))
        except (KeyError, ValueError, TypeError):
            continue
    return out


def week_label(d):
    """該週週五（比照 dataset 既有 w.csv 標籤）。"""
    x = dt.date.fromisoformat(d)
    return (x + dt.timedelta(days=4 - x.weekday())).isoformat()


def month_label(d):
    return d[:7] + "-01"


def _last_weekday_of_month(label):
    y, m = int(label[:4]), int(label[5:7])
    x = dt.date(y, m, calendar.monthrange(y, m)[1])
    while x.weekday() > 4:
        x -= dt.timedelta(days=1)
    return x.isoformat()


def aggregate(d_rows, kind, ref):
    """d_raw → w_raw/m_raw，**只回已結束的週/月**（ref ≥ 該週週五／該月最後一個平日）。
    假日週/月（週五/月底恰休市）會晚一天才寫，屬可接受的保守延遲。"""
    groups = {}
    for d in sorted(d_rows):
        lab = week_label(d) if kind == "w" else month_label(d)
        groups.setdefault(lab, []).append(d_rows[d])
    out = {}
    for lab, rows in groups.items():
        end = lab if kind == "w" else _last_weekday_of_month(lab)
        if ref < end:
            continue
        o = rows[0][1]
        h = max(float(r[2]) for r in rows)
        l = min(float(r[3]) for r in rows)
        c = rows[-1][4]
        v = sum(int(float(r[5])) for r in rows)
        out[lab] = [lab, o, fmt_num(h), fmt_num(l), c, str(v)]
    return out


def missing_dates(rows, calendar_dates, lo, hi):
    """以基準交易日曆（SPY 日線日期）找 rows 在 [lo, hi] 缺的交易日。"""
    return sorted(d for d in calendar_dates if lo <= d <= hi and d not in rows)


def trim_before(rows, list_date):
    """剪掉上市日前的 bar（TD 常帶上市前一日/前身資料，如 SPCX 06-11 量 0 平盤）。"""
    if not list_date or len(str(list_date)) < 10:
        return rows
    return {d: r for d, r in rows.items() if d >= str(list_date)[:10]}


def actions_rows(splits, dividends):
    """Polygon splits/dividends → {date: [date, dividend, split]}（同日合併；無事件欄＝0）。"""
    acc = {}
    for s in splits or []:
        d = s.get("execution_date")
        f, t = s.get("split_from"), s.get("split_to")
        if not d or not f or not t:
            continue
        acc.setdefault(d, {"div": 0.0, "split": 0.0})["split"] = float(t) / float(f)
    for x in dividends or []:
        d = x.get("ex_dividend_date")
        amt = x.get("cash_amount")
        if not d or amt is None:
            continue
        acc.setdefault(d, {"div": 0.0, "split": 0.0})["div"] += float(amt)
    return {d: [d, fmt_num(v["div"], 6), fmt_num(v["split"], 6)] for d, v in acc.items()}


# ══ 外部 IO ══════════════════════════════════════════════════════════

class TD:
    def __init__(self, key):
        self.key, self.last, self.day_exhausted = key, 0.0, False

    def daily_none(self, ticker, start_date=None, outputsize=5000):
        """TD 1day adjust=none。回 values 或 None（失敗/額度用盡）。"""
        if self.day_exhausted or not self.key:
            return None
        params = {"symbol": ticker, "interval": "1day", "adjust": "none", "outputsize": str(outputsize)}
        if start_date:
            params["start_date"] = start_date
        for attempt in range(5):
            wait = TD_PAUSE - (time.time() - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()
            try:
                r = requests.get(TD_URL, params=params, headers={**UA, "Authorization": "apikey " + self.key}, timeout=40)
                j = r.json()
            except Exception as e:
                _log(f"  ⚠️ [TD] {ticker} 連線異常 {type(e).__name__}")
                time.sleep(10)
                continue
            if j.get("status") == "ok":
                return j.get("values") or []
            msg = str(j.get("message", ""))[:120]
            if j.get("code") == 429 and "day" in msg.lower():
                self.day_exhausted = True
                _log("  ⛔ [TD] 今日額度用盡，停止本輪補史")
                return None
            if j.get("code") == 429:
                _log(f"  ⏳ [TD] {ticker} 每分鐘額度撞車（{msg[:60]}），65 秒後重試")
                time.sleep(65)
                continue
            _log(f"  ⚠️ [TD] {ticker} {j.get('code')} {msg}")
            return None
        return None


class Polygon:
    def __init__(self, key):
        self.key, self.last = key, 0.0

    def _get(self, url, params=None):
        for attempt in range(4):
            wait = PG_PAUSE - (time.time() - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()
            try:
                r = requests.get(url, params=params, headers={**UA, "Authorization": "Bearer " + self.key}, timeout=40)
            except Exception as e:
                _log(f"  ⚠️ [Polygon] 連線異常 {type(e).__name__}")
                time.sleep(15)
                continue
            if r.status_code == 429:
                time.sleep(65)
                continue
            if r.status_code != 200:
                _log(f"  ⚠️ [Polygon] HTTP {r.status_code}")
                return None
            return r.json()
        return None

    def daily_raw(self, ticker, frm, to):
        """Polygon 未調整日線（實際價）→ {date: row}。免費方案只能查約 2 年內。"""
        j = self._get(f"{PG_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{frm}/{to}",
                      {"adjusted": "false", "sort": "asc", "limit": 5000})
        out = {}
        for r in (j or {}).get("results") or []:
            # Polygon 日線 t＝美東午夜＝UTC 04:00（夏令）/05:00（冬令）→ UTC 日期即美東日期（勿固定減 5 小時，夏令會跑到前一天）
            d = dt.datetime.fromtimestamp(r["t"] / 1000, dt.timezone.utc).date().isoformat()
            try:
                out[d] = bar_row(d, r["o"], r["h"], r["l"], r["c"], r.get("v"))
            except (KeyError, ValueError, TypeError):
                continue
        return out

    def all_pages(self, path, params):
        out, url, p = [], PG_BASE + path, dict(params)
        while url:
            j = self._get(url, p)
            if j is None:
                return None
            out += j.get("results") or []
            url, p = j.get("next_url"), None
        return out


class HF:
    def __init__(self, token, write):
        from huggingface_hub import HfApi
        self.token, self.write = token, write
        self.api = HfApi(token=token)
        self.pending = []

    def list_files(self):
        return set(self.api.list_repo_files(repo_id=REPO, repo_type="dataset"))

    def read(self, path):
        from huggingface_hub import hf_hub_download
        try:
            p = hf_hub_download(repo_id=REPO, filename=path, repo_type="dataset", token=self.token)
        except Exception as e:
            if type(e).__name__ in ("EntryNotFoundError",) or "404" in str(e):
                return None
            raise
        with open(p, "r", encoding="utf-8") as f:
            return f.read()

    def add(self, path, text):
        self.pending.append((path, text))

    def flush(self, out_dir, note):
        if not self.pending:
            return 0
        n = len(self.pending)
        if self.write:
            from huggingface_hub import CommitOperationAdd
            ops = [CommitOperationAdd(path_in_repo=p, path_or_fileobj=t.encode("utf-8")) for p, t in self.pending]
            self.api.create_commit(repo_id=REPO, repo_type="dataset", operations=ops,
                                   commit_message=f"{WRITTEN_BY}: {note}")
        else:
            for p, t in self.pending:
                fp = os.path.join(out_dir, p)
                os.makedirs(os.path.dirname(fp), exist_ok=True)
                with open(fp, "w", encoding="utf-8", newline="\n") as f:
                    f.write(t)
        self.pending = []
        return n


# ══ 地圖 descriptor（只宣告本寫入方相關格＋寫入方分工；merge 逐 period 取代，故欄位帶齊）══

PRICE_BASIS = "actual(實際成交價：拆股還原、不扣分紅；⚠️ Yahoo auto_adjust=False 與 TD 預設都做了拆股調整，不是實際價)"
SCOPE_SPLIT = "all：列表內(Mongo full_set)=raw_writer.py(數據中心)；列表外=local_yahoo_raw(本地端)"


def _raw_period(fname, purpose_note):
    return {"file": fname, "source": "列表內 TD adjust=none + Mongo D 追加；列表外 Yahoo + 拆股還原",
            "adjust": "unadjusted", "price_basis": PRICE_BASIS, "purpose": "screen/ai",
            "scope": SCOPE_SPLIT, "cols": BAR_HEADER, "write_mode": "append(只追加、既有行優先)",
            "note": purpose_note}


MAP_DESCRIPTOR = {
    "periods": {
        "d_raw": _raw_period("d_raw.csv", "日線實際價"),
        "w_raw": _raw_period("w_raw.csv", "由 d_raw 聚合；標籤＝該週週五；只寫已結束的週"),
        "m_raw": _raw_period("m_raw.csv", "由 d_raw 聚合；標籤＝每月 1 日；只寫已結束的月"),
        "actions": {"file": "actions.csv", "source": "列表內 Polygon reference；列表外 Yahoo",
                    "purpose": "screen/ai", "scope": SCOPE_SPLIT, "cols": ACT_HEADER,
                    "write_mode": "append(只追加、既有行優先)",
                    "note": "split＝比例（10＝1拆10、0.1＝10併1、無＝0）；dividend＝每股現金；實際價序列在拆股日必然斷崖，不是崩盤"},
    },
    "expected_writers": [
        "local_yahoo(前復權 d/w/m 全市場,回測)",
        "raw_writer.py(實際價 d_raw/w_raw/m_raw + actions;僅列表內=Mongo full_set;TD adjust=none 補史 + Mongo D 追加,Polygon actions)",
        "bars_archive.py(實際價 h 30 分;僅列表內;Mongo H append)",
        "local_yahoo_raw(實際價 h 60 分 + d_raw/w_raw/m_raw + actions;僅列表外 = 常規目錄 − full_set)",
    ],
    "watchlist_source": "Mongo Configs.full_set（列表內/外以寫入當下為準；兩邊都只追加、既有行優先，成員變動交接不互蓋）",
}


# ══ 主流程 ═══════════════════════════════════════════════════════════

def run(args):
    mongo = pymongo.MongoClient(os.environ["MONGO_URI"])
    sd = mongo["StockData"]
    ref_doc = sd["Bars"].find_one({"ticker": "SPY", "period": "D"}, {"bars": {"$slice": 1}})
    ref = str(((ref_doc or {}).get("bars") or [{}])[0].get("t", ""))[:10]
    if not ref:
        _log("❌ 基準 SPY 無日線，中止")
        return 1
    cfg = sd["Configs"].find_one({"type": "ticker_lists"}) or {}
    delisted = {d["ticker"].upper() for d in sd["Ticker_Identity"].find({"delisted": True}, {"ticker": 1})}
    universe = sorted({str(t).upper() for t in cfg.get("full_set", [])} - delisted)
    if args.tickers:
        asked = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        outside = [t for t in asked if t not in universe]
        if args.write and outside:
            _log(f"❌ 正式寫入只允許列表內；{outside} 不在 full_set（列表外歸本地端），中止")
            return 1
        universe = asked
    if args.limit:
        universe = universe[:args.limit]
    _log(f"=== raw_writer | 基準交易日 {ref} | 列表內 {len(universe)} 支 | {'正式寫入' if args.write else '試跑（不上傳）'} ===")

    # 基準交易日曆（SPY 日線日期，供缺日偵測）＋ 上市日（剪掉上市前資料）
    spy = sd["Bars"].find_one({"ticker": "SPY", "period": "D"}, {"bars.t": 1}) or {}
    cal = {str(b.get("t", ""))[:10] for b in spy.get("bars") or [] if str(b.get("t", ""))[:10] <= ref}
    list_dates = {d["ticker"].upper(): d.get("list_date") for d in sd["Ticker_Identity"].find(
        {"list_date": {"$exists": True}}, {"ticker": 1, "list_date": 1})}
    pg_lo = (dt.date.today() - dt.timedelta(days=PG_GAP_DAYS)).isoformat()
    # 只寫到「基準日的前一個交易日」：DC 盤後抓的最新一根尚未定稿（實測 NVDA 2026-10-01 Mongo 量 2,573,606
    # vs 定稿 98,369,800），隔天 DC 全量重刷才定稿 → 最新一天留到下一輪再寫（d_raw 固定晚一天，換取寫入即定稿）。
    cal_sorted = sorted(cal)
    upto = cal_sorted[-2] if len(cal_sorted) >= 2 else ref

    hf = HF(os.environ.get("HF_TOKEN", ""), args.write)
    td = TD(os.environ.get("TWELVE_KEY", ""))
    pg = Polygon(os.environ.get("POLYGON_KEY", ""))
    files = hf.list_files()
    has = lambda t, f: f"{ROOT}/{t}/{f}" in files

    # 先讀既有 d_raw 判斷誰要補史、誰要追加
    existing_d = {}
    for t in universe:
        if has(t, "d_raw.csv"):
            existing_d[t] = parse_csv(hf.read(f"{ROOT}/{t}/d_raw.csv"))
    lasts = [max(v) for v in existing_d.values() if v]
    win_start = min(lasts + [(dt.date.fromisoformat(ref) - dt.timedelta(days=EVENT_WINDOW_DAYS)).isoformat()])
    win_start = max(win_start, (dt.date.fromisoformat(ref) - dt.timedelta(days=120)).isoformat())
    today = dt.date.today().isoformat()

    # 全市場事件窗口（供：拆股防呆＋既有票 actions 追加）
    mkt_splits, mkt_divs = {}, {}
    if existing_d:
        sp = pg.all_pages("/v3/reference/splits", {"execution_date.gt": win_start, "execution_date.lte": today, "limit": 1000}) or []
        dv = pg.all_pages("/v3/reference/dividends", {"ex_dividend_date.gt": win_start, "ex_dividend_date.lte": today, "limit": 1000}) or []
        for s in sp:
            mkt_splits.setdefault(str(s.get("ticker", "")).upper(), []).append(s)
        for x in dv:
            mkt_divs.setdefault(str(x.get("ticker", "")).upper(), []).append(x)
        _log(f"  全市場事件（{win_start} 之後）：拆股 {len(sp)}、分紅 {len(dv)}")

    st = {"seeded": 0, "appended": 0, "split_refetch": 0, "unchanged": 0, "pending_seed": 0,
          "gap_filled": 0, "gaps_unfilled": [], "errors": []}

    def fill_gaps(t, have, new, lo, hi):
        """TD 偶有缺日（實測 MNST 2026-08-10 拆股前一日 TD 兩種口徑皆缺）→ 以 SPY 交易日曆找缺，
        Polygon 未調整日線補（只能補約 2 年內；更早的缺口只記錄）。只補缺、不改已有行。"""
        lo = max(lo, str(list_dates.get(t) or "")[:10] or lo)
        miss = missing_dates({**have, **new}, cal, lo, hi)
        if not miss:
            return
        fillable = [d for d in miss if d >= pg_lo]
        if fillable:
            got = pg.daily_raw(t, fillable[0], fillable[-1])
            for d in fillable:
                if d in got:
                    new[d] = got[d]
                    st["gap_filled"] += 1
        left = [d for d in miss if d not in new]
        if left:
            st["gaps_unfilled"].append(f"{t}:{len(left)}天({left[0]}~{left[-1]})")

    def past_only(rows):
        return {d: r for d, r in rows.items() if d <= upto}   # 未發生的事件（如已公告的未來拆股）不寫
    done_in_chunk = 0
    for t in universe:
        try:
            ex = existing_d.get(t)
            if not ex:   # 不存在或空檔 → 補史（絕不拿 Mongo 的拆股調整價當歷史）
                # ── 首次補史 ──
                if st["seeded"] >= SEED_CAP or td.day_exhausted:
                    st["pending_seed"] += 1
                    continue
                vals = td.daily_none(t)
                if vals is None:
                    st["errors"].append(f"{t}: TD 補史失敗")
                    st["pending_seed"] += 1
                    continue
                d_rows = trim_before(td_rows(vals, upto), list_dates.get(t))
                if d_rows:
                    fill_gaps(t, {}, d_rows, min(d_rows), upto)
                if not d_rows:
                    st["errors"].append(f"{t}: TD 無日線")
                    continue
                sp = pg.all_pages("/v3/reference/splits", {"ticker": t, "limit": 1000}) or []
                dv = pg.all_pages("/v3/reference/dividends", {"ticker": t, "limit": 1000}) or []
                # 既有行優先：若別的寫入方（成員變動交接）已寫過，合併而不覆蓋
                for fname, rows in (("d_raw.csv", d_rows),
                                    ("w_raw.csv", aggregate(d_rows, "w", upto)),
                                    ("m_raw.csv", aggregate(d_rows, "m", upto))):
                    old = parse_csv(hf.read(f"{ROOT}/{t}/{fname}")) if has(t, fname) else {}
                    m2, _ = merge_keep_existing(old, rows)
                    hf.add(f"{ROOT}/{t}/{fname}", render_csv(BAR_HEADER, m2))
                ex_act = parse_csv(hf.read(f"{ROOT}/{t}/actions.csv")) if has(t, "actions.csv") else {}
                act, _ = merge_keep_existing(ex_act, past_only(actions_rows(sp, dv)))
                hf.add(f"{ROOT}/{t}/actions.csv", render_csv(ACT_HEADER, act))
                st["seeded"] += 1
                _log(f"  [{t}] 補史 {len(d_rows)} 根（{min(d_rows)}~{max(d_rows)}）｜事件 {len(act)}")
            else:
                # ── 每日追加 ──
                last = max(ex) if ex else "0000-00-00"
                new = {}
                if last < upto:
                    splits_since = [s for s in mkt_splits.get(t, []) if s.get("execution_date", "") > last]
                    if splits_since:
                        vals = td.daily_none(t, start_date=last, outputsize=500)
                        if vals is None:
                            st["errors"].append(f"{t}: 拆股後 TD 重抓失敗（本輪不追加）")
                            continue
                        new = td_rows(vals, upto)
                        st["split_refetch"] += 1
                    else:
                        doc = sd["Bars"].find_one({"ticker": t, "period": "D"}, {"bars": {"$slice": 40}})
                        mbars = (doc or {}).get("bars") or []
                        oldest = str(mbars[-1].get("t", ""))[:10] if mbars else ""
                        if not mbars or oldest > last:
                            # 中斷超過 Mongo 讀取窗（40 根）→ 中間會缺，改用 TD adjust=none 補整段
                            vals = td.daily_none(t, start_date=last, outputsize=500)
                            if vals is None:
                                st["errors"].append(f"{t}: 缺口補抓 TD 失敗（本輪不追加）")
                                continue
                            new = td_rows(vals, upto)
                        else:
                            new = mongo_rows(mbars, last, upto)
                if last < upto:
                    fill_gaps(t, ex, new, last, upto)
                merged, added = merge_keep_existing(ex, new)
                ev = past_only(actions_rows(mkt_splits.get(t), mkt_divs.get(t)))
                ex_act = parse_csv(hf.read(f"{ROOT}/{t}/actions.csv")) if (ev and has(t, "actions.csv")) else {}
                act, act_added = merge_keep_existing(ex_act, ev) if ev else ({}, 0)
                if not added and not act_added:
                    st["unchanged"] += 1
                    continue
                if added:
                    hf.add(f"{ROOT}/{t}/d_raw.csv", render_csv(BAR_HEADER, merged))
                    for kind, fname in (("w", "w_raw.csv"), ("m", "m_raw.csv")):
                        agg = aggregate(merged, kind, upto)
                        old = parse_csv(hf.read(f"{ROOT}/{t}/{fname}")) if has(t, fname) else {}
                        if set(agg) - set(old):
                            m2, _ = merge_keep_existing(old, agg)
                            hf.add(f"{ROOT}/{t}/{fname}", render_csv(BAR_HEADER, m2))
                    st["appended"] += 1
                if act_added:
                    hf.add(f"{ROOT}/{t}/actions.csv", render_csv(ACT_HEADER, act))
        except Exception as e:
            st["errors"].append(f"{t}: {type(e).__name__}: {str(e)[:100]}")
            continue
        done_in_chunk += 1
        if done_in_chunk >= COMMIT_CHUNK:
            n = hf.flush(args.out_dir, f"列表內實際價 {ref}（{done_in_chunk} 支）")
            _log(f"  ↳ 提交 {n} 檔")
            done_in_chunk = 0
    n = hf.flush(args.out_dir, f"列表內實際價 {ref}（{done_in_chunk} 支）")
    if n:
        _log(f"  ↳ 提交 {n} 檔")

    seeded_total = len(existing_d) + st["seeded"]
    newest = [max(v) for v in existing_d.values() if v]
    summary = {"ref_date": ref, "write_upto": upto, "universe": len(universe), "seeded_total": seeded_total,
               "pending_seed": st["pending_seed"], "seeded_this_run": st["seeded"],
               "appended": st["appended"], "split_refetch": st["split_refetch"],
               "unchanged": st["unchanged"], "gap_filled": st["gap_filled"],
               "gaps_unfilled": st["gaps_unfilled"][:30], "errors": st["errors"][:30],
               "d_raw_newest_min": min(newest) if newest else None}
    _log("=== 摘要 === " + str(summary))

    if args.write:
        import data_map as dm
        dm.update_data_map(REPO, os.environ.get("HF_TOKEN", ""), "ticker_bars", MAP_DESCRIPTOR, WRITTEN_BY,
                           note=f"列表內實際價：補史 {st['seeded']}、追加 {st['appended']}（累計 {seeded_total}/{len(universe)}）")
        sd["System_State"].update_one({"id": "raw_writer_status"},
                                      {"$set": {**summary, "id": "raw_writer_status",
                                                "run_at": dt.datetime.now(dt.timezone.utc)}}, upsert=True)
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="正式寫入 HF＋地圖＋Mongo 狀態（預設試跑）")
    ap.add_argument("--tickers", default="", help="只跑指定代號（逗號分隔，試跑用）")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--out-dir", default="raw_writer_out", help="試跑輸出資料夾")
    return run(ap.parse_args())


if __name__ == "__main__":
    sys.exit(main())
