"""
stale_bars_watch.py — 每日「日線停更」看門狗（2026-10-01 新增，C）

起因：2026-10-01 用戶發現 AVB/EQR（2026-08-18 已下市）仍在清單、日線停在 8 月，
指標卻每天照算到最新日（拿舊 K 冒充今天）；另 6 支（SYM/SPCX/SERV/PARA/LUNR/CRCL）
日線被 DC 閘門擋、平日落後到週末才被補回。全程沒有任何通知。

做法（唯讀 Mongo、不寫任何東西）：
  - 清單＝Configs{type:ticker_lists}.full_set
  - 基準＝SPY 日線最近 N 個交易日（用 SPY 的實際 bar 日期當交易日曆，不另維護日曆）
  - 每支 D 最新日 vs 基準：落後 ≥ STALE_MIN_DAYS 個交易日 → 列入告警
  - 另列「已標 delisted 卻仍在清單」「沒有 D 資料」
  - 有問題才發 Telegram（ALWAYS_NOTIFY=1 則一律發一份）

排程：2026-10-02 起併入 actions_minutes_watch「📊 每日健康報告」（stale_section），本檔 main 只供手動執行。Python 3.9 相容。
"""
import os
import sys

import pymongo

from tasks.outbound import notify

MONGO_URI      = os.getenv("MONGO_URI", "")
STALE_MIN_DAYS = int(os.getenv("STALE_MIN_DAYS", "2"))
ALWAYS_NOTIFY  = os.getenv("ALWAYS_NOTIFY", "0").strip() == "1"
REF_TICKER     = "SPY"
REF_WINDOW     = 30
REPORT_TYPE    = "data_stale_watch"


def _newest_d(sd, tickers):
    """{TICKER: (newest_date_str|None, synthetic)}，一次查完。"""
    out = {}
    for d in sd["Bars"].find({"period": "D", "ticker": {"$in": tickers}},
                             {"ticker": 1, "synthetic": 1, "bars": {"$slice": 1}}):
        b = d.get("bars") or []
        out[d["ticker"].upper()] = ((str(b[0].get("t"))[:10] if b else None), bool(d.get("synthetic")))
    return out


def ref_trading_dates(sd):
    """基準 SPY 最近 REF_WINDOW 個交易日（新在前）；空＝無法判斷。"""
    ref = sd["Bars"].find_one({"ticker": REF_TICKER, "period": "D"}, {"bars": {"$slice": REF_WINDOW}})
    return [str(b.get("t"))[:10] for b in (ref or {}).get("bars") or []]


def full_set(sd):
    cfg = sd["Configs"].find_one({"type": "ticker_lists"}) or {}
    return sorted({t.upper() for t in cfg.get("full_set", []) if isinstance(t, str)})


def stale_section(sd, ref_dates, full):
    """（2026-10-02 併入每日健康報告）日線停更檢查。回 (lines, alarm)。"""
    delisted = {d["ticker"].upper() for d in sd["Ticker_Identity"].find({"delisted": True}, {"ticker": 1})}
    newest = _newest_d(sd, full)
    stale, no_data, delisted_in_list = [], [], []
    for tk in full:
        if tk in delisted:
            delisted_in_list.append(tk)
            continue
        nd, synthetic = newest.get(tk, (None, False))
        if synthetic:
            continue
        if not nd:
            no_data.append(tk)
            continue
        lag = sum(1 for d in ref_dates if d > nd)
        if lag >= STALE_MIN_DAYS:
            stale.append((lag, tk, nd))
    stale.sort(key=lambda x: (-x[0], x[1]))
    lines = []
    if stale:
        lines.append(f"⚠️ 日線落後 ≥{STALE_MIN_DAYS} 個交易日 {len(stale)} 支："
                     + "、".join(f"{tk}(停在{nd}，落後{lag if lag < REF_WINDOW else str(REF_WINDOW) + '+'}天)"
                                 for lag, tk, nd in stale[:30])
                     + (f" 等共{len(stale)}支" if len(stale) > 30 else ""))
    if delisted_in_list:
        lines.append(f"🪦 已標下市卻仍在清單 {len(delisted_in_list)} 支（請從 DC config 清單移除）："
                     + "、".join(delisted_in_list))
    if no_data:
        lines.append(f"❓ 沒有日線資料 {len(no_data)} 支：" + "、".join(no_data[:30]))
    alarm = bool(stale or delisted_in_list or no_data)
    if not alarm:
        lines.append(f"✅ 日線：{len(full)} 支全部新鮮")
    return lines, alarm


def main() -> int:
    """單獨手動執行用（2026-10-02 起排程已併入 actions_minutes_watch 每日健康報告）。"""
    if not MONGO_URI:
        print("❌ MONGO_URI 未設定")
        return 1
    sd = pymongo.MongoClient(MONGO_URI)["StockData"]

    full = full_set(sd)
    if not full:
        notify("❌ [停更看門狗] Configs.full_set 為空，無法檢查", report_type=REPORT_TYPE)
        return 1

    ref_dates = ref_trading_dates(sd)
    if not ref_dates:
        notify(f"❌ [停更看門狗] 基準 {REF_TICKER} 無日線，無法檢查", report_type=REPORT_TYPE)
        return 1
    ref_latest = ref_dates[0]
    lines, problems = stale_section(sd, ref_dates, full)
    msg = "\n".join([f"📉 [停更看門狗] 基準 {REF_TICKER} 最新日線 {ref_latest}｜清單 {len(full)} 支"] + lines)
    print(msg)
    if problems or ALWAYS_NOTIFY:
        notify(msg, report_type=REPORT_TYPE)
    return 0


if __name__ == "__main__":
    sys.exit(main())
