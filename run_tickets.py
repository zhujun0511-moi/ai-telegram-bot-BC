# -*- coding: utf-8 -*-
"""
run_tickets.py — 本地端↔數據中心 Mongo 工單通道：數據中心側（2026-10-28 Jun 拍板）

來源：本地端規格（Shared `規格_Mongo工單通道_給數據中心_20261028.md`，本地端 ADR-058）＋ Jun 10-28：
  時間全程全域美東；主要是單一個股斷流 3 個交易日的原因查找；可用其他資源判斷；位置由數據中心定；
  雙方溝通只走 Mongo（電腦沒開機時雲端寫不到 Shared）；數據中心要按時檢查新工單、能預審。
登記：MongoDB_Standard_v3「CommData.Tickets」節；handoff_master「2026-10-28 工單通道」節。

做什麼（bc_tickets.yml 每日 12:30／23:30 UTC＋手動）：
  1. 搶 to=datacenter 的 open 單 → working（條件更新，同一張單不會被處理兩次）；
     working 但本輪還沒預審（dc_review.round ≠ round，例：上次中途失敗）也補做。
  2. 預審 stream_break：Polygon 代號現況／已停止紀錄／代號變更事件／斷流那幾天有沒有日線 ＋ SEC 最近申報 → 寫 dc_review。
  3. Polygon 那幾天確有日線（且是第 1 輪）＝自動回 source_gap；其他一律留 working 等人工（本機 tickets_cli.py）。
  4. Telegram 通知：本次預審幾張、待人工幾張、本地端回了我方幾張單。
寫死的規矩（規格 §3）：對別人的單只追加回覆、改 status、寫我方專用欄位 dc_review；不改開單方欄位；只刪 from=datacenter 的單。
工單不放價格（只放日期、天數、代號／entity）。公開日誌只印數量、不印代號。Python 3.9 相容。
"""
import argparse
import datetime as dt
import sys
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
DB, COL = "CommData", "Tickets"
ME, PEER = "datacenter", "local"
REPORT_TYPE = "data_stale_watch"   # 沿用斷流看門狗的通知類型（不新增 report_type，避免動到 REPORT_CHANNEL_TYPES）
MAX_PER_RUN = 20                   # 每張單約 4～5 次 Polygon 呼叫 × 13.5 秒；個股級別，一次 20 張足夠
SEC_LOOKBACK_DAYS = 120

# 回覆類型（規格 §4）：kind → body 必填欄位。改名／下市必須附已發布的身份表版本。
# 2026-10-28 本地端同意的規格補充：market_changed（本端：換交易所代號不變＝視同 source_gap 補抓接續；
#   轉 OTC＝停止跟蹤、舊數據保留）；delisted 加選填 reason／successor（本端只記入 meta.json，successor 不自動跟蹤）。
REPLY_KINDS = {
    "renamed":        ("new_symbol", "effective", "identity_version"),
    "delisted":       ("last_trading_day", "identity_version"),
    "halted":         ("expected_resume",),      # 可為 None
    "source_gap":     ("trading_days",),
    "market_changed": ("new_market", "effective"),
    "unknown":        (),                        # note 必填
}
DELISTED_REASONS = ("acquired", "merged", "spun_off", "deregistered", "exchange_delist")


def now_et_iso():
    return dt.datetime.now(ET).isoformat(timespec="seconds")


def _date(s):
    return dt.date.fromisoformat(str(s)[:10])


# ── 純函數（tests/test_run_tickets.py 離線測試）──

def validate_reply(kind, body, note):
    """回 None＝合格；否則回錯誤說明。"""
    if kind not in REPLY_KINDS:
        return "不認得的回覆類型 %s（可用：%s）" % (kind, "、".join(REPLY_KINDS))
    body = body or {}
    for k in REPLY_KINDS[kind]:
        if k not in body:
            return "%s 缺 body.%s" % (kind, k)
        if body[k] in (None, "", []) and not (kind == "halted" and k == "expected_resume"):
            return "%s 的 body.%s 不能空" % (kind, k)
    if "identity_version" in REPLY_KINDS[kind]:
        v = body["identity_version"]
        if not isinstance(v, int) or isinstance(v, bool) or v < 1:
            return "identity_version 要是已發布的身份表版本號（正整數）"
    if kind == "source_gap" and not all(isinstance(d, str) and len(d) == 10 for d in body["trading_days"]):
        return "trading_days 要是 YYYY-MM-DD 字串清單"
    if kind == "unknown" and not (note or "").strip():
        return "unknown 必須寫 note"
    if kind == "delisted":
        if body.get("reason") not in (None,) + DELISTED_REASONS:
            return "delisted 的 reason 只能是 %s（或不填）" % "／".join(DELISTED_REASONS)
        if body.get("successor") is not None and not (isinstance(body["successor"], str) and body["successor"].strip()):
            return "delisted 的 successor 要是代號字串（或不填）"
    if kind == "market_changed" and not (isinstance(body["new_market"], str) and body["new_market"].strip()):
        return "market_changed 的 new_market 要寫交易所代碼（例 XNYS、XNAS）或 OTC"
    return None


def reply_update(ticket, kind, body, note, at):
    """回 (條件, 更新)：只追加一則回覆＋改 status。條件鎖住「給我方的、working、同一輪」，不會蓋到別輪或別人的單。"""
    err = validate_reply(kind, body, note)
    if err:
        raise ValueError(err)
    rnd = int(ticket.get("round") or 1)
    q = {"_id": ticket["_id"], "to": ME, "status": "working", "round": rnd}
    u = {"$push": {"replies": {"by": ME, "at": at, "round": rnd, "kind": kind, "body": body or {}, "note": note or ""}},
         "$set": {"status": "replied"}}
    return q, u


def delete_query(ticket_id):
    """只刪自己開的單（規格 §3-6：同一帳號擋不住越權刪除，條件寫死在這裡）。"""
    return {"_id": ticket_id, "from": ME}


def new_ticket(type_, key, subject, summary, at, evidence=None, detail=None, to=PEER):
    """數據中心開單（規格 §3-5）。_id 確定性：datacenter-{類型}-{鍵}。"""
    doc = {"_id": "%s-%s-%s" % (ME, type_, key), "from": ME, "to": to, "type": type_, "subject": subject or {},
           "evidence": evidence or {}, "summary": summary, "status": "open", "created_at": at, "round": 1,
           "replies": []}
    if detail is not None:
        doc["detail"] = detail
    return doc


def classify(ticket, facts):
    """預審判定 → (verdict, suggest, reason, auto_reply)。
    verdict：auto_replied（auto_reply 為 (kind, body, note)）｜needs_human。只有證據明確的 source_gap 才自動回。"""
    sym = ((ticket.get("subject") or {}).get("symbol") or "").upper()
    last = ((ticket.get("evidence") or {}).get("last_data_day") or "")[:10]
    ov, ina = facts.get("overview"), facts.get("inactive") or []
    events, bars, sec = facts.get("events") or [], facts.get("bar_dates"), facts.get("sec") or []
    new_days = [d for d in (bars or []) if not last or d > last]

    if ticket.get("type") != "stream_break":
        return "needs_human", "unsupported_type", "工單類型 %s 尚未支援自動預審" % ticket.get("type"), None
    if int(ticket.get("round") or 1) > 1:
        return "needs_human", "follow_up", "本地端追問（第 %s 輪），交人工" % ticket.get("round"), None
    if new_days and ov and ov.get("active") is True and (ov.get("market") or "stocks").lower() != "otc":
        return ("auto_replied", "source_gap", "Polygon 在 %s 之後有 %d 天日線" % (last or "?", len(new_days)),
                ("source_gap", {"trading_days": new_days},
                 "Polygon 該代號這幾天有日線（不復權），代號仍現役；請補抓"))

    reasons, suggest = [], "unknown"
    forms = {s.get("form") for s in sec}
    items = " ".join(s.get("items") or "" for s in sec)
    later = [e for e in events if e.get("ticker") and e["ticker"].upper() != sym and (not last or (e.get("date") or "") >= str(_date(last) - dt.timedelta(days=30)))]
    if later:
        suggest = "renamed"
        reasons.append("Polygon 代號變更事件：%s 起 → %s" % (later[-1].get("date"), later[-1].get("ticker")))
    gone = [r for r in ina if (r.get("delisted_utc") or "")[:10] and (not last or (r["delisted_utc"][:10] >= str(_date(last) - dt.timedelta(days=10))))]
    if gone and suggest == "unknown":
        suggest = "delisted"
    if gone:
        reasons.append("Polygon 已停止交易：%s" % gone[0]["delisted_utc"][:10])
    if forms & {"25", "25-NSE", "15-12B", "15-12G", "15-15D", "15F-12B"}:
        reasons.append("SEC 下市／終止註冊申報：%s" % "、".join(sorted(forms & {"25", "25-NSE", "15-12B", "15-12G", "15-15D", "15F-12B"})))
        if suggest == "unknown":
            suggest = "delisted"
    if "2.01" in items or forms & {"S-4", "425", "DEFM14A", "SC TO-T", "SC 14D9"}:
        reasons.append("SEC 併購相關申報（8-K 2.01／S-4／425／DEFM14A／收購要約）")
    if "3.01" in items:
        reasons.append("8-K 3.01：收到交易所除名或不符上市標準通知")
    if "10-12B" in forms:
        reasons.append("SEC Form 10-12B：可能有分拆上市")
    if forms & {"8-A12B", "8-A12G"}:
        reasons.append("SEC 8-A：有新的交易所註冊（可能換市場或新代號）")
    otc = bool(ov) and ((ov.get("market") or "").lower() == "otc" or (ov.get("primary_exchange") or "").upper().startswith("OTC"))
    if otc:
        reasons.append("Polygon 顯示代號現在在場外市場（market=%s、%s）" % (ov.get("market"), ov.get("primary_exchange") or "交易所不明"))
        if suggest == "unknown":
            suggest = "market_changed"
    elif ov and ov.get("active") is True and not new_days:
        reasons.append("Polygon 代號仍現役（%s）但那幾天沒有日線：可能停牌或換市場" % (ov.get("primary_exchange") or "交易所不明"))
        if suggest == "unknown":
            suggest = "halted_or_market_changed"
    if bars is None:
        reasons.append("Polygon 日線查詢失敗（狀態未知）")
    if not reasons:
        reasons.append("Polygon 與 SEC 都沒有找到原因")
    return "needs_human", suggest, "；".join(reasons), None


# ── 門外流程（連線一律經 courier/）──

def _facts(ticket, log):
    from courier import polygon as PG
    from courier import public_data as PD
    sym = ((ticket.get("subject") or {}).get("symbol") or "").upper().replace("-", ".")
    last = ((ticket.get("evidence") or {}).get("last_data_day") or "")[:10]
    today = dt.datetime.now(ET).date()
    frm = str(_date(last) - dt.timedelta(days=5)) if last else str(today - dt.timedelta(days=20))
    ov = PG.ticket_overview(sym, log)
    ina = PG.ticket_inactive(sym, log)
    ident = (ov or {}).get("composite_figi") or next((r.get("composite_figi") for r in (ina or []) if r.get("composite_figi")), None) or sym
    ev = PG.ticket_change_events(ident, log)
    bars = PG.ticket_bar_dates(sym, frm, str(today), log)
    cik = (ov or {}).get("cik") or next((r.get("cik") for r in (ina or []) if r.get("cik")), None)
    sec = []
    if cik and PD.has_sec_contact():
        try:
            sec = PD.sec_recent_filings(cik, str(today - dt.timedelta(days=SEC_LOOKBACK_DAYS)))
        except Exception as e:
            log("  ⚠️ SEC 讀取失敗：%s" % type(e).__name__)
    return {"overview": ov, "inactive": ina, "events": ev, "bar_dates": bars, "sec": sec, "query_from": frm}


def _ensure_indexes(mongo):
    mongo.create_index(DB, COL, [("status", 1), ("to", 1)])
    mongo.create_index(DB, COL, [("type", 1), ("subject.symbol", 1)])


def _to_review(mongo):
    """要預審的單：open 的先搶成 working；working 但本輪沒預審的補做。"""
    out = []
    for d in mongo.find(DB, COL, {"to": ME, "status": "open"}, sort=("created_at", 1), limit=MAX_PER_RUN):
        got = mongo.find_one_and_update(DB, COL, {"_id": d["_id"], "to": ME, "status": "open"},
                                        {"$set": {"status": "working"}})
        if got is not None:
            got["status"] = "working"
            out.append(got)
    left = MAX_PER_RUN - len(out)
    if left > 0:
        # 排除剛搶到的（它們也還沒有 dc_review，不排除會被處理兩次；10-28 實測抓到）
        q = {"to": ME, "status": "working", "_id": {"$nin": [d["_id"] for d in out]},
             "$expr": {"$ne": [{"$ifNull": ["$dc_review.round", 0]}, "$round"]}}
        out += list(mongo.find(DB, COL, q, sort=("created_at", 1), limit=left))
    return out


def run(dry_run=False, log=print):
    from courier import mongo
    from courier.outbound import notify
    mongo.configure(strict=True, serverSelectionTimeoutMS=20000)
    if not dry_run:
        _ensure_indexes(mongo)
    todo = [] if dry_run else _to_review(mongo)
    n_auto = n_human = 0
    human_lines = []
    for t in todo:
        facts = _facts(t, log)
        verdict, suggest, reason, auto = classify(t, facts)
        review = {"round": int(t.get("round") or 1), "at": now_et_iso(), "verdict": verdict, "suggest": suggest,
                  "reason": reason, "facts": facts}
        mongo.update_one(DB, COL, {"_id": t["_id"], "to": ME}, {"$set": {"dc_review": review}})
        if auto:
            q, u = reply_update(t, auto[0], auto[1], auto[2], now_et_iso())
            if mongo.update_one(DB, COL, q, u).modified_count == 1:
                n_auto += 1
                continue
            reason += "；自動回覆寫入失敗（單子狀態已變）"
        n_human += 1
        human_lines.append("• %s｜建議 %s｜%s" % ((t.get("subject") or {}).get("symbol"), suggest, reason))
    pending = list(mongo.find(DB, COL, {"to": ME, "status": "working"}, {"_id": 1, "created_at": 1},
                              sort=("created_at", 1)))
    peer_replied = sum(1 for _ in mongo.find(DB, COL, {"from": ME, "status": "replied"}, {"_id": 1}))
    log("工單：本次預審 %d 張（自動回 source_gap %d、待人工 %d）｜目前待人工共 %d 張｜本地端已回我方 %d 張" % (
        len(todo), n_auto, n_human, len(pending), peer_replied))
    evening = dt.datetime.now(ET).hour >= 12
    if not dry_run and (todo or ((pending or peer_replied) and evening)):
        msg = ["🎫 工單通道（數據中心）",
               "本次預審 %d 張：自動回覆 source_gap %d、待人工 %d" % (len(todo), n_auto, n_human)]
        msg += human_lines
        if pending:
            msg.append("目前待人工共 %d 張（最早 %s 開單）→ 跟數據中心窗口說「處理工單」" % (
                len(pending), str(pending[0].get("created_at"))[:10]))
        if peer_replied:
            msg.append("本地端已回覆我方開的單 %d 張，待看" % peer_replied)
        notify("\n".join(msg), report_type=REPORT_TYPE)
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="只連線計數，不搶單、不寫入、不通知")
    a = ap.parse_args()
    return run(dry_run=a.dry_run)


if __name__ == "__main__":
    sys.exit(main())
