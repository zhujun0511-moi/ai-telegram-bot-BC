"""
td_ready_gate.py — TD 當日日線「定稿閘門」：確認資料定稿才觸發 DC 盤後鏈（2026-10-02 新增，BC）

起因（2026-10-02 實測）：cron-job.org 17:00 ET 起每 30 分打 DC `/v3-update`，但 TD 當日日線約
17:40 才出現、量之後還會修正（NVDA 10-01 17:54 抓到量 2,573,606、定稿 98,591,400）→ DC 每晚混入
「沒有今天 bar」與「量為草稿」兩種壞資料，指標/CFET/報告當晚都受影響。

流程（交易日；休市日直接結束）：
  1. ~17:15 ET 起，每 15 分鐘查一次 TD 抽樣（SAMPLES）今天的日線
  2. 定稿＝抽樣全部都有今天 bar，且與上一次相比「收盤不變、量變動 < VOL_TOL」
     （TD 隔天仍會小幅修量 ~0.4%，故不要求完全相同；草稿那種差數十倍會被擋）
  3. 定稿 → 觸發 DC `/v3-update?mode=incremental`；FORCE_AT_ET 仍未定稿 → 強制觸發（保底一）
  4. 觸發後每 30 分查 Mongo System_State{id:global}.after_hours_done_{date}：未完成就再打一次
     （保留原 cron「每 30 分續打、DC 斷了接著跑」的功能）；完成即收工；最晚 END_AT_ET
  5. 每晚觀測寫 System_State{id:td_ready_gate}（首見/定稿/觸發/續打/完成時間），供健康報告
保底二：cron-job.org 改 23:00/23:30/00:00 各打一次（GitHub 失靈時才真正起作用；DC 冪等）。

env：MONGO_URI、TWELVE_KEY、WEBHOOK_SECRET、DC_URL（預設 DC HF Space 根網址）、GATE_DRY_RUN=1＝只觀測不觸發
Python 3.9 相容。
"""
import datetime as dt
import os
import sys
import time

import pytz

# 2026-10-16「唯一的門」：TwelveData、觸發 DC、Mongo 一律經 courier（金鑰／密鑰不離開門）。
from courier import mongo as M
from courier import twelvedata as TDC
from courier.outbound import dc_v3_update

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

ET          = pytz.timezone("US/Eastern")
SAMPLES     = ["SPY", "QQQ", "AAPL", "NVDA", "JPM", "XLE", "MNST", "LUNR"]
VOL_TOL     = 0.005          # 量變動 < 0.5% 視為穩定
POLL_MIN    = 15
WATCH_MIN   = 30
START_AT_ET = (17, 15)
FORCE_AT_ET = (22, 30)
LATE_START_ET = (19, 0)      # 2026-10-05：晚於此時間才啟動（排程延遲／手動補跑），第一次查詢抽樣全有當日線即觸發，不再等 15 分比對
END_AT_ET   = (23, 0)
JOB_BUDGET_S = 5 * 3600 + 45 * 60   # GitHub job 上限 6h，留餘裕
# DC_URL 已搬到 courier/outbound.dc_v3_update（2026-10-16）
DRY_RUN     = os.getenv("GATE_DRY_RUN", "0").strip() == "1"


def _log(m):
    print(f"[{dt.datetime.now(ET).strftime('%H:%M:%S ET')}] {m}", flush=True)


def _hm(now=None):
    return (now or dt.datetime.now(ET)).strftime("%H:%M")


def _at(today, hm):
    return ET.localize(dt.datetime.combine(today, dt.time(*hm)))


# ══ 純函數（可離線測試）══

def is_trading_day(d, holidays):
    return d.weekday() < 5 and d.isoformat() not in holidays


def is_stable(prev, cur, samples, today):
    """全部抽樣都有今天 bar，且與上一輪相比收盤不變、量變動 < VOL_TOL。回 (bool, 未穩清單)。"""
    bad = []
    for s in samples:
        c, p = cur.get(s), (prev or {}).get(s)
        if not c or c["date"] != today:
            bad.append(f"{s}:無今日")
            continue
        if not p or p["date"] != today:
            bad.append(f"{s}:首見")
            continue
        if c["close"] != p["close"]:
            bad.append(f"{s}:收盤變動")
            continue
        base = max(p["volume"], 1.0)
        if abs(c["volume"] - p["volume"]) / base >= VOL_TOL:
            bad.append(f"{s}:量變{(c['volume'] - p['volume']) / base * 100:+.1f}%")
    return (not bad), bad


# ══ IO ══

def td_snapshot(samples):
    """一次批量查抽樣最新日線（逐字搬到 courier/twelvedata.gate_snapshot）。"""
    return TDC.gate_snapshot(samples, _log)


def trigger_dc():
    """觸發 DC /v3-update（逐字搬到 courier/outbound.dc_v3_update；重試 3 次、試跑略過）。"""
    return dc_v3_update(_log, DRY_RUN)


def keep_existing(prev, today_iso):
    """2026-10-06：cron-job.org 17:03／17:33 兩次觸發 → 第二輪排隊到第一輪結束才跑、一開始就發現 DC 已完成。
    同一交易日已有實際觸發紀錄（triggered_at 有值）時，第二輪不要覆蓋它（否則健康報告看不到定稿／觸發／完成時間）。"""
    return bool(prev) and prev.get("date") == today_iso and bool(prev.get("triggered_at")) and not prev.get("dry_run")


def main():
    t0 = time.time()
    now = dt.datetime.now(ET)
    today = now.date()
    M.configure(strict=True)   # 原寫法 os.environ["MONGO_URI"]（缺就 KeyError）
    cal = M.find_one("StockData", "Configs", {"type": "market_calendar"}) or {}
    holidays = set(cal.get("holidays") or [])
    rec = {"id": "td_ready_gate", "date": today.isoformat(), "started_at": dt.datetime.now(dt.timezone.utc),
           "samples": SAMPLES, "first_seen": {}, "polls": 0, "ready_at": None, "ready_reason": None,
           "triggered_at": None, "retriggers": 0, "done_at": None, "finished": None, "td_credits": 0,
           "dry_run": DRY_RUN}

    def save():
        M.update_one("StockData", "System_State", {"id": "td_ready_gate"}, {"$set": rec}, upsert=True)

    if not is_trading_day(today, holidays):
        rec["finished"] = "holiday"
        save()
        _log(f"{today} 非交易日，結束")
        return 0

    done_key = f"after_hours_done_{today.isoformat()}"
    deadline = min(_at(today, END_AT_ET), now + dt.timedelta(seconds=JOB_BUDGET_S))
    start = _at(today, START_AT_ET)
    if now < start:
        _log(f"等到 {START_AT_ET[0]}:{START_AT_ET[1]:02d} ET 開始")
        time.sleep((start - now).total_seconds())

    # TWELVE_KEY／WEBHOOK_SECRET 改在門內讀（2026-10-16）
    prev = None
    # ── 階段一：等 TD 定稿 ──
    while True:
        now = dt.datetime.now(ET)
        if (M.find_one("StockData", "System_State", {"id": "global"}) or {}).get(done_key):
            if keep_existing(M.find_one("StockData", "System_State", {"id": "td_ready_gate"}), today.isoformat()):
                _log("DC 今日盤後已完成，且今天已有閘門觸發紀錄（例：17:03 那輪）→ 保留原紀錄、不覆蓋，收工")
                return 0
            _log("DC 今日盤後已完成（可能被手動/保底觸發），閘門收工")
            rec["finished"] = "already_done"
            save()
            return 0
        cur = td_snapshot(SAMPLES)
        rec["polls"] += 1
        rec["td_credits"] += len(SAMPLES)
        if cur:
            for s, v in cur.items():
                if v["date"] == today.isoformat() and s not in rec["first_seen"]:
                    rec["first_seen"][s] = _hm(now)
            all_today = all((cur.get(s) or {}).get("date") == today.isoformat() for s in SAMPLES)
            if rec["polls"] == 1 and all_today and now >= _at(today, LATE_START_ET):
                rec["ready_at"], rec["ready_reason"] = _hm(now), "late_start"
                _log("晚啟動（%02d:%02d ET 後）且抽樣全有當日線 → 視為已定稿，直接觸發" % LATE_START_ET)
                break
            ok, bad = is_stable(prev, cur, SAMPLES, today.isoformat())
            _log(f"第 {rec['polls']} 次：{'✅ 定稿' if ok else '未定稿 ' + '、'.join(bad)}")
            if ok:
                rec["ready_at"], rec["ready_reason"] = _hm(now), "stable"
                break
            prev = cur
        save()
        if now >= _at(today, FORCE_AT_ET) or now >= deadline:
            rec["ready_at"], rec["ready_reason"] = _hm(now), "deadline"
            _log("到保底時間仍未定稿 → 強制觸發")
            break
        time.sleep(POLL_MIN * 60)

    # ── 階段二：觸發 DC，之後盯到完成（未完成就續打＝斷了接著跑）──
    trigger_dc()
    rec["triggered_at"] = _hm()
    save()
    while True:
        time.sleep(WATCH_MIN * 60)
        now = dt.datetime.now(ET)
        if (M.find_one("StockData", "System_State", {"id": "global"}) or {}).get(done_key):
            rec["done_at"], rec["finished"] = _hm(now), "done"
            save()
            _log(f"✅ DC 今日盤後完成（{rec['done_at']}）")
            return 0
        if now >= deadline or time.time() - t0 > JOB_BUDGET_S:
            rec["finished"] = "deadline"
            save()
            _log("到收工時間 DC 仍未完成 → 交給 cron-job.org 23:00 起的保底")
            return 0
        _log("DC 尚未完成 → 續打 /v3-update（斷了接著跑）")
        trigger_dc()
        rec["retriggers"] += 1
        save()


if __name__ == "__main__":
    sys.exit(main())
