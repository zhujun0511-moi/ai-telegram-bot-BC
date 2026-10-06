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

import pymongo
import pytz
import requests

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
DC_URL      = os.getenv("DC_URL", "").strip().rstrip("/") or "https://zhujun0511-ai-ai-telegram-bot-dc.hf.space"
DRY_RUN     = os.getenv("GATE_DRY_RUN", "0").strip() == "1"
UA          = {"User-Agent": "python-requests/2.32.3"}


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

def td_snapshot(key, samples):
    """一次批量查抽樣最新日線 → {sym: {date, close, volume}}；失敗回 None。"""
    try:
        r = requests.get("https://api.twelvedata.com/time_series",
                         params={"symbol": ",".join(samples), "interval": "1day", "outputsize": "1"},
                         headers={**UA, "Authorization": "apikey " + key}, timeout=40)
        j = r.json()
    except Exception as e:
        _log(f"⚠️ TD 連線異常 {type(e).__name__}")
        return None
    if j.get("code") == 429 or j.get("status") == "error":
        _log(f"⚠️ TD {j.get('code')} {str(j.get('message', ''))[:80]}")
        return None
    out = {}
    for s in samples:
        v = ((j.get(s) or {}).get("values") or [None])[0]
        if v:
            try:
                out[s] = {"date": v["datetime"][:10], "close": v["close"], "volume": float(v.get("volume") or 0)}
            except (KeyError, ValueError, TypeError):
                pass
    return out


def trigger_dc(secret):
    """觸發 DC /v3-update（HF 剛好重啟可能失敗 → 重試 3 次）。"""
    if DRY_RUN:
        _log("（試跑）略過觸發 DC")
        return True
    for i in range(3):
        try:
            r = requests.get(f"{DC_URL}/v3-update", params={"mode": "incremental"},
                             headers={**UA, "x-webhook-secret": secret}, timeout=60)
            _log(f"→ DC /v3-update：HTTP {r.status_code} {r.text[:80]}")
            if r.status_code == 200:
                return True
        except Exception as e:
            _log(f"⚠️ 觸發 DC 異常 {type(e).__name__}")
        time.sleep(30)
    return False


def main():
    t0 = time.time()
    now = dt.datetime.now(ET)
    today = now.date()
    sd = pymongo.MongoClient(os.environ["MONGO_URI"])["StockData"]
    st_col = sd["System_State"]
    cal = sd["Configs"].find_one({"type": "market_calendar"}) or {}
    holidays = set(cal.get("holidays") or [])
    rec = {"id": "td_ready_gate", "date": today.isoformat(), "started_at": dt.datetime.now(dt.timezone.utc),
           "samples": SAMPLES, "first_seen": {}, "polls": 0, "ready_at": None, "ready_reason": None,
           "triggered_at": None, "retriggers": 0, "done_at": None, "finished": None, "td_credits": 0,
           "dry_run": DRY_RUN}

    def save():
        st_col.update_one({"id": "td_ready_gate"}, {"$set": rec}, upsert=True)

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

    key, secret = os.environ.get("TWELVE_KEY", ""), os.environ.get("WEBHOOK_SECRET", "")
    prev = None
    # ── 階段一：等 TD 定稿 ──
    while True:
        now = dt.datetime.now(ET)
        if (st_col.find_one({"id": "global"}) or {}).get(done_key):
            _log("DC 今日盤後已完成（可能被手動/保底觸發），閘門收工")
            rec["finished"] = "already_done"
            save()
            return 0
        cur = td_snapshot(key, SAMPLES)
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
    trigger_dc(secret)
    rec["triggered_at"] = _hm()
    save()
    while True:
        time.sleep(WATCH_MIN * 60)
        now = dt.datetime.now(ET)
        if (st_col.find_one({"id": "global"}) or {}).get(done_key):
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
        trigger_dc(secret)
        rec["retriggers"] += 1
        save()


if __name__ == "__main__":
    sys.exit(main())
