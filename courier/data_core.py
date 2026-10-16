# -*- coding: utf-8 -*-
"""
data_core.py — OpenClaw 統一數據存取純核心（DAL data_core，2026-08-23）

定位：**純函數、零 I/O、零副作用。** 集中「schema 知識 + 合併/去重/cap + update-doc
      builders + 寫入閘門政策」。所有中心的資料存取層（database.py / stock_db.py / db.py）
      都應瘦身成薄 I/O 適配（門），把「該查什麼 / 該寫什麼 update-doc / 能不能寫」交給本檔。

目的（用戶 2026-08-23 定調）：統一整個程序組對 Mongo 的存取格式——schema/格式/後端的
      內部修改只改本檔一處、呼叫者不動，消滅「一處修改必須調整整個腳本組」。

⚠️ canonical 副本：本檔為權威版（DC）。之後 vendored 進其他 repo，**逐字同步**
   （比照 bar_validator / decisive_level 慣例），audit 腳本可 checksum 比對是否走樣。
⛔ 本檔**不得** import 任何 center-specific 模組（config / bar_validator / pymongo / motor）
   ——保持可 vendored、sync/async 通用。驗證器（bar_validator）由各中心「門」注入
   （見 gate_ok 的 validate_fn / blocking_fn 參數）。

Python 3.9 相容。
"""
import pytz
from datetime import datetime

# ── schema 常數（唯一真源）───────────────────────────────────────────────
BARS = "Bars"                         # 合併後單一 collection
EST = pytz.timezone("US/Eastern")     # ＝各中心 config.EST_TZ（updated_at 用，逐字等價）

INDICATORS_PERIOD = "INDICATORS"
KEY_LEVELS_PERIOD = "KEY_LEVELS"
SNAPSHOT_PERIOD   = "SIGNAL_SNAPSHOTS"
PRICE_PERIODS     = frozenset({"D", "W", "H", "15m"})

DEFAULT_BARS_LIMIT = 500              # period 不在 BARS_LIMIT 時的 fallback（沿用 .get(period, 500)）
STRUCTURE_KEEP     = 10               # save_dow_structure structure_* 只保留最近 10 個擺動點


def now_est():
    return datetime.now(EST)


# ── filter / projection ───────────────────────────────────────────────
def bars_filter(ticker, period):
    """單一 Bars 的 {ticker,period} 主鍵 filter（ticker 由呼叫端 .upper()）。"""
    return {"ticker": ticker, "period": period}


def bars_slice_projection(limit):
    """讀取端 $slice 投影；limit 為 None/0 → None（全取，find_one 不帶投影）。"""
    return {"bars": {"$slice": limit}} if limit else None


# ── bars 合併 / cap（皇冠邏輯，逐字保留 DC push_bars）────────────────────
def merge_bars_by_t(existing, new_bars, limit):
    """按 t 字典合併、同 t 新值覆蓋（新贏）、降序、cap limit。
    ＝DC push_bars 的 2026-07-25 /prev 止血邏輯（權威重抓覆蓋暫定值、同 t 只留一筆），
    不得簡化。existing/new_bars 皆「新在前」的 bar list，元素含 "t"。"""
    by_t = {b["t"]: b for b in existing}
    for b in new_bars:
        by_t[b["t"]] = b
    return sorted(by_t.values(), key=lambda b: b["t"], reverse=True)[:limit]


def cap_bars(new_bars, limit):
    """全量覆蓋用：取新在前 limit 根（不排序、不去重；DC replace_bars 用）。"""
    return new_bars[:limit]


def sort_cap_bars(new_bars, limit):
    """降序（新在前）排序 + cap；**不去重**（insert 分支：無 existing）。
    ＝BC run_verify_weekend push_bars 的 insert 分支（強制 bars 新在前，防補抓破壞排序）。"""
    return sorted(new_bars, key=lambda b: b["t"], reverse=True)[:limit]


# ── bars update-doc builders ─────────────────────────────────────────
def bars_set_update(bars):
    """push_bars 的 update 分支（doc 已存在，不重設 ticker/period）。"""
    return {"$set": {"bars": bars, "updated_at": now_est()}}


def bars_insert_doc(ticker, period, bars):
    """push_bars 的 insert 分支（doc 不存在）。"""
    return {"ticker": ticker, "period": period, "bars": bars, "updated_at": now_est()}


def bars_replace_update(ticker, period, bars):
    """replace_bars 的 upsert update（全量覆蓋，帶 ticker/period 供 insert）。"""
    return {"$set": {"ticker": ticker, "period": period,
                     "bars": bars, "updated_at": now_est()}}


# ── 事件（prepend + cap，伺服器端 $push/$slice）─────────────────────────
def prep_event(event, ts_field="created_at"):
    """複製 event，補 created_at 預設（不改呼叫端傳入的 dict）。"""
    ev = dict(event)
    ev.setdefault(ts_field, now_est())
    return ev


def event_push_update(event, limit):
    """伺服器端一次完成 prepend(最新在前)+cap：$push $each/$position:0/$slice。
    ＝取代「find_one 整 doc → ([e]+existing)[:limit] → $set 整條」O(doc大小) 讀-改-寫。"""
    return {"$push": {"events": {"$each": [event], "$position": 0, "$slice": limit}},
            "$set": {"updated_at": now_est()}}


# ── INDICATORS 整段 replace（save_dow_structure，盤後權威全量）──────────
def dow_structure_update(ticker, dow_result):
    """整段 replace data 子文件（盤後權威全量重算才可用）。逐字保留 DC save_dow_structure。"""
    data = {
        "dow_trend_h1":  dow_result.get("trend_h1", "sideways"),
        "dow_trend_d1":  dow_result.get("trend_d1", "sideways"),
        "dow_trend_w1":  dow_result.get("trend_w1", "sideways"),
        "dow_reason_h1": dow_result.get("reason_h1", ""),
        "dow_reason_d1": dow_result.get("reason_d1", ""),
        "dow_reason_w1": dow_result.get("reason_w1", ""),
        "structure_h1":  dow_result.get("structure_h1", [])[-STRUCTURE_KEEP:],
        "structure_d1":  dow_result.get("structure_d1", [])[-STRUCTURE_KEEP:],
        "structure_w1":  dow_result.get("structure_w1", [])[-STRUCTURE_KEEP:],
        "updated_at":    now_est(),
    }
    return {"$set": {"ticker": ticker, "period": INDICATORS_PERIOD,
                     "data": data, "updated_at": now_est()}}


# ── INDICATORS dotted 部分 $set（盤中，⛔ 絕不碰 d1/w1）──────────────────
def intraday_dow_h1_update(ticker, dow_result, computed_at):
    """dotted 部分 $set，只更新 h1 子欄位 + h1_source/h1_computed_at。
    ⛔ 絕不碰 data.dow_trend_d1/_w1、data.structure_d1/_w1（盤後權威）。
    逐字保留 DC save_intraday_dow_h1。"""
    return {"$set": {
        "ticker":              ticker,
        "period":              INDICATORS_PERIOD,
        "data.dow_trend_h1":   dow_result.get("trend", "sideways"),
        "data.dow_reason_h1":  dow_result.get("reason", ""),
        "data.structure_h1":   (dow_result.get("structure", []) or [])[-STRUCTURE_KEEP:],
        "data.h1_source":      "intraday",
        "data.h1_computed_at": computed_at,
        "updated_at":          now_est(),
    }}


def intraday_key_level_h_update(ticker, decisive, computed_at):
    """dotted 部分 $set，只寫 key_level_h 子欄位。⛔ 絕不碰盤後 D/W。
    逐字保留 DC save_intraday_key_level_h。"""
    return {"$set": {
        "ticker":                       ticker,
        "period":                       INDICATORS_PERIOD,
        "data.key_level_h":             decisive,
        "data.key_level_h_computed_at": computed_at,
        "updated_at":                   now_est(),
    }}


# ── KEY_LEVELS 整段寫（BC.p save_key_levels）─────────────────────────────
def key_levels_update(ticker, kl_doc):
    """整段寫 KEY_LEVELS doc：schema_version 預設 candidate_v0.1、補 updated_at、
    $set 整段 + ticker/period。逐字保留 BC.p save_key_levels（配 upsert=True 由 adapter 帶）。"""
    kl = dict(kl_doc)
    kl.setdefault("schema_version", "candidate_v0.1")
    kl["updated_at"] = now_est()
    kl["ticker"] = ticker
    kl["period"] = KEY_LEVELS_PERIOD
    return {"$set": kl}


# ── 寫入閘門（fail-open）——集中一份供各中心「門」呼叫 ────────────────────
def gate_ok(new_bars, spec, validate_fn, blocking_fn, label="", log=None):
    """寫入前驗證閘門：回 True=可寫；False=有 blocking 硬失敗、應擋寫、保留現有。
    ⚠️ 閘門本身異常＝**放行**（不擋鏈路）——逐字保留原 v3_processor._gate 的 fail-open。
    validate_fn / blocking_fn 由 adapter 注入（＝各中心 vendored bar_validator 的
    validate / blocking_fails），本檔不直接 import bar_validator（保持可 vendored）。"""
    try:
        verdict = validate_fn(new_bars, spec)
        blk = blocking_fn(verdict)
    except Exception as e:
        if log:
            log(f"  [{label}] ⚠️ 閘門異常，放行不擋: {e}")
        return True
    if blk:
        if log:
            log(f"  [{label}] ⛔ 寫入閘門擋下 {[x['code'] for x in blk]} → 跳過不寫、保留現有")
        return False
    return True
