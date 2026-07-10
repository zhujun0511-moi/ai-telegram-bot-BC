"""
run_mp_dow_structure.py — BC job2：MP 道氏結構計算 v1.0

職責（只做一件事）：
  讀 HF mp_data/ticker/XXX/d.csv
  → 計算 D1/W1 道氏趨勢結構
  → 寫 mp_data/ticker/XXX/structure.json（覆蓋，最新狀態）

不做的事：
  - 不計算指標（由 run_mp_indicator_calc.py 負責）
  - 不計算 phase（由 run_phase_calc.py 負責）
  - 不寫 indicators.csv

並行說明：
  與 run_mp_indicator_calc.py 並行執行（job1 + job2 同時跑）。
  兩者均只讀 d.csv，輸出文件不同，無衝突。
  HF commit 共 ~84 次 < 128次/小時上限，安全。

未收盤 bar 保護：
  讀 mp_data/meta/daily_status.json
  is_week_complete=false → W1 道氏結構跳過，dow_trend_w1=null

道氏算法：
  與 DC tasks/dow_structure.py 完全一致（禁止跨文件 import，複製核心函數）。
  fractal 右側保護：range(2, n-3)（2026-07-10統一：道氏跟CFET一律用n-3，
  目的是防止組成分形的5根bar還沒走完就告警/回測誤判，不再保留n-2版本）。
  bars 輸入：新在前 list，每個 bar {"h": float, "l": float, "t": str}。

進度追蹤：
  StockData.System_State  id="mp_dow_structure"
  每天重置：date ≠ 今天 → 清空 completed_tickers

環境變量（GitHub Actions Secrets）：
  HF_TOKEN   → HF Dataset 讀寫
  HF_REPO_ID → HF Dataset repo
  MONGO_URI  → MongoDB 進度存取

Python 3.9 兼容。
"""

import os
import io
import re
import sys
import json
import base64
import time
from datetime import datetime
from typing import Optional, List

import requests
import pandas as pd
import pymongo
import pytz

# ─────────────────────────────────────────────
# 常數
# ─────────────────────────────────────────────

HF_REPO_ID    = os.getenv("HF_REPO_ID", "zhujun0511-AI/ai-telegram-bot-dataset")
HF_API_BASE   = "https://huggingface.co/api/datasets"
HF_TICKER_DIR = "mp_data/ticker"
HF_META_DIR   = "mp_data/meta"

BATCH_SIZE         = 200
SLEEP_AFTER_COMMIT = 2.0

EST_TZ       = pytz.timezone("US/Eastern")
PROGRESS_KEY = "mp_dow_structure"


# ─────────────────────────────────────────────
# 環境變量讀取
# ─────────────────────────────────────────────

def _hf_token() -> str:
    return os.getenv("HF_TOKEN", "")

def _hf_headers() -> dict:
    return {
        "Authorization": f"Bearer {_hf_token()}",
        "Content-Type":  "application/json",
    }

def _mongo_uri() -> str:
    return os.getenv("MONGO_URI", "")


# ─────────────────────────────────────────────
# MongoDB 進度追蹤（與 run_mp_indicator_calc.py 結構一致）
# ─────────────────────────────────────────────

class ProgressDB:
    def __init__(self):
        uri = _mongo_uri()
        if not uri:
            raise RuntimeError("MONGO_URI 未設定")
        self.client = pymongo.MongoClient(uri)
        self.col    = self.client["StockData"]["System_State"]

    def _today(self) -> str:
        return datetime.now(EST_TZ).strftime("%Y-%m-%d")

    def _now_iso(self) -> str:
        return datetime.now(EST_TZ).strftime("%Y-%m-%dT%H:%M:%S")

    def get(self) -> dict:
        doc = self.col.find_one({"id": PROGRESS_KEY})
        return doc or {}

    def reset_if_new_day(self, all_tickers: list) -> dict:
        today = self._today()
        doc   = self.get()
        if doc.get("date") != today:
            print(f"📅 [進度] 新的一天（{today}），重置進度，共 {len(all_tickers)} 個 ticker")
            new_doc = {
                "id":                PROGRESS_KEY,
                "date":              today,
                "status":            "running",
                "all_tickers":       all_tickers,
                "total_tickers":     len(all_tickers),
                "completed_tickers": [],
                "completed_count":   0,
                "last_updated":      self._now_iso(),
            }
            self.col.update_one(
                {"id": PROGRESS_KEY},
                {"$set": new_doc},
                upsert=True,
            )
            return new_doc
        else:
            print(f"📅 [進度] 今天（{today}），斷點續跑，"
                  f"已完成 {doc.get('completed_count', 0)}/{doc.get('total_tickers', 0)}")
            return doc

    def save_all_tickers(self, all_tickers: list):
        self.col.update_one(
            {"id": PROGRESS_KEY},
            {"$set": {
                "all_tickers":   all_tickers,
                "total_tickers": len(all_tickers),
                "last_updated":  self._now_iso(),
            }},
            upsert=True,
        )

    def update_completed(self, completed: set):
        self.col.update_one(
            {"id": PROGRESS_KEY},
            {"$set": {
                "completed_tickers": sorted(list(completed)),
                "completed_count":   len(completed),
                "status":            "running",
                "last_updated":      self._now_iso(),
            }},
            upsert=True,
        )

    def mark_done(self):
        self.col.update_one(
            {"id": PROGRESS_KEY},
            {"$set": {"status": "done", "last_updated": self._now_iso()}},
            upsert=True,
        )

    def close(self):
        self.client.close()


# ─────────────────────────────────────────────
# HF Dataset 工具
# ─────────────────────────────────────────────

def _hf_download(path: str) -> Optional[bytes]:
    url = f"https://huggingface.co/datasets/{HF_REPO_ID}/resolve/main/{path}"
    for attempt in range(2):
        try:
            resp = requests.get(
                url,
                headers={"Authorization": f"Bearer {_hf_token()}"},
                timeout=30,
            )
            if resp.status_code == 200:
                return resp.content
            if resp.status_code == 404:
                return None
            if resp.status_code == 429:
                print(f"⚠️ [HF] 下載 429，等待 70 秒重試: {path}")
                time.sleep(70)
                continue
            print(f"⚠️ [HF] 下載失敗 {resp.status_code}: {path}")
            return None
        except Exception as e:
            print(f"❌ [HF] 下載異常 {path}: {e}")
            return None
    return None


def _hf_batch_commit(files: list, message: str) -> bool:
    if not files:
        return True
    try:
        file_payloads = [
            {"path": f["path"], "content": f["content_b64"], "encoding": "base64"}
            for f in files
        ]
        url     = f"{HF_API_BASE}/{HF_REPO_ID}/commit/main"
        payload = {"summary": message, "files": file_payloads}
        for attempt in range(2):
            resp = requests.post(url, headers=_hf_headers(), json=payload, timeout=120)
            if resp.status_code in (200, 201):
                return True
            if resp.status_code == 429:
                print(f"⚠️ [HF] commit 429，等待 70 秒重試")
                time.sleep(70)
                continue
            print(f"❌ [HF] commit 失敗 {resp.status_code}: {resp.text[:200]}")
            return False
        return False
    except Exception as e:
        print(f"❌ [HF] commit 異常: {e}")
        return False


def _hf_scan_ticker_dirs() -> list:
    all_tickers = []
    next_url = (
        f"{HF_API_BASE}/{HF_REPO_ID}/tree/main/{HF_TICKER_DIR}"
        f"?recursive=false&expand=false"
    )
    page = 0
    while next_url:
        try:
            resp = requests.get(next_url, headers=_hf_headers(), timeout=30)
            if resp.status_code == 429:
                print(f"⚠️ [掃描] 429，等待 70 秒重試（第 {page+1} 頁）")
                time.sleep(70)
                resp = requests.get(next_url, headers=_hf_headers(), timeout=30)
            if resp.status_code != 200:
                print(f"❌ [掃描] 失敗 {resp.status_code}（第 {page+1} 頁）")
                break
            items = resp.json()
            if not isinstance(items, list):
                break
            page += 1
            dirs = [
                item["path"].split("/")[-1]
                for item in items
                if item.get("type") == "directory"
                and "_" not in item["path"].split("/")[-1]
            ]
            all_tickers.extend(dirs)
            print(f"  📄 掃描第 {page} 頁：{len(items)} items，"
                  f"{len(dirs)} 個 ticker，累計 {len(all_tickers)}")
            link = resp.headers.get("Link", "")
            m    = re.search(r'<([^>]+)>;\s*rel="next"', link)
            next_url = m.group(1) if m else None
        except Exception as e:
            print(f"❌ [掃描] 異常（第 {page+1} 頁）: {e}")
            break
    print(f"✅ [掃描] 完成：共 {len(all_tickers)} 個 ticker，{page} 頁")
    return all_tickers


# ─────────────────────────────────────────────
# daily_status.json 讀取
# ─────────────────────────────────────────────

def _load_daily_status() -> dict:
    raw = _hf_download(f"{HF_META_DIR}/daily_status.json")
    if raw is None:
        print("⚠️ [daily_status] 文件不存在，W1 道氏結構將完整計算（不截斷）")
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as e:
        print(f"⚠️ [daily_status] 解析失敗: {e}，W1 不截斷")
        return {}


# ─────────────────────────────────────────────
# 數據讀取與 resample
# ─────────────────────────────────────────────

def _read_d_csv(ticker: str) -> Optional[pd.DataFrame]:
    path = f"{HF_TICKER_DIR}/{ticker}/d.csv"
    raw  = _hf_download(path)
    if raw is None:
        return None
    try:
        df = pd.read_csv(io.BytesIO(raw))
        df.columns = [c.lower() for c in df.columns]
        required = {"date", "open", "high", "low", "close", "volume"}
        if not required.issubset(set(df.columns)):
            return None
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["open", "high", "low", "close"])
        df = df.sort_values("date").reset_index(drop=True)
        if len(df) < 20:
            return None
        return df
    except Exception as e:
        print(f"  ❌ {ticker}: d.csv 解析失敗: {e}")
        return None


def _resample_weekly(df_d: pd.DataFrame) -> pd.DataFrame:
    """日線 → 週線（W-MON 錨點，與 mp_reorganize 一致）。"""
    df = df_d.copy()
    df = df.set_index("date")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close", "volume"])
    resampled = df.resample("W-MON", label="left", closed="left").agg({
        "open":   "first",
        "high":   "max",
        "low":    "min",
        "close":  "last",
        "volume": "sum",
    }).dropna(subset=["open", "high", "low", "close"])
    resampled = resampled.reset_index()
    resampled["date"] = resampled["date"].dt.strftime("%Y-%m-%d")
    return resampled[["date", "open", "high", "low", "close", "volume"]]


# ─────────────────────────────────────────────
# 道氏結構算法（完整複製自 DC tasks/dow_structure.py）
# 禁止跨文件 import，必須在此處複製
# ─────────────────────────────────────────────

def _find_top_fractals(bars_raw: list) -> list:
    """
    掃描頂部 fractal。
    bars_raw：新在前，每個 bar {"h": float, "l": float, "t": str}。
    右側保護：range(2, n-3)（2026-07-10統一為跟CFET/DC dow_structure一致，
    防止組成分形的5根bar還沒走完就告警/回測誤判）。
    """
    n      = len(bars_raw)
    result = []
    for i in range(2, n - 3):
        if (bars_raw[i]["h"] > bars_raw[i-1]["h"] and
                bars_raw[i]["h"] > bars_raw[i-2]["h"] and
                bars_raw[i]["h"] > bars_raw[i+1]["h"] and
                bars_raw[i]["h"] > bars_raw[i+2]["h"]):
            result.append({"type": "H", "price": bars_raw[i]["h"],
                            "time": bars_raw[i]["t"], "bar_idx": i})
    return result


def _find_bot_fractals(bars_raw: list) -> list:
    """掃描底部 fractal，右側保護同 n-3（2026-07-10統一，理由同上）。"""
    n      = len(bars_raw)
    result = []
    for i in range(2, n - 3):
        if (bars_raw[i]["l"] < bars_raw[i-1]["l"] and
                bars_raw[i]["l"] < bars_raw[i-2]["l"] and
                bars_raw[i]["l"] < bars_raw[i+1]["l"] and
                bars_raw[i]["l"] < bars_raw[i+2]["l"]):
            result.append({"type": "L", "price": bars_raw[i]["l"],
                            "time": bars_raw[i]["t"], "bar_idx": i})
    return result


def _merge_and_deduplicate(tops: list, bots: list) -> list:
    """
    合併頂底 fractal，按 bar_idx 降序（舊在前），相鄰同向只保留最極端點。
    bar_idx 大 = 舊，降序排列後才能正確標記 HH/HL/LH/LL。
    """
    merged = sorted(tops + bots, key=lambda x: x["bar_idx"], reverse=True)
    if not merged:
        return []
    deduped = [merged[0]]
    for point in merged[1:]:
        last = deduped[-1]
        if last["type"] == point["type"]:
            # 同向，保留最極端
            if point["type"] == "H" and point["price"] > last["price"]:
                deduped[-1] = point
            elif point["type"] == "L" and point["price"] < last["price"]:
                deduped[-1] = point
        else:
            deduped.append(point)
    return deduped


def _label_swing_points(points: list) -> list:
    """
    標記 HH / HL / LH / LL。
    輸入必須是舊在前（_merge_and_deduplicate 輸出）。
    """
    labeled = []
    tops    = [p for p in points if p["type"] == "H"]
    bots    = [p for p in points if p["type"] == "L"]

    for point in points:
        p = dict(point)
        if point["type"] == "H":
            idx_in_tops = tops.index(point)
            if idx_in_tops == 0:
                p["label"] = "?"
            else:
                prev_top = tops[idx_in_tops - 1]
                p["label"] = "HH" if point["price"] > prev_top["price"] else "LH"
        else:
            idx_in_bots = bots.index(point)
            if idx_in_bots == 0:
                p["label"] = "?"
            else:
                prev_bot = bots[idx_in_bots - 1]
                p["label"] = "HL" if point["price"] > prev_bot["price"] else "LL"
        labeled.append(p)
    return labeled


def _determine_trend(labeled: list) -> dict:
    """根據最新頂+最新底的 label 組合判定趨勢。"""
    tops = [p for p in labeled if p["type"] == "H" and p.get("label") in ("HH", "LH")]
    bots = [p for p in labeled if p["type"] == "L" and p.get("label") in ("HL", "LL")]

    if not tops or not bots:
        return {"trend": "sideways", "reason": "擺動點不足"}

    latest_top = tops[-1]
    latest_bot = bots[-1]
    top_label  = latest_top["label"]
    bot_label  = latest_bot["label"]

    if top_label == "HH" and bot_label == "HL":
        trend  = "up"
        reason = f"HH({latest_top['price']:.2f}) + HL({latest_bot['price']:.2f})"
    elif top_label == "LH" and bot_label == "LL":
        trend  = "down"
        reason = f"LH({latest_top['price']:.2f}) + LL({latest_bot['price']:.2f})"
    else:
        trend  = "sideways"
        reason = f"{top_label}({latest_top['price']:.2f}) + {bot_label}({latest_bot['price']:.2f})"

    return {"trend": trend, "reason": reason}


def _calculate_dow_structure(bars_raw: list, timeframe: str) -> dict:
    """
    道氏趨勢結構計算主入口。
    bars_raw：新在前，每個 bar {"h": float, "l": float, "t": str}。
    """
    empty = {"timeframe": timeframe, "trend": "sideways",
              "structure": [], "reason": "數據不足"}
    if not bars_raw or len(bars_raw) < 10:
        return empty
    try:
        tops   = _find_top_fractals(bars_raw)
        bots   = _find_bot_fractals(bars_raw)
        merged = _merge_and_deduplicate(tops, bots)
        if len(merged) < 4:
            empty["reason"] = f"有效擺動點不足（{len(merged)} 個，需至少 4 個）"
            return empty
        labeled = _label_swing_points(merged)
        result  = _determine_trend(labeled)
        recent  = labeled[-10:]  # 最近 10 個點
        return {
            "timeframe": timeframe,
            "trend":     result["trend"],
            "structure": recent,
            "reason":    result["reason"],
        }
    except Exception as e:
        empty["reason"] = f"計算異常: {e}"
        return empty


def _df_to_bars_raw(df: pd.DataFrame) -> list:
    """
    DataFrame（舊在前，全名欄位）→ 新在前 bars_raw list。
    每個 bar：{"h": float, "l": float, "t": str}
    """
    bars = []
    for _, row in df.iloc[::-1].iterrows():  # 倒序 = 新在前
        bars.append({
            "h": float(row["high"]),
            "l": float(row["low"]),
            "t": str(row["date"])[:10],
        })
    return bars


# ─────────────────────────────────────────────
# 單 ticker 處理
# ─────────────────────────────────────────────

def _process_ticker(ticker: str,
                    today_str: str,
                    is_week_complete: bool) -> Optional[dict]:
    """
    處理單個 ticker，返回 {"path": ..., "content_b64": ...} 或 None（跳過）。
    """
    df_d = _read_d_csv(ticker)
    if df_d is None:
        print(f"  ⚠️ {ticker}: d.csv 缺失或不足，跳過")
        return None

    # D1 道氏結構
    bars_d = _df_to_bars_raw(df_d)
    res_d  = _calculate_dow_structure(bars_d, "D")

    # W1 道氏結構
    if is_week_complete:
        try:
            df_w   = _resample_weekly(df_d)
            bars_w = _df_to_bars_raw(df_w)
            res_w  = _calculate_dow_structure(bars_w, "W")
            w1_note = "完整"
        except Exception as e:
            res_w   = {"timeframe": "W", "trend": "sideways", "structure": [], "reason": f"計算異常:{e}"}
            w1_note = f"W1異常:{e}"
    else:
        # 截斷最後一根未收盤週線
        try:
            df_w = _resample_weekly(df_d)
            if len(df_w) > 1:
                df_w_trimmed = df_w.iloc[:-1].reset_index(drop=True)
                bars_w = _df_to_bars_raw(df_w_trimmed)
                res_w  = _calculate_dow_structure(bars_w, "W")
            else:
                res_w = {"timeframe": "W", "trend": "sideways", "structure": [], "reason": "週線不足"}
            w1_note = "截斷(partial)"
        except Exception as e:
            res_w   = {"timeframe": "W", "trend": "sideways", "structure": [], "reason": f"計算異常:{e}"}
            w1_note = f"W1異常:{e}"

    # 組裝 structure.json
    structure = {
        "dow_trend_d1":    res_d["trend"],
        "dow_trend_w1":    res_w["trend"] if is_week_complete else None,
        "dow_reason_d1":   res_d["reason"],
        "dow_reason_w1":   res_w["reason"] if is_week_complete else None,
        "structure_d1":    res_d["structure"],
        "structure_w1":    res_w["structure"] if is_week_complete else [],
        "is_week_complete": is_week_complete,
        "updated_at":      today_str,
    }

    content_bytes = json.dumps(structure, ensure_ascii=False, indent=2).encode("utf-8")
    content_b64   = base64.b64encode(content_bytes).decode()

    print(f"  ✅ {ticker} | D1:{res_d['trend']}({res_d['reason'][:30]}) | "
          f"W1:{res_w['trend'] if is_week_complete else 'SKIP'}({w1_note})")

    return {
        "path":        f"{HF_TICKER_DIR}/{ticker}/structure.json",
        "content_b64": content_b64,
    }


# ─────────────────────────────────────────────
# 主入口
# ─────────────────────────────────────────────

def main():
    start_time = time.monotonic()
    today_str  = datetime.now(EST_TZ).strftime("%Y-%m-%d")

    print(f"🚀 [mp_dow_structure] 開始 | 日期: {today_str}")

    # 初始化進度 DB
    try:
        pdb = ProgressDB()
    except Exception as e:
        print(f"❌ [mp_dow_structure] MongoDB 連接失敗: {e}")
        sys.exit(1)

    # 讀 daily_status.json
    daily_status     = _load_daily_status()
    is_week_complete = daily_status.get("is_week_complete", True)
    if not daily_status:
        print("⚠️ [mp_dow_structure] STATUS_MISSING：W1 不截斷（保守處理）")
    else:
        print(f"📋 [mp_dow_structure] daily_status: "
              f"is_week_complete={is_week_complete} "
              f"last_w={daily_status.get('last_complete_w_date', 'N/A')}")

    # 讀或重置進度
    progress    = pdb.get()
    all_tickers = progress.get("all_tickers", [])

    if not all_tickers:
        print(f"📂 [mp_dow_structure] 首次運行，掃描 HF ticker 目錄...")
        all_tickers = _hf_scan_ticker_dirs()
        if not all_tickers:
            print("❌ [mp_dow_structure] 掃描結果為空，退出")
            pdb.close()
            sys.exit(1)
        pdb.save_all_tickers(all_tickers)
        print(f"✅ [mp_dow_structure] 掃描完成，共 {len(all_tickers)} 個 ticker")

    progress  = pdb.reset_if_new_day(all_tickers)
    completed = set(progress.get("completed_tickers", []))
    total     = len(all_tickers)

    print(f"📊 [mp_dow_structure] 總計 {total} 個 ticker，已完成 {len(completed)}")

    remaining = [t for t in all_tickers if t not in completed]
    print(f"▶️  [mp_dow_structure] 剩餘 {len(remaining)} 個 ticker 待處理")

    batch_num  = 0
    ok_count   = 0
    skip_count = 0
    err_count  = 0

    for i in range(0, len(remaining), BATCH_SIZE):
        batch      = remaining[i: i + BATCH_SIZE]
        batch_num += 1
        hf_files   = []
        batch_ok   = []
        batch_skip = []
        batch_err  = []

        print(f"\n🔄 [mp_dow_structure] 批次 {batch_num} | "
              f"ticker {i+1}~{min(i+BATCH_SIZE, len(remaining))}/{len(remaining)}")

        for ticker in batch:
            try:
                result = _process_ticker(ticker, today_str, is_week_complete)
                if result is not None:
                    hf_files.append(result)
                    batch_ok.append(ticker)
                else:
                    batch_skip.append(ticker)
            except Exception as e:
                print(f"  ❌ {ticker}: 異常: {e}")
                batch_err.append(ticker)

        if hf_files:
            commit_msg = (
                f"mp_dow_structure {today_str} "
                f"batch {batch_num} ({len(hf_files)} tickers)"
            )
            print(f"💾 [mp_dow_structure] commit {len(hf_files)} 個文件...")
            commit_ok = _hf_batch_commit(hf_files, commit_msg)
            if commit_ok:
                print(f"✅ [mp_dow_structure] commit 成功")
                completed.update(batch_ok)
                ok_count   += len(batch_ok)
                skip_count += len(batch_skip)
                err_count  += len(batch_err)
                pdb.update_completed(completed)
                time.sleep(SLEEP_AFTER_COMMIT)
            else:
                print(f"❌ [mp_dow_structure] commit 失敗，本批進度不推進")
        else:
            completed.update(batch_ok + batch_skip)
            skip_count += len(batch_skip)
            err_count  += len(batch_err)
            pdb.update_completed(completed)

        elapsed = time.monotonic() - start_time
        print(f"📊 [mp_dow_structure] 進度: {len(completed)}/{total} | "
              f"本批 OK:{len(batch_ok)} 跳過:{len(batch_skip)} ERR:{len(batch_err)} | "
              f"耗時 {elapsed:.0f}s")

    pdb.mark_done()
    pdb.close()

    elapsed_total = time.monotonic() - start_time
    print(f"\n✅ [mp_dow_structure] 全部完成 | "
          f"總計 {total} 個 ticker | "
          f"OK:{ok_count} 跳過:{skip_count} ERR:{err_count} | "
          f"耗時 {elapsed_total:.0f}s ({elapsed_total/60:.1f}min)")


if __name__ == "__main__":
    main()
