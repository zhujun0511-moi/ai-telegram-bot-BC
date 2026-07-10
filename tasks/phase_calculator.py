"""
tasks/phase_calculator.py — 相位計算引擎（v2.4 MongoDB 候選池輪替）

v2.4 改動（本次新增）：
  問題根因：模型接替機制不健壯
    - env var 沒有在 yaml 傳入，層一永遠跳過
    - MongoDB 只設了 primary，backup 全 false，層二只有一個模型
    - 單一模型 429 → 三層全敗
    - 解決方案：完全移除 env var 層，改用 MongoDB 候選池輪替

  新設計：
    1. 從 CommData.Configs.free_models_registry 讀取完整候選池（21個模型）
    2. active_phase_primary=True 的模型優先嘗試
    3. 失敗（任何非 200 或解析失敗）→ 自動輪替下一個候選模型
    4. 全部失敗 → 靜默放棄，本批 sector=None

  移除：
    - _get_sector_primary_model()（env var 方式）
    - _get_sector_backup_model()（env var 方式）
    - yaml 不再需要 AI_MODEL_SECTOR_PRIMARY / AI_MODEL_SECTOR_BACKUP

  新增：
    - PhaseCalcDB.fetch_sector_model_pool()：讀取完整候選池，active 的排前面
    - _query_sector_via_openrouter(tickers, db)：遍歷候選池直到成功或全敗

  告警訊息更新：
    - 改為列出嘗試過的模型數量，不再列具體模型名稱（候選池動態）

v2.3 改動（保留）：
  PhaseCalcDB 加入 comm_db（CommData 連線）
  fetch_mongo_models() → 已由 fetch_sector_model_pool() 取代

v2.2 改動（保留）：
  連續失敗 Telegram 提醒機制
  _send_telegram_alert() 走 AC /comm/send

v2.1 改動（保留）：
  Phase_History 寫入改為 upsert
  unknown_sector_tickers 條件修正

v2.0 改動（保留）：
  多輪自激發框架
  Round 2 大盤 + 板塊連動附加

v1.2 改動（保留）：
  HF 目錄掃描改為 cursor 翻頁

Python 3.9 兼容，不用 str | None。
"""


import os
import io
import re
import json
import base64
import requests
import pymongo
from datetime import datetime
from typing import Optional, List, Dict, Tuple
import pytz
import time
import pandas as pd

from tasks.outbound import notify as _notify_shared


# ─────────────────────────────────────────────
# 常數
# ─────────────────────────────────────────────


HF_REPO_ID    = os.getenv("HF_REPO_ID", "zhujun0511-AI/ai-telegram-bot-dataset")
HF_API_BASE   = "https://huggingface.co/api/datasets"
HF_TICKER_DIR = "mp_data/ticker"
HF_PHASE_DIR  = "phase_data"

EST_TZ = pytz.timezone("US/Eastern")

PHASE_CALC_BATCH_SIZE = 10

MA_PERIODS = {"M": 6, "W": 10, "D": 20}
SLOPE_LOOKBACK = {"M": 2, "W": 3, "D": 3}
SLOPE_THRESHOLD = 0.001

MAX_ROUND = 20

SECTOR_ETFS = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLB", "XLU", "XLRE", "XLC"]

OPENROUTER_API_BASE = "https://openrouter.ai/api/v1/chat/completions"

SECTOR_FAIL_ALERT_THRESHOLD = 5


# ─────────────────────────────────────────────
# 環境變量動態讀取
# ─────────────────────────────────────────────


def _get_hf_token() -> str:
    return os.getenv("HF_TOKEN", "")


def _get_mongo_uri() -> str:
    return os.getenv("MONGO_URI", "")


def _get_openrouter_key() -> str:
    return os.getenv("OPENROUTER_API_KEY", "")


def _hf_headers() -> dict:
    return {
        "Authorization": f"Bearer {_get_hf_token()}",
        "Content-Type": "application/json",
    }


# ─────────────────────────────────────────────
# Telegram 提醒（連續失敗用）
# ─────────────────────────────────────────────


def _send_telegram_alert(text: str) -> bool:
    """2026-07-10改用 tasks/outbound.py 統一出口，重試行為不變（3次+2秒間隔）。"""
    return _notify_shared(text, report_type="brief", retries=3, delay=2)


# ─────────────────────────────────────────────
# List3 解析（複製自 cfet_backtest，禁止 import）
# ─────────────────────────────────────────────


def _parse_list3_line(line: str) -> Tuple[Optional[str], List[str], str]:
    line = line.strip().upper()
    if ":" not in line:
        return None, [], "leader"
    sector_raw, rest = line.split(":", 1)
    sector_raw = sector_raw.strip()
    tickers    = [t.strip() for t in rest.split(",") if t.strip()]
    if sector_raw.endswith("_MID"):
        sector = sector_raw[:-4]
        tier   = "mid"
    else:
        sector = sector_raw
        tier   = "leader"
    return sector, tickers, tier


# ─────────────────────────────────────────────
# OpenRouter 板塊查詢
# ─────────────────────────────────────────────


def _call_openrouter(tickers: List[str], model: str) -> Optional[Dict[str, Optional[str]]]:
    """
    呼叫 OpenRouter 查詢 ticker → 板塊 ETF 映射。
    返回 {ticker: sector_etf_or_null}，失敗返回 None。
    """
    if not model:
        return None

    key = _get_openrouter_key()
    if not key:
        print("⚠️ OPENROUTER_API_KEY 未設定，跳過板塊查詢")
        return None

    ticker_list_str = ", ".join(tickers)
    etf_list_str    = ", ".join(SECTOR_ETFS)

    prompt = (
        f"Given these US stock tickers: {ticker_list_str}\n"
        f"For each ticker, determine which SPDR sector ETF it primarily belongs to.\n"
        f"You MUST only choose from: {etf_list_str}\n"
        f"If you are not sure, use null.\n"
        f"Respond ONLY with a valid JSON object mapping each ticker to its sector ETF or null.\n"
        f"Example: {{\"AAPL\": \"XLK\", \"XOM\": \"XLE\", \"UNKNWN\": null}}\n"
        f"No explanation, no markdown, just the JSON object."
    )

    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 300,
        "temperature": 0,
    }

    content = ""
    try:
        resp = requests.post(
            OPENROUTER_API_BASE,
            headers={
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"  ⚠️ OpenRouter [{model}] 失敗: {resp.status_code} {resp.text[:100]}")
            return None

        content = resp.json()["choices"][0]["message"]["content"].strip()
        content = re.sub(r"```[a-z]*", "", content).strip().strip("`")
        result  = json.loads(content)

        validated = {}
        for t, v in result.items():
            t_upper = t.upper()
            if v is None or (isinstance(v, str) and v.upper() in SECTOR_ETFS):
                validated[t_upper] = v.upper() if v else None
            else:
                print(f"  ⚠️ OpenRouter 返回無效板塊 {t}={v}，設為 null")
                validated[t_upper] = None
        return validated

    except json.JSONDecodeError as e:
        print(f"  ⚠️ OpenRouter [{model}] JSON 解析失敗: {e} | 原文: {content[:100]}")
        return None
    except Exception as e:
        print(f"  ⚠️ OpenRouter [{model}] 異常: {e}")
        return None


def _query_sector_via_openrouter(
    tickers: List[str],
    db: "PhaseCalcDB",
) -> Dict[str, Optional[str]]:
    """
    v2.4：MongoDB 候選池輪替。
    從 CommData.Configs.free_models_registry 讀取完整候選池，
    active_phase_primary=True 的模型排在最前面優先嘗試，
    失敗則自動輪替下一個，直到成功或全部耗盡。
    """
    if not tickers:
        return {}

    model_pool = db.fetch_sector_model_pool()

    if not model_pool:
        print(f"  ⚠️ 候選池為空（MongoDB 未設定任何模型），跳過板塊查詢")
        return {}

    print(f"  🔍 OpenRouter 板塊查詢，候選池共 {len(model_pool)} 個模型...")

    for i, model_id in enumerate(model_pool):
        print(f"  🔄 嘗試模型 [{i+1}/{len(model_pool)}] {model_id}...")
        result = _call_openrouter(tickers, model_id)
        if result is not None:
            print(f"  ✅ [{model_id}] 查詢成功：{len(result)} 個 ticker")
            return result

    print(f"  ❌ 候選池 {len(model_pool)} 個模型全部失敗，本批 {len(tickers)} 個 ticker sector=None")
    return {}


# ─────────────────────────────────────────────
# HF Dataset 讀寫工具
# ─────────────────────────────────────────────


def _hf_download_file(path: str) -> Optional[bytes]:
    url = f"https://huggingface.co/datasets/{HF_REPO_ID}/resolve/main/{path}"
    for attempt in range(2):
        try:
            resp = requests.get(
                url,
                headers={"Authorization": f"Bearer {_get_hf_token()}"},
                timeout=30
            )
            if resp.status_code == 200:
                return resp.content
            if resp.status_code == 404:
                return None
            if resp.status_code == 429:
                print(f"⚠️ HF 下載 429 rate limit（{path}），等待 70 秒後重試...")
                time.sleep(70)
                continue
            print(f"⚠️ HF 下載失敗 {path}: {resp.status_code}")
            return None
        except Exception as e:
            print(f"❌ HF 下載異常 {path}: {e}")
            return None
    print(f"❌ HF 下載 {path}：重試後仍失敗（rate limit）")
    return None


def _hf_batch_commit(files: List[dict], commit_message: str = "phase_calc batch") -> bool:
    if not files:
        return True
    try:
        file_payloads = []
        for f in files:
            csv_bytes   = f["df"].to_csv(index=False).encode("utf-8")
            content_b64 = base64.b64encode(csv_bytes).decode()
            file_payloads.append({
                "path":     f["path"],
                "content":  content_b64,
                "encoding": "base64",
            })

        url     = f"{HF_API_BASE}/{HF_REPO_ID}/commit/main"
        payload = {"summary": commit_message, "files": file_payloads}

        for attempt in range(2):
            resp = requests.post(url, headers=_hf_headers(), json=payload, timeout=120)
            if resp.status_code in (200, 201):
                return True
            if resp.status_code == 429:
                print(f"⚠️ HF commit 429 rate limit（{len(files)} 個文件），等待 70 秒後重試...")
                time.sleep(70)
                continue
            print(f"❌ HF batch commit 失敗: {resp.status_code} {resp.text[:200]}")
            return False

        print(f"❌ HF batch commit：重試後仍失敗（rate limit），本批進度不推進")
        return False

    except Exception as e:
        print(f"❌ HF batch commit 異常: {e}")
        return False


def _parse_next_url(link_header: str) -> Optional[str]:
    if not link_header:
        return None
    for part in link_header.split(","):
        part = part.strip()
        m = re.match(r'<([^>]+)>;\s*rel="next"', part)
        if m:
            return m.group(1)
    return None


def _hf_scan_all_tickers(prefix: str) -> dict:
    all_tickers  = []
    total_items  = 0
    pages        = 0
    next_url     = f"{HF_API_BASE}/{HF_REPO_ID}/tree/main/{prefix}?recursive=false&expand=false"

    while next_url:
        try:
            resp = requests.get(next_url, headers=_hf_headers(), timeout=30)
            if resp.status_code == 429:
                print(f"⚠️ HF 目錄掃描 429（第 {pages+1} 頁），等待 70 秒後重試...")
                time.sleep(70)
                resp = requests.get(next_url, headers=_hf_headers(), timeout=30)
                if resp.status_code != 200:
                    print(f"❌ HF 目錄掃描重試後仍失敗: {resp.status_code}")
                    return {"tickers": all_tickers, "total_items": total_items,
                            "pages": pages, "error": True}

            if resp.status_code != 200:
                print(f"⚠️ HF 目錄掃描失敗（第 {pages+1} 頁）: {resp.status_code}")
                return {"tickers": all_tickers, "total_items": total_items,
                        "pages": pages, "error": True}

            items = resp.json()
            if not isinstance(items, list):
                break

            pages       += 1
            total_items += len(items)

            dirs = [
                item["path"].split("/")[-1]
                for item in items
                if item.get("type") == "directory"
                and "_" not in item["path"].split("/")[-1]
            ]
            all_tickers.extend(dirs)

            print(f"  📄 第 {pages} 頁：{len(items)} items，{len(dirs)} 個 ticker，"
                  f"累計 {len(all_tickers)} 個")

            next_url = _parse_next_url(resp.headers.get("Link", ""))

        except Exception as e:
            print(f"❌ HF 目錄掃描異常（第 {pages+1} 頁）: {e}")
            return {"tickers": all_tickers, "total_items": total_items,
                    "pages": pages, "error": True}

    return {
        "tickers":     all_tickers,
        "total_items": total_items,
        "pages":       pages,
        "error":       False,
    }


# ─────────────────────────────────────────────
# 數據加載
# ─────────────────────────────────────────────


def load_bars(ticker: str, timeframe: str) -> Optional[pd.DataFrame]:
    tf_map = {"M": "m", "W": "w", "D": "d"}
    tf_key = tf_map.get(timeframe)
    if not tf_key:
        return None

    path = f"{HF_TICKER_DIR}/{ticker}/{tf_key}.csv"
    raw  = _hf_download_file(path)
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

        min_bars = MA_PERIODS[timeframe] + SLOPE_LOOKBACK[timeframe] + 10
        if len(df) < min_bars:
            return None

        return df

    except Exception as e:
        print(f"❌ 解析 {path} 失敗: {e}")
        return None


def load_phase_csv(ticker: str) -> Optional[pd.DataFrame]:
    path = f"{HF_PHASE_DIR}/{ticker}/phase.csv"
    raw  = _hf_download_file(path)
    if raw is None:
        return None

    try:
        df = pd.read_csv(io.BytesIO(raw))
        df.columns = [c.lower() for c in df.columns]
        if "date" not in df.columns or "timeframe" not in df.columns or "new_phase" not in df.columns:
            return None
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=["date"])
        df = df.sort_values("date").reset_index(drop=True)
        return df
    except Exception as e:
        print(f"❌ 解析 phase.csv {ticker} 失敗: {e}")
        return None


def get_phase_at_date(phase_df: pd.DataFrame, date_str: str, timeframe: str) -> str:
    if phase_df is None or phase_df.empty:
        return "UNKNOWN"
    sub = phase_df[
        (phase_df["timeframe"] == timeframe) &
        (phase_df["date"] <= date_str)
    ]
    if sub.empty:
        return "UNKNOWN"
    return str(sub.iloc[-1]["new_phase"])


# ─────────────────────────────────────────────
# 分形計算
# ─────────────────────────────────────────────


def _calc_top_fractals(df: pd.DataFrame) -> pd.Series:
    highs  = df["high"].values
    n      = len(highs)
    result = [False] * n
    for i in range(2, n - 3):
        if (highs[i] > highs[i-1] and highs[i] > highs[i-2]
                and highs[i] > highs[i+1] and highs[i] > highs[i+2]):
            result[i] = True
    return pd.Series(result, index=df.index)


def _calc_bot_fractals(df: pd.DataFrame) -> pd.Series:
    lows   = df["low"].values
    n      = len(lows)
    result = [False] * n
    for i in range(2, n - 3):
        if (lows[i] < lows[i-1] and lows[i] < lows[i-2]
                and lows[i] < lows[i+1] and lows[i] < lows[i+2]):
            result[i] = True
    return pd.Series(result, index=df.index)


# ─────────────────────────────────────────────
# 相位判斷四要素
# ─────────────────────────────────────────────


def _calc_slope(ma_values: list, idx: int, lookback: int) -> str:
    if idx < lookback:
        return "flat"
    prev = ma_values[idx - lookback]
    curr = ma_values[idx]
    if prev <= 0:
        return "flat"
    slope = (curr - prev) / prev
    if slope > SLOPE_THRESHOLD:
        return "rising"
    if slope < -SLOPE_THRESHOLD:
        return "falling"
    return "flat"


def _calc_slope_value(ma_values: list, idx: int, lookback: int) -> float:
    if idx < lookback:
        return 0.0
    prev = ma_values[idx - lookback]
    curr = ma_values[idx]
    if prev <= 0:
        return 0.0
    return round((curr - prev) / prev, 6)


def _calc_structure(top_fractals_idx: List[int], bot_fractals_idx: List[int],
                    df: pd.DataFrame, up_to_idx: int) -> str:
    tops = [i for i in top_fractals_idx if i <= up_to_idx]
    bots = [i for i in bot_fractals_idx if i <= up_to_idx]

    if len(tops) < 2 or len(bots) < 2:
        return "mixed"

    top1, top2 = tops[-1], tops[-2]
    bot1, bot2 = bots[-1], bots[-2]

    hh = df["high"].iloc[top1] > df["high"].iloc[top2]
    hl = df["low"].iloc[bot1]  > df["low"].iloc[bot2]
    lh = df["high"].iloc[top1] < df["high"].iloc[top2]
    ll = df["low"].iloc[bot1]  < df["low"].iloc[bot2]

    if hh and hl:
        return "HH_HL"
    if lh and ll:
        return "LH_LL"
    return "mixed"


def _calc_last_fractal(top_fractals_idx: List[int], bot_fractals_idx: List[int],
                       up_to_idx: int) -> str:
    last_top = max((i for i in top_fractals_idx if i <= up_to_idx), default=-1)
    last_bot = max((i for i in bot_fractals_idx if i <= up_to_idx), default=-1)
    if last_top == -1 and last_bot == -1:
        return "none"
    if last_top > last_bot:
        return "top"
    return "bottom"


def _determine_phase(slope: str, price_vs_ma: str, structure: str, last_fractal: str) -> str:
    if (last_fractal == "bottom" and structure == "mixed"
            and price_vs_ma == "above" and slope in ("flat", "rising")):
        return "A1"
    if (structure == "HH_HL" and slope == "rising" and price_vs_ma == "above"):
        return "A2"
    if (last_fractal == "top" and structure == "HH_HL"
            and slope in ("flat", "rising") and price_vs_ma == "above"):
        return "A3"
    if (last_fractal == "top" and structure == "mixed"
            and price_vs_ma == "below" and slope in ("flat", "falling")):
        return "B1"
    if (structure == "LH_LL" and slope == "falling" and price_vs_ma == "below"):
        return "B2"
    if (last_fractal == "bottom" and structure == "LH_LL"
            and slope in ("flat", "falling") and price_vs_ma == "below"):
        return "B3"
    return "UNCLEAR"


# ─────────────────────────────────────────────
# 相位事件計算（單時間框架）
# ─────────────────────────────────────────────


def calc_phase_events(df: pd.DataFrame, timeframe: str) -> List[dict]:
    ma_period = MA_PERIODS[timeframe]
    slope_lb  = SLOPE_LOOKBACK[timeframe]

    df = df.copy()
    df["ma"] = df["close"].rolling(ma_period).mean()

    df["top_frac"] = _calc_top_fractals(df)
    df["bot_frac"] = _calc_bot_fractals(df)

    top_pos = [df.index.get_loc(i) for i in df.index[df["top_frac"]].tolist()]
    bot_pos = [df.index.get_loc(i) for i in df.index[df["bot_frac"]].tolist()]

    ma_values = df["ma"].values.tolist()
    closes    = df["close"].values.tolist()
    dates     = df["date"].tolist()

    events        = []
    current_phase = None
    start_pos     = ma_period + slope_lb + 5

    for pos in range(start_pos, len(df) - 2):
        if pd.isna(ma_values[pos]):
            continue

        slope        = _calc_slope(ma_values, pos, slope_lb)
        price_vs_ma  = "above" if closes[pos] > ma_values[pos] else "below"
        structure    = _calc_structure(top_pos, bot_pos, df, pos)
        last_fractal = _calc_last_fractal(top_pos, bot_pos, pos)

        new_phase = _determine_phase(slope, price_vs_ma, structure, last_fractal)

        if new_phase != "UNCLEAR" and new_phase != current_phase:
            date_str = dates[pos].strftime("%Y-%m-%d") if hasattr(dates[pos], "strftime") else str(dates[pos])[:10]
            events.append({
                "date":        date_str,
                "timeframe":   timeframe,
                "prev_phase":  current_phase if current_phase else "UNKNOWN",
                "new_phase":   new_phase,
                "indicators": {
                    "ma_slope":     _calc_slope_value(ma_values, pos, slope_lb),
                    "price_vs_ma":  price_vs_ma,
                    "fractal_type": last_fractal,
                    "structure":    structure,
                    "ma_value":     round(ma_values[pos], 4),
                    "close":        round(closes[pos], 4),
                },
            })
            current_phase = new_phase

    return events


# ─────────────────────────────────────────────
# 三層快照補充（ticker 自身）
# ─────────────────────────────────────────────


def _get_phase_at_date(events: List[dict], date_str: str) -> str:
    phase = "UNKNOWN"
    for ev in events:
        if ev["date"] <= date_str:
            phase = ev["new_phase"]
        else:
            break
    return phase


def enrich_with_context(all_events: List[dict],
                        events_m: List[dict],
                        events_w: List[dict],
                        events_d: List[dict]) -> List[dict]:
    enriched = []
    for ev in all_events:
        date_str = ev["date"]
        phase_m  = _get_phase_at_date(events_m, date_str)
        phase_w  = _get_phase_at_date(events_w, date_str)
        phase_d  = _get_phase_at_date(events_d, date_str)

        ev_copy = dict(ev)
        ev_copy["phase_M"] = phase_m
        ev_copy["phase_W"] = phase_w
        ev_copy["phase_D"] = phase_d
        ev_copy["pattern"] = f"M_{phase_m}_W_{phase_w}_D_{phase_d}"
        enriched.append(ev_copy)
    return enriched


# ─────────────────────────────────────────────
# Round 2 環境附加
# ─────────────────────────────────────────────


def enrich_with_environment(
    enriched: List[dict],
    spy_phase_df: Optional[pd.DataFrame],
    sector_etf: Optional[str],
    sector_phase_df: Optional[pd.DataFrame],
    current_round: int,
) -> List[dict]:
    result = []
    for ev in enriched:
        date_str = ev["date"]
        ev_copy  = dict(ev)
        ev_copy["round"] = current_round

        if spy_phase_df is not None:
            ev_copy["spy_phase_M"] = get_phase_at_date(spy_phase_df, date_str, "M")
            ev_copy["spy_phase_W"] = get_phase_at_date(spy_phase_df, date_str, "W")
            ev_copy["spy_phase_D"] = get_phase_at_date(spy_phase_df, date_str, "D")
        else:
            ev_copy["spy_phase_M"] = None
            ev_copy["spy_phase_W"] = None
            ev_copy["spy_phase_D"] = None

        ev_copy["sector_etf"] = sector_etf
        if sector_etf and sector_phase_df is not None:
            ev_copy["sector_phase_M"] = get_phase_at_date(sector_phase_df, date_str, "M")
            ev_copy["sector_phase_W"] = get_phase_at_date(sector_phase_df, date_str, "W")
            ev_copy["sector_phase_D"] = get_phase_at_date(sector_phase_df, date_str, "D")
        else:
            ev_copy["sector_phase_M"] = None
            ev_copy["sector_phase_W"] = None
            ev_copy["sector_phase_D"] = None

        result.append(ev_copy)
    return result


# ─────────────────────────────────────────────
# 存儲：HF phase.csv
# ─────────────────────────────────────────────

_PHASE_CSV_COLS_R1 = [
    "date", "timeframe", "prev_phase", "new_phase",
    "phase_M", "phase_W", "phase_D", "pattern",
    "ma_slope", "price_vs_ma", "fractal_type", "structure",
    "ma_value", "close",
]

_PHASE_CSV_COLS_R2 = _PHASE_CSV_COLS_R1 + [
    "round",
    "spy_phase_M", "spy_phase_W", "spy_phase_D",
    "sector_etf", "sector_phase_M", "sector_phase_W", "sector_phase_D",
]


def _events_to_df(events: List[dict], current_round: int) -> pd.DataFrame:
    cols = _PHASE_CSV_COLS_R2 if current_round >= 2 else _PHASE_CSV_COLS_R1
    rows = []
    for ev in events:
        ind = ev.get("indicators", {})
        row = {
            "date":         ev["date"],
            "timeframe":    ev["timeframe"],
            "prev_phase":   ev["prev_phase"],
            "new_phase":    ev["new_phase"],
            "phase_M":      ev.get("phase_M", ""),
            "phase_W":      ev.get("phase_W", ""),
            "phase_D":      ev.get("phase_D", ""),
            "pattern":      ev.get("pattern", ""),
            "ma_slope":     ind.get("ma_slope", ""),
            "price_vs_ma":  ind.get("price_vs_ma", ""),
            "fractal_type": ind.get("fractal_type", ""),
            "structure":    ind.get("structure", ""),
            "ma_value":     ind.get("ma_value", ""),
            "close":        ind.get("close", ""),
        }
        if current_round >= 2:
            row["round"]          = ev.get("round", current_round)
            row["spy_phase_M"]    = ev.get("spy_phase_M", "")
            row["spy_phase_W"]    = ev.get("spy_phase_W", "")
            row["spy_phase_D"]    = ev.get("spy_phase_D", "")
            row["sector_etf"]     = ev.get("sector_etf", "")
            row["sector_phase_M"] = ev.get("sector_phase_M", "")
            row["sector_phase_W"] = ev.get("sector_phase_W", "")
            row["sector_phase_D"] = ev.get("sector_phase_D", "")
        rows.append(row)
    df = pd.DataFrame(rows, columns=cols)
    return df


def _build_phase_file(ticker: str, events: List[dict], current_round: int) -> Optional[dict]:
    if not events:
        return None
    df   = _events_to_df(events, current_round)
    path = f"{HF_PHASE_DIR}/{ticker}/phase.csv"
    return {"path": path, "df": df}


# ─────────────────────────────────────────────
# 存儲：MongoDB Phase_History
# ─────────────────────────────────────────────


class PhaseCalcDB:
    """
    學習中心相位計算 DB 操作類 v2.4。

    v2.4 新增：
      self.comm_db — CommData database
      fetch_sector_model_pool() — 從 CommData.Configs.free_models_registry
        讀取完整候選池，active_phase_primary=True 的排最前面
    """

    RUN_ID = "phase_v1"

    def __init__(self):
        uri = _get_mongo_uri()
        if not uri:
            raise RuntimeError("MONGO_URI 未設定")
        self.client   = pymongo.MongoClient(uri)
        self.stock_db = self.client["StockData"]
        self.comm_db  = self.client["CommData"]
        self._setup_indices()

        self._list_tickers = set()
        self._sector_map   = {}
        self._load_list_tickers()
        self._load_sector_map()

    def _setup_indices(self):
        try:
            self.stock_db["Phase_History"].create_index(
                [("ticker", pymongo.ASCENDING),
                 ("timeframe", pymongo.ASCENDING),
                 ("date", pymongo.DESCENDING)],
                name="ticker_tf_date"
            )
            self.stock_db["Phase_History"].create_index(
                [("ticker", pymongo.ASCENDING),
                 ("timeframe", pymongo.ASCENDING),
                 ("date", pymongo.ASCENDING)],
                unique=True,
                name="unique_ticker_tf_date"
            )
            self.stock_db["Phase_Calc_Progress"].create_index("run_id", unique=True)
            self.stock_db["Ticker_Sector_Map"].create_index("ticker", unique=True)
            self.stock_db["Ticker_Sector_Conflicts"].create_index(
                [("ticker", pymongo.ASCENDING),
                 ("detected_at", pymongo.DESCENDING)]
            )
        except Exception as e:
            print(f"⚠️ 索引建立失敗（可能已存在）: {e}")

    def _load_list_tickers(self):
        cfg = self.stock_db["Configs"].find_one({"type": "ticker_lists"})
        if not cfg:
            print("⚠️ PhaseCalcDB: Configs 未找到，List ticker 集合為空")
            return

        lists = cfg.get("lists", {})
        seen  = set()

        def _add(t):
            seen.add(t.upper().strip())

        for t in lists.get("list_1", []):
            _add(t)
        for t in lists.get("list_2", []):
            _add(t)
        for line in lists.get("list_3", []):
            if ":" in line:
                _, rest = line.split(":", 1)
                for t in rest.split(","):
                    t = t.strip().upper()
                    if t:
                        seen.add(t)
        for t in lists.get("list_4", []):
            _add(t)

        self._list_tickers = seen
        print(f"✅ PhaseCalcDB: List ticker 集合加載完成，共 {len(seen)} 個")

    def _load_sector_map(self):
        cfg = self.stock_db["Configs"].find_one({"type": "ticker_lists"})
        if not cfg:
            print("⚠️ PhaseCalcDB: Configs 未找到，sector_map 為空")
            return

        sector_map = {}
        for line in cfg.get("lists", {}).get("list_3", []):
            sector, tickers, _ = _parse_list3_line(line)
            if sector and tickers:
                for t in tickers:
                    sector_map[t.upper()] = sector.upper()

        self._sector_map = sector_map
        print(f"✅ PhaseCalcDB: List3 sector_map 加載完成，共 {len(sector_map)} 個映射")

    def fetch_sector_model_pool(self) -> List[str]:
        """
        v2.4 核心：從 CommData.Configs.free_models_registry 讀取完整候選池。
        排序規則：
          1. active_phase_primary=True 的模型排最前面
          2. 其餘模型依原始順序排列
        返回 model_id 列表，空列表代表無可用模型。
        """
        try:
            doc = self.comm_db["Configs"].find_one({"type": "free_models_registry"})
            if not doc:
                print("  ⚠️ CommData.Configs: free_models_registry 不存在")
                return []

            models = doc.get("models", [])
            primary   = []
            secondary = []

            for m in models:
                mid = m.get("model_id", "").strip()
                if not mid:
                    continue
                if m.get("active_phase_primary"):
                    primary.append(mid)
                else:
                    secondary.append(mid)

            pool = primary + secondary
            print(f"  📋 候選池載入：{len(primary)} 個 primary + {len(secondary)} 個備選，共 {len(pool)} 個")
            return pool

        except Exception as e:
            print(f"  ⚠️ fetch_sector_model_pool 異常: {e}")
            return []

    def is_list_ticker(self, ticker: str) -> bool:
        return ticker.upper() in self._list_tickers

    def get_sector_etf(self, ticker: str) -> Optional[str]:
        t = ticker.upper()
        if t in self._sector_map:
            return self._sector_map[t]
        doc = self.stock_db["Ticker_Sector_Map"].find_one({"ticker": t})
        if doc:
            return doc.get("sector_etf")
        return None

    def save_sector_mapping(self, ticker: str, sector_etf: Optional[str], source: str = "openrouter"):
        t   = ticker.upper()
        col = self.stock_db["Ticker_Sector_Map"]
        existing = col.find_one({"ticker": t})

        if existing is None:
            col.insert_one({
                "ticker":      t,
                "sector_etf":  sector_etf,
                "source":      source,
                "conflict":    False,
                "created_at":  datetime.now(EST_TZ),
                "updated_at":  datetime.now(EST_TZ),
            })
        else:
            existing_val = existing.get("sector_etf")
            if existing_val != sector_etf:
                self.stock_db["Ticker_Sector_Conflicts"].insert_one({
                    "ticker":      t,
                    "original":    existing_val,
                    "new_value":   sector_etf,
                    "detected_at": datetime.now(EST_TZ),
                })
                col.update_one(
                    {"ticker": t},
                    {"$set": {"conflict": True, "updated_at": datetime.now(EST_TZ)}}
                )
                print(f"  ⚠️ 板塊映射衝突 {t}: {existing_val} vs {sector_etf}，已記錄，保留原值")

    def save_sector_mappings_batch(self, mappings: Dict[str, Optional[str]]):
        for ticker, sector_etf in mappings.items():
            try:
                self.save_sector_mapping(ticker, sector_etf)
            except Exception as e:
                print(f"  ⚠️ 保存板塊映射失敗 {ticker}: {e}")

    def get_sector_fail_streak(self) -> int:
        doc = self.get_progress()
        return doc.get("sector_fail_streak", 0)

    def update_sector_fail_streak(self, success: bool) -> int:
        col = self.stock_db["Phase_Calc_Progress"]
        new_streak = 0 if success else self.get_sector_fail_streak() + 1
        col.update_one(
            {"run_id": self.RUN_ID},
            {"$set": {
                "sector_fail_streak":        new_streak,
                "sector_fail_last_updated":  datetime.now(EST_TZ),
            }},
            upsert=True
        )
        return new_streak

    def save_phase_events(self, ticker: str, events: List[dict]):
        if not events:
            return
        col  = self.stock_db["Phase_History"]
        now  = datetime.now(EST_TZ)
        docs = []
        for ev in events:
            ind = ev.get("indicators", {})
            docs.append({
                "ticker":         ticker,
                "date":           ev["date"],
                "timeframe":      ev["timeframe"],
                "prev_phase":     ev["prev_phase"],
                "new_phase":      ev["new_phase"],
                "phase_M":        ev.get("phase_M", "UNKNOWN"),
                "phase_W":        ev.get("phase_W", "UNKNOWN"),
                "phase_D":        ev.get("phase_D", "UNKNOWN"),
                "pattern":        ev.get("pattern", ""),
                "round":          ev.get("round", 1),
                "spy_phase_M":    ev.get("spy_phase_M"),
                "spy_phase_W":    ev.get("spy_phase_W"),
                "spy_phase_D":    ev.get("spy_phase_D"),
                "sector_etf":     ev.get("sector_etf"),
                "sector_phase_M": ev.get("sector_phase_M"),
                "sector_phase_W": ev.get("sector_phase_W"),
                "sector_phase_D": ev.get("sector_phase_D"),
                "indicators":     ind,
                "created_at":     now,
            })

        if not docs:
            return
        try:
            upserted = 0
            modified = 0
            for doc in docs:
                r = col.update_one(
                    {"ticker": doc["ticker"], "timeframe": doc["timeframe"], "date": doc["date"]},
                    {"$set": doc},
                    upsert=True,
                )
                if r.upserted_id:
                    upserted += 1
                elif r.modified_count:
                    modified += 1
            print(f"  📝 Phase_History upsert {ticker}: 新增 {upserted} 條，更新 {modified} 條")
        except Exception as e:
            print(f"  ❌ Phase_History 寫入 {ticker} 異常: {e}")

    def get_progress(self) -> dict:
        col = self.stock_db["Phase_Calc_Progress"]
        doc = col.find_one({"run_id": self.RUN_ID})
        return doc or {}

    def is_done(self) -> bool:
        doc = self.get_progress()
        return doc.get("status") in ("done", "all_rounds_done")

    def get_current_round(self) -> int:
        return self.get_progress().get("current_round", 1)

    def get_max_round(self) -> int:
        return self.get_progress().get("max_round", MAX_ROUND)

    def advance_round(self) -> str:
        col = self.stock_db["Phase_Calc_Progress"]
        doc = self.get_progress()

        current_round = doc.get("current_round", 1)
        max_round     = doc.get("max_round", MAX_ROUND)
        history       = doc.get("round_history", [])

        history.append({
            "round":        current_round,
            "completed_at": datetime.now(EST_TZ).isoformat(),
            "total":        doc.get("total_tickers", 0),
        })

        next_round = current_round + 1
        if next_round > max_round:
            col.update_one(
                {"run_id": self.RUN_ID},
                {"$set": {
                    "status":        "all_rounds_done",
                    "round_history": history,
                    "last_updated":  datetime.now(EST_TZ),
                }},
                upsert=True
            )
            print(f"🏁 全部 {max_round} 輪相位計算完成")
            return "ALL_ROUNDS_DONE"
        else:
            col.update_one(
                {"run_id": self.RUN_ID},
                {"$set": {
                    "current_round":     next_round,
                    "completed_tickers": [],
                    "completed_count":   0,
                    "status":            "running",
                    "round_history":     history,
                    "last_updated":      datetime.now(EST_TZ),
                }},
                upsert=True
            )
            print(f"🔄 第 {current_round} 輪完成，進入第 {next_round} 輪")
            return "NEXT_ROUND"

    def is_all_rounds_done(self) -> bool:
        return self.get_progress().get("status") == "all_rounds_done"

    def mark_done(self):
        col = self.stock_db["Phase_Calc_Progress"]
        col.update_one(
            {"run_id": self.RUN_ID},
            {"$set": {"status": "done", "last_updated": datetime.now(EST_TZ)}},
            upsert=True
        )

    def reset_progress(self):
        col = self.stock_db["Phase_Calc_Progress"]
        col.delete_one({"run_id": self.RUN_ID})
        print(f"🔄 Phase_Calc_Progress 已重置")

    def get_scan_status(self) -> str:
        return self.get_progress().get("scan_status", "scanning")

    def save_scanned_tickers(self, tickers: List[str]):
        col = self.stock_db["Phase_Calc_Progress"]
        col.update_one(
            {"run_id": self.RUN_ID},
            {"$set": {
                "scan_status":   "calculating",
                "all_tickers":   tickers,
                "total_tickers": len(tickers),
                "current_round": 1,
                "max_round":     MAX_ROUND,
                "round_history": [],
                "status":        "running",
                "last_updated":  datetime.now(EST_TZ),
            }},
            upsert=True
        )
        print(f"✅ 目錄掃描完成，共 {len(tickers)} 個 ticker，切換到計算階段")

    def reset_scan(self):
        col = self.stock_db["Phase_Calc_Progress"]
        col.update_one(
            {"run_id": self.RUN_ID},
            {"$set": {
                "scan_status":   "scanning",
                "all_tickers":   [],
                "total_tickers": 0,
                "last_updated":  datetime.now(EST_TZ),
            }},
            upsert=True
        )
        print(f"🔄 掃描進度已重置（計算進度保留）")

    def get_cached_tickers(self) -> List[str]:
        return self.get_progress().get("all_tickers", [])

    def get_completed_tickers(self) -> set:
        return set(self.get_progress().get("completed_tickers", []))

    def get_total_tickers(self) -> int:
        return self.get_progress().get("total_tickers", 0)

    def update_progress(self, completed_tickers: set):
        col   = self.stock_db["Phase_Calc_Progress"]
        tlist = sorted(list(completed_tickers))
        col.update_one(
            {"run_id": self.RUN_ID},
            {"$set": {
                "completed_tickers": tlist,
                "completed_count":   len(tlist),
                "status":            "running",
                "last_updated":      datetime.now(EST_TZ),
            }},
            upsert=True
        )


# ─────────────────────────────────────────────
# 單 ticker 計算邏輯
# ─────────────────────────────────────────────


def calc_phase_for_ticker(
    ticker: str,
    db: PhaseCalcDB,
    current_round: int,
    spy_phase_df: Optional[pd.DataFrame],
    sector_phase_cache: Dict[str, Optional[pd.DataFrame]],
) -> Optional[List[dict]]:
    df_m = load_bars(ticker, "M")
    df_w = load_bars(ticker, "W")
    df_d = load_bars(ticker, "D")

    if df_m is None or df_w is None or df_d is None:
        missing = [tf for tf, df in [("M", df_m), ("W", df_w), ("D", df_d)] if df is None]
        print(f"  ⏭️  {ticker}: 數據缺失 {missing}，跳過")
        return None

    try:
        events_m = calc_phase_events(df_m, "M")
        events_w = calc_phase_events(df_w, "W")
        events_d = calc_phase_events(df_d, "D")
    except Exception as e:
        print(f"  ❌ {ticker}: 計算相位事件異常: {e}")
        return None

    all_events = events_m + events_w + events_d

    if all_events:
        all_events_sorted = sorted(all_events, key=lambda x: (x["date"], x["timeframe"]))
        enriched = enrich_with_context(all_events_sorted, events_m, events_w, events_d)
    else:
        enriched = []

    if current_round >= 2 and enriched:
        sector_etf      = db.get_sector_etf(ticker)
        sector_phase_df = sector_phase_cache.get(sector_etf) if sector_etf else None
        enriched = enrich_with_environment(
            enriched,
            spy_phase_df    = spy_phase_df,
            sector_etf      = sector_etf,
            sector_phase_df = sector_phase_df,
            current_round   = current_round,
        )

    if enriched and db.is_list_ticker(ticker):
        db.save_phase_events(ticker, enriched)

    print(f"  ✅ {ticker}: M={len(events_m)} W={len(events_w)} D={len(events_d)} "
          f"共{len(enriched)}條 R{current_round}")
    return enriched


# ─────────────────────────────────────────────
# 批次主入口
# ─────────────────────────────────────────────


def run_phase_calc_batch(db: PhaseCalcDB) -> dict:
    """
    批次執行相位計算（v2.4 MongoDB 候選池輪替）。
    """
    if db.is_done():
        return {"status": "ALREADY_DONE"}

    scan_status   = db.get_scan_status()
    current_round = db.get_current_round()

    # ═══════════════════════════════
    # 階段一：SCANNING
    # ═══════════════════════════════
    if scan_status == "scanning":
        print(f"📂 目錄掃描開始（cursor 翻頁模式）...")

        result = _hf_scan_all_tickers(HF_TICKER_DIR)

        if result["error"] and len(result["tickers"]) == 0:
            print(f"⚠️ 掃描失敗且無數據，下次重試")
            return {"status": "SCAN_ERROR"}

        tickers = result["tickers"]
        print(f"✅ 掃描完成：{result['pages']} 頁，{result['total_items']} items，"
              f"{len(tickers)} 個有效 ticker")

        if len(tickers) == 0:
            print("❌ 掃描完成但 ticker 列表為空")
            return {"status": "SCAN_ERROR"}

        db.save_scanned_tickers(tickers)
        scan_status = "calculating"

    # ═══════════════════════════════
    # 階段二：CALCULATING
    # ═══════════════════════════════
    completed   = db.get_completed_tickers()
    all_tickers = db.get_cached_tickers()

    if not all_tickers:
        print("⚠️ ticker 列表為空（掃描狀態異常），重置掃描")
        db.reset_scan()
        return {"status": "SCAN_ERROR"}

    remaining = [t for t in all_tickers if t not in completed]

    if not remaining:
        advance_result = db.advance_round()
        if advance_result == "ALL_ROUNDS_DONE":
            return {
                "status":          "ALL_ROUNDS_DONE",
                "round":           current_round,
                "total_tickers":   len(all_tickers),
                "completed_count": len(completed),
            }
        else:
            return {
                "status":          "ALL_DONE",
                "round":           current_round,
                "next_round":      db.get_current_round(),
                "total_tickers":   len(all_tickers),
                "completed_count": len(completed),
            }

    batch = remaining[:PHASE_CALC_BATCH_SIZE]
    print(f"🔄 R{current_round} 相位計算 | "
          f"已完成 {len(completed)}/{len(all_tickers)} | 本批: {len(batch)} 個")

    # ── Round 2：預加載 SPY + 板塊 ETF phase.csv ──
    spy_phase_df       = None
    sector_phase_cache = {}

    if current_round >= 2:
        print(f"  📥 R2：加載 SPY phase.csv...")
        spy_phase_df = load_phase_csv("SPY")
        if spy_phase_df is None:
            print(f"  ⚠️ SPY phase.csv 不存在，SPY 相位將為 UNKNOWN")

        needed_sectors = set()
        for ticker in batch:
            sector = db.get_sector_etf(ticker)
            if sector:
                needed_sectors.add(sector)

        for sector in needed_sectors:
            if sector not in sector_phase_cache:
                print(f"  📥 加載 {sector} phase.csv...")
                sector_phase_cache[sector] = load_phase_csv(sector)

    # ── 計算本批 ──
    hf_files              = []
    batch_success         = []
    unknown_sector_tickers = []

    for ticker in batch:
        try:
            enriched = calc_phase_for_ticker(
                ticker, db, current_round, spy_phase_df, sector_phase_cache
            )

            if current_round >= 2:
                sector = db.get_sector_etf(ticker)
                if sector is None and ticker not in db._sector_map:
                    cached = db.stock_db["Ticker_Sector_Map"].find_one(
                        {"ticker": ticker.upper()}
                    )
                    if cached is None:
                        unknown_sector_tickers.append(ticker)

            if enriched is not None and len(enriched) > 0:
                file_entry = _build_phase_file(ticker, enriched, current_round)
                if file_entry:
                    hf_files.append(file_entry)
            batch_success.append(ticker)
        except Exception as e:
            print(f"  ❌ {ticker} 異常（跳過）: {e}")
            batch_success.append(ticker)

    # ── HF commit ──
    commit_ok = True
    if hf_files:
        print(f"📤 HF batch commit：{len(hf_files)} 個文件...")
        commit_ok = _hf_batch_commit(
            hf_files,
            commit_message=f"phase_calc R{current_round} batch {len(hf_files)} tickers"
        )

    if not commit_ok:
        print(f"⚠️ HF commit 失敗，本批進度不推進，下次重試（{len(batch)} 個 ticker）")
        return {
            "status":          "COMMIT_FAILED",
            "round":           current_round,
            "batch_size":      len(batch),
            "completed_count": len(completed),
            "total_tickers":   len(all_tickers),
        }

    # ── 推進進度 ──
    completed.update(batch_success)
    db.update_progress(completed)

    new_remaining = len(all_tickers) - len(completed)
    print(f"💾 R{current_round} 進度保存：已完成 {len(completed)}/{len(all_tickers)}，剩餘 {new_remaining}")

    # ── Round 2：OpenRouter 候選池查詢（v2.4）──
    if current_round >= 2 and unknown_sector_tickers:
        print(f"  🔍 OpenRouter 查詢 {len(unknown_sector_tickers)} 個未知板塊 ticker...")
        mappings = _query_sector_via_openrouter(unknown_sector_tickers, db)

        if mappings:
            db.save_sector_mappings_batch(mappings)
            print(f"  💾 板塊映射已保存：{len(mappings)} 個")
            db.update_sector_fail_streak(success=True)
        else:
            streak = db.update_sector_fail_streak(success=False)
            print(f"  ⚠️ 本批 OpenRouter 板塊查詢完全失敗，連續失敗 batch 數：{streak}")

            if streak >= SECTOR_FAIL_ALERT_THRESHOLD:
                alert_text = (
                    f"⚠️ OpenClaw phase_calc 板塊分類連續失敗\n"
                    f"連續 {streak} 個 batch，候選池所有模型均失敗\n"
                    f"輪次: R{current_round}\n"
                    f"建議：至 AC 模型管理介面更新 free_models_registry，"
                    f"或檢查 OPENROUTER_API_KEY 是否有效"
                )
                _send_telegram_alert(alert_text)
                db.update_sector_fail_streak(success=True)

    return {
        "status":          "BATCH_DONE",
        "round":           current_round,
        "completed_count": len(completed),
        "total_tickers":   len(all_tickers),
        "remaining":       new_remaining,
        "batch_success":   len(batch_success),
        "hf_files":        len(hf_files),
    }
