# -*- coding: utf-8 -*-
"""
courier/openrouter.py — BC 呼叫 OpenRouter 免費模型的門（2026-10-18；歷史板塊問題股詳查專用）

⛔ 模型白名單例外：只准 Jun 10-18 親自指定的兩個模型（寫死），其他一律拒絕——全系統「免費模型白名單、
   禁中資、唯一過濾點在 AC」的規則不變；本檔是 Jun 批准的一次性例外（登記 MongoDB_Standard_v3 §六-D）。
送出內容只准公開資訊（代號、公司名、SIC 行業說明）。金鑰讀 BC 既有 secret OPENROUTER_API_KEY，只在門內讀、不印出。
Python 3.9 相容。
"""
import os
import time

import requests

from courier.keys import scrub

API = "https://openrouter.ai/api/v1/chat/completions"
ALLOWED_MODELS = ("nvidia/nemotron-3-ultra-550b-a55b:free", "google/gemma-4-31b-it:free")


def has_key() -> bool:
    return bool(os.environ.get("OPENROUTER_API_KEY", "").strip())


def chat(model: str, prompt: str, max_tokens: int = 4000, tries: int = 3, _log=print):
    """回 (文字, 狀態)。狀態＝ok／quota（額度用完，呼叫端應存進度後停）／error。"""
    if model not in ALLOWED_MODELS:
        raise ValueError("⛔ 模型不在 Jun 指定的白名單例外內：%s" % model)
    key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if not key:
        raise RuntimeError("缺 OPENROUTER_API_KEY")
    last = None
    for k in range(tries):
        try:
            r = requests.post(API, timeout=180, headers={"Authorization": "Bearer " + key,
                                                         "Content-Type": "application/json"},
                              json={"model": model, "messages": [{"role": "user", "content": prompt}],
                                    "max_tokens": max_tokens, "temperature": 0})
        except requests.RequestException as e:
            last = scrub(e)
            time.sleep(10 * (k + 1))
            continue
        if r.status_code == 429:
            body = r.text[:300]
            if "per-day" in body or "daily" in body.lower():
                return None, "quota"
            last = "429"
            time.sleep(30 * (k + 1))
            continue
        if r.status_code != 200:
            last = "HTTP %d" % r.status_code
            time.sleep(10 * (k + 1))
            continue
        j = r.json()
        try:
            return j["choices"][0]["message"]["content"], "ok"
        except (KeyError, IndexError, TypeError):
            last = "回應格式不對"
    _log("  ⚠️ [OpenRouter] %s 失敗：%s" % (model, last))
    return None, "error"
