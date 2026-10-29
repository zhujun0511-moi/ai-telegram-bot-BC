# -*- coding: utf-8 -*-
"""
courier/mongo.py — BC 的 Mongo（2026-10-16「唯一的門」）

BC 原本沒有門：9 支腳本各自 pymongo.MongoClient(MONGO_URI)、自己拿 db 物件查。
現在連線與鑰匙只在這裡；門外腳本用「通用操作」請門代辦（查詢條件逐字照原處傳進來），
拿不到 client／db／collection 物件。查詢結果以生成器回傳（保留原本 cursor 的逐筆讀取）。

連線選項：原腳本有的用預設、有的帶 serverSelectionTimeoutMS（健康報告 8000、dataset 健檢 20000）
→ 腳本開頭呼叫 configure(...) 指定，同一支腳本內共用一條連線（原本也是一支一條或兩條同設定）。
strict=True 對應原寫法 os.environ["MONGO_URI"]（缺就 KeyError）。Python 3.9 相容。
"""
import pymongo
from pymongo.errors import DuplicateKeyError  # noqa: F401  門外捕捉用（原 from pymongo.errors import）

from courier import keys

ASCENDING = pymongo.ASCENDING
DESCENDING = pymongo.DESCENDING

_opts = {}
_strict = {"on": False}
_fallback = {"local": False}
_clients = {}


def configure(strict: bool = None, local_fallback: bool = None, **client_opts):
    """設定本支腳本的連線方式（在第一次查詢前呼叫）。
    local_fallback：環境變數沒有 MONGO_URI 時改用本機備援鑰匙檔（原 dataset_health_check 寫法；檔案只在門內讀）。"""
    if strict is not None:
        _strict["on"] = strict
    if local_fallback is not None:
        _fallback["local"] = local_fallback
    _opts.update(client_opts)


def _client():
    uri = keys.require("MONGO_URI") if _strict["on"] else keys.env("MONGO_URI", "")
    if not uri and _fallback["local"]:
        uri = keys.local_env_secret("MONGO_URI")   # 原 os.getenv("MONGO_URI") or env.get("MONGO_URI","")
    k = (uri, tuple(sorted(_opts.items())))
    if k not in _clients:
        _clients[k] = pymongo.MongoClient(uri, **_opts)
    return _clients[k]


def _col(db, col):
    return _client()[db][col]


def close_all():
    for c in list(_clients.values()):
        c.close()
    _clients.clear()


# ── 通用操作（參數原樣轉給 pymongo）──

def find_one(db, col, query=None, projection=None):
    return _col(db, col).find_one(query, projection)


def find(db, col, query=None, projection=None, sort=None, limit=None):
    cur = _col(db, col).find(query, projection)
    if sort is not None:
        # tuple＝原寫法 .sort(key, direction)；list＝.sort([(key, direction), ...])
        cur = cur.sort(*sort) if isinstance(sort, tuple) else cur.sort(sort)
    if limit is not None:
        cur = cur.limit(limit)
    for d in cur:
        yield d


def aggregate(db, col, pipeline):
    for d in _col(db, col).aggregate(pipeline):
        yield d


def update_one(db, col, query, update, upsert=False):
    return _col(db, col).update_one(query, update, upsert=upsert)


def insert_one(db, col, doc):
    return _col(db, col).insert_one(doc)


def find_one_and_update(db, col, query, update, upsert=False):
    return _col(db, col).find_one_and_update(query, update, upsert=upsert)


def delete_one(db, col, query):
    """2026-10-28 工單通道：刪除條件由呼叫端寫死（run_tickets.delete_own 只刪 from=datacenter）。"""
    return _col(db, col).delete_one(query)


def create_index(db, col, keys_, **kw):
    return _col(db, col).create_index(keys_, **kw)


def db_stats(db):
    return _client()[db].command("dbStats")


def ping():
    return _client().admin.command("ping")
