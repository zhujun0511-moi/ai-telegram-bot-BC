# -*- coding: utf-8 -*-
"""
data_map.py — DATA_MAP 資料地圖：dataset（HF）佈局的 read-first 契約（2026-09-05 定案）

設計文件：ai/script/腳本匯總/handoff_subsystem_dataset_map.md ｜ 登記：MongoDB_Standard_v3.md 六-D
定位：dataset 上「哪種資料在哪、什麼格式、誰寫了」的唯一真源。
  - 寫入方 update_data_map()：更新自己那格，written_by **必填**（無法匿名寫）。
  - 讀取方 resolve_from_map()：先讀地圖再抓 → 內部佈局改，只改地圖、消費端不動。

⚠️ vendored 純核心：canonical＝本檔（BC.p，dataset 主要寫入方）。之後逐字複製進
   BC / AC / comm-hub / 本地 Yahoo 工具，checksum audit 比對（比照 data_core 慣例）。
   ⛔ 改動先改 canonical、再同步全副本。
   ⛔ 不得 import center-specific（config/pymongo/motor/bar_validator）——保持跨 repo 通用。
   huggingface_hub 只在 IO 函數內 lazy import（純邏輯零依賴、可離線單測）。

實體佈局：per-product 小檔 _map/{product}.json（各寫入方寫自己檔＝消除單檔多寫競態）
          + 根 DATA_MAP.json 索引（低頻改）。

責任歸屬誠實邊界：written_by 是自報（軟，合作型內部夠用）；HF git 所有 bot 共用同一 HF_TOKEN、
   只分帳號不分模塊。問責主要靠 written_by（用戶認定小概率、接受）。
Python 3.9 相容。
"""
import json

MAP_SCHEMA_VERSION = "datamap_v1"
DEFAULT_MAP_DIR = "_map"
ROOT_INDEX_PATH = "DATA_MAP.json"
DEFAULT_WRITES_CAP = 10


# ══ 純核心（零 IO、可離線單測）══════════════════════════════════════

def _touched_keys(descriptor):
    """列出本次 descriptor 動到哪些格（top-level key；periods 展開成 periods.<pk>）。"""
    keys = []
    for k, v in descriptor.items():
        if k == "periods" and isinstance(v, dict):
            for pk in v.keys():
                keys.append("periods.%s" % pk)
        else:
            keys.append(k)
    return sorted(keys)


def merge_product_entry(existing, descriptor, written_by, now_str,
                        note="", writes_cap=DEFAULT_WRITES_CAP):
    """把 descriptor 併進某 product 的地圖格，蓋章 written_by/at，回新 dict（不改入參）。
    - periods 做「逐 period 合併」（只動 descriptor 給的 period，不清別人的 period）；
    - 其餘 top-level key 覆蓋；
    - provenance.writes 前插一筆 {by,at,note,touched}，上限 writes_cap（記錄「誰寫了哪格」）。
    誰都能寫任何格（非一格一主）；責任靠蓋章 + 稽核，不靠禁止。written_by 必填。"""
    if not written_by:
        raise ValueError("written_by 必填：地圖不允許匿名寫入")
    new = dict(existing or {})
    for k, v in descriptor.items():
        if k == "periods" and isinstance(v, dict):
            merged = dict(new.get("periods", {}))
            for pk, pv in v.items():
                merged[pk] = pv
            new["periods"] = merged
        else:
            new[k] = v
    prov = dict(new.get("provenance", {}))
    prov["last_written_by"] = written_by
    prov["last_written_at"] = now_str
    writes = list(prov.get("writes", []))
    writes.insert(0, {"by": written_by, "at": now_str,
                      "note": note or "", "touched": _touched_keys(descriptor)})
    prov["writes"] = writes[:writes_cap]
    new["provenance"] = prov
    return new


def build_root_index(product_names, retired, now_str, updated_by, map_dir=DEFAULT_MAP_DIR):
    """組根索引 DATA_MAP.json：product → 其小檔路徑。"""
    return {
        "schema_version": MAP_SCHEMA_VERSION,
        "updated_at": now_str,
        "updated_by": updated_by,
        "products": {name: "%s/%s.json" % (map_dir, name) for name in sorted(product_names)},
        "_retired": sorted(retired or []),
    }


def _now_est_str():
    """EST 牆鐘字串（比照 bars_archive / FractalRadar manifest 慣例）。"""
    import datetime as _dt
    import pytz
    return _dt.datetime.now(pytz.timezone("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S EST")


# ══ IO（huggingface_hub lazy import；vendored 時連同純核心一起帶）══════════

def _api(token):
    from huggingface_hub import HfApi
    return HfApi(token=token)


def _download_json(repo, path, token):
    """讀 dataset 上一個 JSON；不存在（404/EntryNotFound）回 None；其他錯往上拋。"""
    from huggingface_hub import hf_hub_download
    try:
        local = hf_hub_download(repo_id=repo, filename=path, repo_type="dataset", token=token)
    except Exception as e:
        name = type(e).__name__
        if name in ("EntryNotFoundError", "RepositoryNotFoundError") or "404" in str(e):
            return None
        raise
    with open(local, "r", encoding="utf-8") as f:
        return json.load(f)


def _upload_json(api, repo, path, obj, message):
    import io
    raw = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
    api.upload_file(path_or_fileobj=io.BytesIO(raw), path_in_repo=path,
                    repo_id=repo, repo_type="dataset", commit_message=message)


def update_data_map(repo, token, product, descriptor, written_by,
                    note="", map_dir=DEFAULT_MAP_DIR, retired=None):
    """寫入方 hook：原子更新某 product 的地圖格 + 確保根索引含此 product。
    written_by **必填**（無法匿名寫）。回傳新的 product doc。"""
    if not written_by:
        raise ValueError("written_by 必填：地圖不允許匿名寫入")
    api = _api(token)
    now = _now_est_str()
    prod_path = "%s/%s.json" % (map_dir, product)
    existing = _download_json(repo, prod_path, token)
    new_doc = merge_product_entry(existing, descriptor, written_by, now, note=note)
    _upload_json(api, repo, prod_path, new_doc, "data_map: %s <- %s" % (product, written_by))
    # 根索引：缺此 product、或 _retired 有變才補寫（低頻）
    idx = _download_json(repo, ROOT_INDEX_PATH, token) or {}
    products = dict(idx.get("products", {}))
    changed = product not in products
    products[product] = prod_path
    retired_final = idx.get("_retired", []) if retired is None else retired
    if retired is not None and set(idx.get("_retired", [])) != set(retired):
        changed = True
    if changed:
        idx2 = build_root_index(products.keys(), retired_final, now, written_by, map_dir=map_dir)
        _upload_json(api, repo, ROOT_INDEX_PATH, idx2, "data_map: index <- %s" % written_by)
    return new_doc


def resolve_from_map(repo, token, product, ticker=None, period=None, map_dir=DEFAULT_MAP_DIR):
    """讀取方：先讀地圖、回該 product 的 descriptor；給 ticker/period 則套 path_template 回實際路徑。
    回 dict：{"descriptor":..., "path":..(可選), "source":..(可選)}；找不到 product 回 {"descriptor":None}。"""
    prod_path = "%s/%s.json" % (map_dir, product)
    doc = _download_json(repo, prod_path, token)
    out = {"descriptor": doc}
    if not doc:
        return out
    tmpl = doc.get("path_template")
    if tmpl and ticker is not None:
        fmt = {"ticker": ticker}
        if period is not None:
            fmt["period"] = period
        try:
            out["path"] = tmpl.format(**fmt)
        except KeyError:
            pass
    if period is not None:
        pinfo = (doc.get("periods") or {}).get(period) or {}
        out["source"] = pinfo.get("source")
    return out
