# -*- coding: utf-8 -*-
"""
bc_secutil.py — 密鑰遮蔽（2026-08-06 全域密鑰遮蔽，見 DANGER_ZONES 2026-08-05 §5）

洩漏機制：對含 apiKey= 的 Polygon URL 發請求，連線例外的 str(e) 會帶出整條含密鑰的 URL；
_log/print 這條 {e} 就把密鑰寫進 GHA log。scrub() 是全域出口遮蔽：凡會進 log 的例外訊息、URL 先過它。
（各中心獨立部署，本檔為 DC config.scrub / AC / comm-hub / BC.p 同一份實作的 BC 副本。）
"""
import os
import re

_SECRET_ENV_NAMES = (
    "POLYGON_KEY", "TWELVE_KEY", "TWELVE_KEY_2", "MONGO_URI",
    "GH_TOKEN", "HF_TOKEN", "WEBHOOK_SECRET", "MP_API_KEY",
)
_QS_SECRET_RE = re.compile(
    r'(?i)(apikey|api_key|secret|token|password|pwd|access_token|sig|signature)=([^&\s"\'#]+)'
)
_MONGO_CRED_RE = re.compile(r'(mongodb(?:\+srv)?://)[^:/@\s]+:[^@\s]+@')


def scrub(s):
    """遮蔽字串中的密鑰值，避免密鑰外洩到 GHA log。
    遮蔽：① query-param 密鑰（apikey/secret/token…=值）② mongodb URI 帳密 ③ 已知 env 密鑰值。
    None/非字串安全處理。"""
    if s is None:
        return s
    try:
        s = str(s)
    except Exception:
        return "***"
    # env 精確值先替換（含特殊字元/密碼含 @ 也整段遮；必須在正則前，否則正則改寫後精確比對失配）
    for _name in _SECRET_ENV_NAMES:
        _val = os.getenv(_name, "")
        if _val and len(_val) >= 8:
            s = s.replace(_val, "***")
    s = _QS_SECRET_RE.sub(r'\1=***', s)
    s = _MONGO_CRED_RE.sub(r'\1***:***@', s)
    return s


# ─────────────────────────────────────────────
# 2026-10-16「唯一的門」：BC 的鑰匙與對外地址只在 courier/ 讀（本檔原為 bc_secutil.py）。
# courier/ 以外不得讀秘密類環境變數（tools/door_audit.py 會擋）；鑰匙值不離開門。
# 讀取語意逐字照原處：.strip() 與否、預設值、os.environ[...]（缺就 KeyError）都保留。
# ─────────────────────────────────────────────

def env(name: str, default: str = "") -> str:
    """門內共用：原樣 os.getenv(name, default)（不 strip）。只給 courier/ 內部模組用。"""
    return os.getenv(name, default)


def env_strip(name: str, default: str = "") -> str:
    """門內共用：原樣 os.getenv(name, default).strip()。"""
    return os.getenv(name, default).strip()


def require(name: str) -> str:
    """門內共用：原樣 os.environ[name]（缺就 KeyError，與原寫法一致）。"""
    return os.environ[name]


def load_local_env(path=r"C:\Users\zhuju\pass.env.txt") -> dict:
    """原 dataset_health_check._load_env（本機手動執行時的備援鑰匙檔；GitHub 上不存在＝回空）。"""
    out = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    out[k.strip()] = v.strip()
    except Exception:
        pass
    return out


def has(name: str) -> bool:
    """給門外判斷「某把鑰匙／設定有沒有設」用（只回 True/False，不回值）。"""
    return bool(os.getenv(name, "").strip())


def has_raw(name: str) -> bool:
    """同 has() 但不去空白（對應原寫法 `if not os.getenv(name, "")`）。"""
    return bool(os.getenv(name, ""))


_LOCAL = {"env": None}


def _local_env() -> dict:
    """本機備援鑰匙檔，只讀一次、只留在門內（原 dataset_health_check._load_env）。"""
    if _LOCAL["env"] is None:
        _LOCAL["env"] = load_local_env()
    return _LOCAL["env"]


def local_env_has(name: str) -> bool:
    return bool(_local_env().get(name, ""))


def local_env_repo_id() -> str:
    """HF_REPO_ID（不是秘密）：本機備援檔的值。"""
    return _local_env().get("HF_REPO_ID", "")


def local_env_secret(name: str) -> str:
    """門內共用：取本機備援檔的鑰匙值（只給 courier/ 內部模組用）。"""
    return _local_env().get(name, "")
