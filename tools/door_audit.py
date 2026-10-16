# -*- coding: utf-8 -*-
"""
tools/door_audit.py — 門外偵測（2026-10-16「唯一的門」原則的防退化檢查）

規則：courier/ 以外的程式
  1. 不得 import 連外套件（Mongo、HTTP、HF dataset、行情、dotenv、socket/smtp）
  2. 不得讀秘密類環境變數（名稱含 URI/KEY/TOKEN/SECRET/PASSWORD/URL/GH_/HF_）
  3. 不得在程式碼（非註解說明字串）裡寫死外部網址
  4. 不得取用門內私有名稱（底線開頭，例如資料庫物件 _comm_handle）——門只交付按用途的函數
  5. 不得取用門物件的資料庫屬性（db.stock_db／db.client 等）
有違規 → 列出並 exit 1；全部乾淨 → exit 0。每次改 DC、commit 前先跑：
    python tools/door_audit.py
"""
import ast
import os
import re
import sys

DOOR_DIR = "courier"
SKIP_DIRS = {"__pycache__", ".git", "tools", "tests"}
NET_MODULES = {"pymongo", "motor", "bson", "requests", "httpx", "aiohttp", "urllib3", "huggingface_hub",
               "yfinance", "dotenv", "socket", "smtplib", "websocket", "websockets", "boto3"}
NET_SUBMODULES = {"urllib.request", "http.client"}
SECRET_ENV_RE = re.compile(r"(URI|KEY|TOKEN|SECRET|PASSWORD|URL|^GH_|^HF_)", re.I)
URL_RE = re.compile(r"https?://", re.I)
DB_HANDLE_ATTRS = {"stock_db", "comm_db", "client", "_stock_db", "_comm_db", "_client"}


def _docstring_nodes(tree):
    ids = set()
    for n in ast.walk(tree):
        if isinstance(n, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = getattr(n, "body", [])
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                ids.add(id(body[0].value))
    # 單獨一行的字串（區塊說明）也視為說明文字
    for n in ast.walk(tree):
        if isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant) and isinstance(n.value.value, str):
            ids.add(id(n.value))
    return ids


def _env_name(call):
    if call.args and isinstance(call.args[0], ast.Constant) and isinstance(call.args[0].value, str):
        return call.args[0].value
    return None


def _courier_aliases(tree):
    """門外檔案裡指向 courier 模組的名字（import courier.x as y／from courier import x）。"""
    names = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            for a in n.names:
                if a.name.split(".")[0] == DOOR_DIR:
                    names.add(a.asname or a.name.split(".")[0])
        elif isinstance(n, ast.ImportFrom) and n.module and n.module.split(".")[0] == DOOR_DIR:
            for a in n.names:
                names.add(a.asname or a.name)
    return names


def scan(path):
    src = open(path, encoding="utf-8", errors="replace").read()
    tree = ast.parse(src)
    docs = _docstring_nodes(tree)
    hits = []
    aliases = _courier_aliases(tree)
    for n in ast.walk(tree):
        # 規則 4：不得取用門內的私有名稱（底線開頭，例如資料庫物件 _comm_handle）——門只交付按用途的函數
        if isinstance(n, ast.ImportFrom) and n.module and n.module.split(".")[0] == DOOR_DIR:
            for a in n.names:
                if a.name.startswith("_"):
                    hits.append((n.lineno, "取用門內私有名稱 %s.%s" % (n.module, a.name)))
        if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id in aliases \
                and n.attr.startswith("_") and not n.attr.startswith("__"):
            hits.append((n.lineno, "取用門內私有名稱 %s.%s" % (n.value.id, n.attr)))
        # 規則 5：不得取用門物件的資料庫屬性（例如 db.stock_db[...]、db.client.admin）——要什麼請門的方法代辦
        if isinstance(n, ast.Attribute) and n.attr in DB_HANDLE_ATTRS \
                and not (isinstance(n.value, ast.Name) and n.value.id == "self"):
            hits.append((n.lineno, "取用資料庫物件屬性 .%s" % n.attr))
        if isinstance(n, ast.Import):
            for a in n.names:
                if a.name.split(".")[0] in NET_MODULES or a.name in NET_SUBMODULES:
                    hits.append((n.lineno, "連外套件 import " + a.name))
        elif isinstance(n, ast.ImportFrom) and n.module:
            if n.module.split(".")[0] in NET_MODULES or n.module in NET_SUBMODULES \
                    or (n.module == "urllib" and any(a.name == "request" for a in n.names)):
                hits.append((n.lineno, "連外套件 from %s import" % n.module))
        elif isinstance(n, ast.Call):
            fn = ast.unparse(n.func)
            if fn in ("os.getenv", "os.environ.get", "getenv", "environ.get"):
                name = _env_name(n)
                if name is None or SECRET_ENV_RE.search(name):
                    hits.append((n.lineno, "讀環境變數 %s" % (name or "(動態名稱)")))
        elif isinstance(n, ast.Subscript) and ast.unparse(n.value) in ("os.environ", "environ"):
            s = n.slice
            name = s.value if isinstance(s, ast.Constant) else None
            if name is None or SECRET_ENV_RE.search(str(name)):
                hits.append((n.lineno, "讀環境變數 %s" % (name or "(動態名稱)")))
        elif isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in docs:
            if URL_RE.search(n.value):
                hits.append((n.lineno, "寫死外部網址 %s" % n.value[:60]))
        elif isinstance(n, ast.JoinedStr):
            for v in n.values:
                if isinstance(v, ast.Constant) and isinstance(v.value, str) and URL_RE.search(v.value):
                    hits.append((n.lineno, "寫死外部網址 %s" % v.value[:60]))
    return hits


def main(root):
    total = 0
    for dp, dn, fn in os.walk(root):
        rel = os.path.relpath(dp, root).replace("\\", "/")
        dn[:] = [d for d in dn if d not in SKIP_DIRS and not (rel == "." and d == DOOR_DIR)]
        for f in sorted(fn):
            if not f.endswith(".py"):
                continue
            p = os.path.join(dp, f)
            for line, why in scan(p):
                total += 1
                print("%s:%d  %s" % (os.path.relpath(p, root).replace("\\", "/"), line, why))
    print("門外呼叫：%d 處" % total)
    return 1 if total else 0


if __name__ == "__main__":
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else here))
