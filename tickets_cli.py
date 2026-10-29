# -*- coding: utf-8 -*-
"""
tickets_cli.py — 工單通道的人工處理工具（數據中心窗口在本機用；2026-10-28）

通道本身不靠這支（BC bc_tickets.yml 雲端預審）；這支只給「Jun 說處理工單」時，數據中心窗口看證據、人工回覆用。
規矩與 run_tickets.py 共用同一組純函數（回覆格式檢查、只追加回覆、只刪 from=datacenter）。
Mongo 鑰匙：環境變數 MONGO_URI，沒有就讀本機備援鑰匙檔（只在門內讀，不印）。
用法：
  python tickets_cli.py list [--all]                      待處理的單（--all＝全部）
  python tickets_cli.py show <_id>
  python tickets_cli.py reply <_id> --kind renamed --body '{"new_symbol":"XYZ","effective":"2026-10-27","identity_version":3}' --note "..."
  python tickets_cli.py open --type spec_reply --key <鍵> --summary "..." [--subject JSON] [--detail-file 檔.json] [--to local]
  python tickets_cli.py delete <_id>                      只能刪數據中心自己開的單
Python 3.9 相容。
"""
import argparse
import json
import sys

import run_tickets as RT


def _mongo():
    from courier import mongo
    mongo.configure(local_fallback=True, serverSelectionTimeoutMS=20000)
    return mongo


def _line(d):
    sub = d.get("subject") or {}
    rv = d.get("dc_review") or {}
    return "%-48s %-6s→%-10s %-9s 第%s輪 %s %s｜預審：%s" % (
        d["_id"], d.get("from"), d.get("to"), d.get("status"), d.get("round"), d.get("type"),
        sub.get("symbol") or "", (rv.get("suggest") or "-") + ("（%s）" % rv["reason"] if rv.get("reason") else ""))


def cmd_list(a):
    m = _mongo()
    q = {} if a.all else {"$or": [{"to": RT.ME, "status": {"$in": ["open", "working"]}},
                                  {"from": RT.ME, "status": "replied"}]}
    n = 0
    for d in m.find(RT.DB, RT.COL, q, sort=("created_at", 1)):
        print(_line(d))
        n += 1
    print("共 %d 張" % n)


def cmd_show(a):
    d = _mongo().find_one(RT.DB, RT.COL, {"_id": a.id})
    print(json.dumps(d, ensure_ascii=False, indent=1, default=str) if d else "查無 %s" % a.id)


def cmd_reply(a):
    m = _mongo()
    d = m.find_one(RT.DB, RT.COL, {"_id": a.id, "to": RT.ME})
    if not d:
        sys.exit("⛔ 查無給數據中心的單 %s" % a.id)
    if d.get("status") == "open":                      # 還沒被預審搶走：先搶成 working
        if m.find_one_and_update(RT.DB, RT.COL, {"_id": a.id, "to": RT.ME, "status": "open"},
                                 {"$set": {"status": "working"}}) is None:
            sys.exit("⛔ 單子狀態剛變了，請重新 show")
        d["status"] = "working"
    if d.get("status") != "working":
        sys.exit("⛔ 單子狀態是 %s，不能回覆" % d.get("status"))
    body = json.loads(a.body) if a.body else {}
    q, u = RT.reply_update(d, a.kind, body, a.note, RT.now_et_iso())
    r = m.update_one(RT.DB, RT.COL, q, u)
    print("✅ 已回覆 %s（%s）" % (a.id, a.kind) if r.modified_count == 1 else "⛔ 沒寫進去（單子狀態或輪次已變）")


def cmd_open(a):
    m = _mongo()
    detail = None
    if a.detail_file:
        with open(a.detail_file, encoding="utf-8") as f:
            detail = json.load(f)
    doc = RT.new_ticket(a.type, a.key, json.loads(a.subject) if a.subject else {}, a.summary, RT.now_et_iso(),
                        detail=detail, to=a.to)
    try:
        m.insert_one(RT.DB, RT.COL, doc)
        print("✅ 已開單 %s → %s" % (doc["_id"], a.to))
    except m.DuplicateKeyError:
        print("⚠️ %s 已存在（同一件事不重開）" % doc["_id"])


def cmd_delete(a):
    r = _mongo().delete_one(RT.DB, RT.COL, RT.delete_query(a.id))
    print("✅ 已刪 %s" % a.id if r.deleted_count == 1 else "⛔ 沒刪（不存在，或不是數據中心開的單）")


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    sp = ap.add_subparsers(dest="cmd", required=True)
    p = sp.add_parser("list")
    p.add_argument("--all", action="store_true")
    p.set_defaults(fn=cmd_list)
    p = sp.add_parser("show")
    p.add_argument("id")
    p.set_defaults(fn=cmd_show)
    p = sp.add_parser("reply")
    p.add_argument("id")
    p.add_argument("--kind", required=True, choices=sorted(RT.REPLY_KINDS))
    p.add_argument("--body", default="")
    p.add_argument("--note", default="")
    p.set_defaults(fn=cmd_reply)
    p = sp.add_parser("open")
    p.add_argument("--type", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--summary", required=True)
    p.add_argument("--subject", default="")
    p.add_argument("--detail-file", default="")
    p.add_argument("--to", default=RT.PEER, choices=[RT.PEER])
    p.set_defaults(fn=cmd_open)
    p = sp.add_parser("delete")
    p.add_argument("id")
    p.set_defaults(fn=cmd_delete)
    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
