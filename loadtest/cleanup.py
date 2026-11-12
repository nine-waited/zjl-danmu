"""按 sent_ids 删除压测弹幕，并检查 LT- 残留。"""
import argparse
import json
import os
import urllib.request
from pathlib import Path

BASE = "https://nineee.xyz/danmu"


def request(method, path, token):
    req = urllib.request.Request(BASE + path, method=method)
    if token:
        req.add_header("x-admin-token", token)
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    global BASE
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--base", default=BASE, help="站点前缀，如 https://nineee.xyz/danmu")
    args = parser.parse_args()
    BASE = args.base.rstrip("/")
    token = os.environ.get("DANMU_ADMIN_TOKEN", "").strip()
    ids = [line.strip() for line in (Path(args.run_dir) / "sent_ids.txt").read_text(encoding="utf-8").splitlines() if line.strip()]
    print(f"待删除 {len(ids)} 条")
    if args.dry_run:
        print(" ".join(ids[:30]))
        return
    removed = 0
    for item_id in ids:
        try:
            result = request("DELETE", f"/api/danmu/{item_id}", token)
            if result.get("ok"):
                removed += 1
        except Exception as exc:
            print(f"删除 {item_id} 失败: {type(exc).__name__}")
    listing = request("GET", "/api/danmu", token)
    # 回执没赶上进程退出的弹幕不在 sent_ids 里，按名字前缀补删
    for it in listing.get("items") or []:
        if str(it.get("name") or "").startswith("LT-") and it.get("id") is not None:
            try:
                if request("DELETE", f"/api/danmu/{it['id']}", token).get("ok"):
                    removed += 1
            except Exception as exc:
                print(f"删除 {it['id']} 失败: {type(exc).__name__}")
    listing = request("GET", "/api/danmu", token)
    leftover = [it for it in listing.get("items") or [] if str(it.get("name") or "").startswith("LT-")]
    print(f"已删除 {removed}，库内仍可见 LT- 开头 {len(leftover)} 条，当前 count={listing.get('count', len(listing.get('items') or []))}")


if __name__ == "__main__":
    main()
