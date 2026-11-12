import asyncio
import hmac
import ipaddress
import json
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

DATA_DIR = Path("/data")
DB_PATH = DATA_DIR / "danmu.db"
STATIC_DIR = Path("/static")
FIRE_INTERVAL = 1.8
# 热队列 : 循环库 = 3 : 1。比例跟库大小无关，新弹幕不会被旧内容稀释。
HOT_WEIGHT = 3
LOOP_WEIGHT = 1
# 一条新弹幕在热队列里优先播出的次数，播完后回到普通循环。
HOT_BOOST = 4
# 热队列积压超过 HOT_BURST_AT 条时，每个间隔最多出 HOT_BURST 条。设为 1 即关闭。
HOT_BURST_AT = 10
HOT_BURST = 3
HOT_MAX = 300
# 循环库只留最新的这么多条，更早的从时间一头淘汰。
LIBRARY_MAX = 250
# 同一发送者两条弹幕的最小间隔（秒）
MIN_SEND_GAP = 3.0
# 单个连接一次发送的最长等待，超时视为卡死并断开
SEND_TIMEOUT = 2.0
CITY_PUSH_INTERVAL = 3.0
MAX_TEXT = 120
MAX_NAME = 32
MAX_AVATAR = 2048
DEFAULT_COLOR = "#ffffff"
DEFAULT_BG = "bg-black/70 border-pink-500/50"
CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9-]{1,64}$")
CITY_NAMES = ("上海", "东莞", "武汉", "成都", "杭州", "北京", "西安", "南京")
CHINA_TZ = timezone(timedelta(hours=8))

DEFAULT_SEED = [
    {"name": "张明", "text": "祝大家新年快乐，万事如意！🎉", "avatar": "https://api.dicebear.com/7.x/avataaars/svg?seed=Felix", "color": "#ffffff", "bg_style": "bg-black/70 border-pink-500/50"},
    {"name": "王丽丽", "text": "期待今年的年终大奖发发发 🔥🔥", "avatar": "https://api.dicebear.com/7.x/avataaars/svg?seed=Aneka", "color": "#ffffff", "bg_style": "bg-black/70 border-pink-500/50"},
    {"name": "陈杰", "text": "感谢团队小伙伴一年的付出！💪", "avatar": "https://api.dicebear.com/7.x/avataaars/svg?seed=Jack", "color": "#ffffff", "bg_style": "bg-black/70 border-pink-500/50"},
    {"name": "赵敏", "text": "遥遥领先！项目大卖！🚀✨", "avatar": "https://api.dicebear.com/7.x/avataaars/svg?seed=Zoe", "color": "#ffffff", "bg_style": "bg-black/70 border-pink-500/50"},
]

app = FastAPI()
clients: set[WebSocket] = set()
client_ids: dict[WebSocket, str] = {}
# 发过有效管理指令的连接。弹幕库列表只推给它们，观众端用不到。
admin_clients: set[WebSocket] = set()
last_send_at: dict[str, float] = {}
city_dirty = False
library: list[dict[str, Any]] = []
# 热队列条目：{"item", "boost", "seq"}。boost 是剩余优先次数，seq 越大越新。
hot_queue: list[dict[str, Any]] = []
hot_seq = 0
lane_cursor = 0
last_fired_key: str | None = None
loop_index = 0
loop_enabled = True
danmu_speed = 50
lock = asyncio.Lock()
# 访问计数的代数。新 IP 或跨过中国零点时加一，页面丢掉更小的代数。
stats_version = 0
# 当前这批计数所属的中国日期。换日时清空昨日记录，从 0 重新计。
stats_day = ""


def db() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.row_factory = sqlite3.Row
    return con


def init_db() -> None:
    global library, stats_day
    con = db()
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS danmu (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            text TEXT NOT NULL,
            avatar TEXT NOT NULL,
            color TEXT NOT NULL,
            bg_style TEXT NOT NULL,
            created_at REAL NOT NULL
        )
        """
    )
    con.commit()
    cols = {row[1] for row in con.execute("PRAGMA table_info(danmu)")}
    if "sender_id" not in cols:
        con.execute("ALTER TABLE danmu ADD COLUMN sender_id TEXT")
    if "sender_ip" not in cols:
        con.execute("ALTER TABLE danmu ADD COLUMN sender_ip TEXT")
    if "sender_region" not in cols:
        con.execute("ALTER TABLE danmu ADD COLUMN sender_region TEXT")
    version = con.execute("PRAGMA user_version").fetchone()[0]
    rows = con.execute("SELECT * FROM danmu ORDER BY id ASC").fetchall()
    # 只在库从未初始化过时写入示例数据；管理面清空后重启不会再冒出来。
    if not rows and version < 1:
        now = time.time()
        for item in DEFAULT_SEED:
            con.execute(
                "INSERT INTO danmu(name, text, avatar, color, bg_style, created_at) VALUES (?,?,?,?,?,?)",
                (item["name"], item["text"], item["avatar"], item["color"], item["bg_style"], now),
            )
        rows = con.execute("SELECT * FROM danmu ORDER BY id ASC").fetchall()
    if version < 1:
        con.execute("PRAGMA user_version = 1")
        version = 1
    # 撤销历史特殊样式，之后一律使用默认气泡。
    if version < 2:
        con.execute(
            "UPDATE danmu SET color = ?, bg_style = ?",
            (DEFAULT_COLOR, DEFAULT_BG),
        )
        con.execute("PRAGMA user_version = 2")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ip_visit (
            ip TEXT NOT NULL,
            day TEXT NOT NULL,
            city TEXT,
            PRIMARY KEY (ip, day)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS browser_visit (
            client_id TEXT NOT NULL,
            day TEXT NOT NULL,
            city TEXT,
            ip TEXT,
            PRIMARY KEY (client_id, day)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS client_city (
            client_id TEXT PRIMARY KEY,
            city TEXT NOT NULL
        )
        """
    )
    # 已选城市按浏览器永久保留，不随日切或管理端清空一起删。
    for row in con.execute(
        "SELECT client_id, city FROM browser_visit WHERE city IS NOT NULL AND city != '' ORDER BY day ASC"
    ):
        city = str(row["city"] or "").strip()
        if city in CITY_NAMES:
            con.execute(
                "INSERT OR REPLACE INTO client_city(client_id, city) VALUES (?, ?)",
                (row["client_id"], city),
            )
    load_geo()
    backfill_regions(con)
    backfill_visit_cities(con)
    today = china_today()
    con.execute("DELETE FROM ip_visit WHERE day IS NULL OR day < ?", (today,))
    con.execute("DELETE FROM browser_visit WHERE day IS NULL OR day < ?", (today,))
    stats_day = today
    excess = con.execute("SELECT COUNT(*) FROM danmu").fetchone()[0] - LIBRARY_MAX
    if excess > 0:
        con.execute(
            "DELETE FROM danmu WHERE id IN (SELECT id FROM danmu ORDER BY created_at ASC, id ASC LIMIT ?)",
            (excess,),
        )
    rows = con.execute("SELECT * FROM danmu ORDER BY created_at ASC, id ASC").fetchall()
    con.commit()
    library = [dict(r) for r in rows]
    con.close()


def clamp_speed(value: Any) -> int | None:
    try:
        speed = int(round(float(value)))
    except (TypeError, ValueError):
        return None
    return max(0, min(100, speed))


def sanitize(payload: dict[str, Any]) -> dict[str, Any] | None:
    name = str(payload.get("name") or "").strip()[:MAX_NAME]
    text = str(payload.get("text") or "").strip()[:MAX_TEXT]
    if not name or not text:
        return None
    avatar = str(payload.get("avatar") or "").strip()
    if not avatar or len(avatar) > MAX_AVATAR or avatar.startswith("data:"):
        seed = name or "default"
        avatar = f"https://api.dicebear.com/7.x/avataaars/svg?seed={seed}"
    result = {
        "name": name,
        "text": text,
        "avatar": avatar,
        "color": DEFAULT_COLOR,
        "bg_style": DEFAULT_BG,
    }
    raw_id = payload.get("id")
    if raw_id is not None and raw_id != "":
        try:
            result["id"] = int(raw_id)
        except (TypeError, ValueError):
            pass
    return result


def clamp_client_id(value: Any) -> str | None:
    text = str(value or "").strip()
    if CLIENT_ID_RE.fullmatch(text):
        return text
    return None


def admin_token() -> str:
    return os.environ.get("DANMU_ADMIN_TOKEN", "").strip()


def admin_authorized(token: Any) -> bool:
    expected = admin_token()
    if not expected:
        return True
    given = str(token or "")
    if len(given) != len(expected):
        return False
    return hmac.compare_digest(given, expected)


def require_admin(token: Any) -> None:
    if not admin_authorized(token):
        raise HTTPException(status_code=403, detail="forbidden")


def is_trusted_proxy(host: str) -> bool:
    if host in ("localhost",):
        return True
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    return addr.is_private or addr.is_loopback or addr.is_link_local


def client_ip_from(host: str | None, headers: Any) -> str | None:
    """反代（本机或内网）才信任转发头，公网直连不采用客户端自报的地址。"""
    remote = (host or "").strip()
    if is_trusted_proxy(remote):
        for key in ("cf-connecting-ip", "x-real-ip"):
            value = str(headers.get(key) or "").strip()
            if value:
                return value[:64]
        forwarded = str(headers.get("x-forwarded-for") or "").split(",")[0].strip()
        if forwarded:
            return forwarded[:64]
    return remote or None


def ws_client_ip(ws: WebSocket) -> str | None:
    host = ws.client.host if ws.client else ""
    return client_ip_from(host, ws.headers)


def http_client_ip(request: Request) -> str | None:
    host = request.client.host if request.client else ""
    return client_ip_from(host, request.headers)


def public_item(item: dict[str, Any]) -> dict[str, Any]:
    out = {
        "name": item.get("name"),
        "text": item.get("text"),
        "avatar": item.get("avatar"),
        "color": DEFAULT_COLOR,
        "bg_style": DEFAULT_BG,
    }
    if item.get("id") is not None:
        out["id"] = item["id"]
    if item.get("created_at") is not None:
        out["created_at"] = item["created_at"]
    return out


def admin_item(item: dict[str, Any]) -> dict[str, Any]:
    out = public_item(item)
    ip = item.get("sender_ip")
    if ip:
        out["sender_ip"] = ip
    region = item.get("sender_region")
    if region:
        out["sender_region"] = region
    return out


_geo: Any = None


def load_geo() -> None:
    global _geo
    if _geo is not None:
        return
    candidates = [
        DATA_DIR / "ip2region_v4.xdb",
        Path(__file__).resolve().parent.parent / "data" / "ip2region_v4.xdb",
    ]
    path = next((item for item in candidates if item.is_file()), None)
    if path is None:
        _geo = False
        return
    try:
        import ip2region.searcher as xdb
        import ip2region.util as util

        _geo = xdb.new_with_buffer(util.IPv4, util.load_content_from_file(str(path)))
    except Exception:
        _geo = False


def format_region(raw: str | None) -> str | None:
    if not raw:
        return None
    parts = raw.split("|")
    if len(parts) < 3:
        return None
    country, province, city = (parts[0] or "").strip(), (parts[1] or "").strip(), (parts[2] or "").strip()
    if country in ("", "0", "Reserved"):
        return None

    def clean(name: str) -> str:
        if name in ("", "0", "Reserved"):
            return ""
        for suffix in ("特别行政区", "维吾尔自治区", "回族自治区", "壮族自治区", "自治区", "省", "市"):
            if name.endswith(suffix) and len(name) > len(suffix):
                return name[: -len(suffix)]
        return name

    country_name = "中国" if country == "中国" else clean(country)
    place = clean(city) or clean(province)
    if place and place != country_name:
        if country == "中国":
            return f"中国{place}"
        return f"{country_name} {place}".strip()
    return country_name or None


def china_today() -> str:
    return datetime.now(CHINA_TZ).strftime("%Y-%m-%d")


def stats_city(region: str | None) -> str | None:
    if not region:
        return None
    name = region[2:] if region.startswith("中国") else region
    name = name.strip()
    for city in CITY_NAMES:
        if name == city:
            return city
    return None


def clamp_city(value: Any) -> str | None:
    text = str(value or "").strip()
    return text if text in CITY_NAMES else None


def suggest_city(ip: str | None) -> str | None:
    load_geo()
    return stats_city(lookup_region(ip))


def purge_old_visits(today: str) -> None:
    con = db()
    try:
        con.execute("DELETE FROM ip_visit WHERE day IS NULL OR day < ?", (today,))
        con.execute("DELETE FROM browser_visit WHERE day IS NULL OR day < ?", (today,))
        con.commit()
    finally:
        con.close()


def wipe_visit_rows() -> None:
    """只清当天人数。client_city（自选城市）保留，零点日切也不碰它。"""
    con = db()
    try:
        con.execute("DELETE FROM ip_visit")
        con.execute("DELETE FROM browser_visit")
        con.commit()
    finally:
        con.close()


def ensure_stats_day() -> bool:
    """调用方必须已持有 lock。跨过中国零点时删掉昨日记录并推进代数。"""
    global stats_day, stats_version
    today = china_today()
    if stats_day == today:
        return False
    purge_old_visits(today)
    stats_day = today
    stats_version += 1
    return True


def clear_visits() -> tuple[list[dict[str, Any]], int, str]:
    """管理面清空当天人数。调用方必须已持有 lock。

    与零点重置的分工：
    - 先 ensure_stats_day：若已跨日，先完成日切，再清「今天」；
    - 只删 browser_visit / ip_visit，不删 client_city，也不改 stats_day；
    - 零点只删过期的按日人数，自选城市继续留在 client_city。
    """
    global stats_version, city_dirty
    ensure_stats_day()
    wipe_visit_rows()
    stats_version += 1
    city_dirty = False
    return city_stats(), stats_version, stats_day or china_today()


def city_stats() -> list[dict[str, Any]]:
    """只统计中国时间当天的访问，不把昨日人数加进来。"""
    con = db()
    rows = con.execute(
        "SELECT city, COUNT(*) AS n FROM browser_visit WHERE day = ? AND city IS NOT NULL AND city != '' GROUP BY city",
        (china_today(),),
    ).fetchall()
    con.close()
    counts = {city: 0 for city in CITY_NAMES}
    for row in rows:
        if row["city"] in counts:
            counts[row["city"]] = row["n"]
    return [{"city": city, "count": counts[city]} for city in CITY_NAMES]


def saved_client_city(con: sqlite3.Connection, client_id: str) -> str | None:
    row = con.execute("SELECT city FROM client_city WHERE client_id = ?", (client_id,)).fetchone()
    return clamp_city(row["city"]) if row else None


def remember_client_city(con: sqlite3.Connection, client_id: str, city: str) -> None:
    con.execute(
        "INSERT OR REPLACE INTO client_city(client_id, city) VALUES (?, ?)",
        (client_id, city),
    )


def record_visit(
    client_id: str | None,
    ip: str | None,
    city: str | None = None,
) -> tuple[bool, str | None, str | None, bool]:
    """按浏览器 + 当天去重。城市只来自用户自选缓存，IP 只做建议，不写入统计。

    返回 (是否新计入榜, 已确认城市, IP 建议城市, 是否还需要用户选城)。
    """
    client_id = clamp_client_id(client_id)
    suggested = suggest_city(ip)
    if not client_id:
        return False, None, suggested, False
    day = china_today()
    ip = str(ip or "").strip()[:64]
    chosen = clamp_city(city)
    con = db()
    try:
        con.execute("BEGIN IMMEDIATE")
        if chosen:
            remember_client_city(con, client_id, chosen)
        effective = chosen or saved_client_city(con, client_id)
        existing = con.execute(
            "SELECT city FROM browser_visit WHERE client_id = ? AND day = ?",
            (client_id, day),
        ).fetchone()
        if not effective:
            if not existing:
                con.execute(
                    "INSERT INTO browser_visit(client_id, day, city, ip) VALUES (?,?,NULL,?)",
                    (client_id, day, ip or None),
                )
            else:
                con.execute(
                    "UPDATE browser_visit SET ip = COALESCE(?, ip) WHERE client_id = ? AND day = ?",
                    (ip or None, client_id, day),
                )
            con.commit()
            return False, None, suggested, True
        if existing and clamp_city(existing["city"]) == effective:
            con.commit()
            return False, effective, suggested, False
        if existing:
            con.execute(
                "UPDATE browser_visit SET city = ?, ip = ? WHERE client_id = ? AND day = ?",
                (effective, ip or None, client_id, day),
            )
        else:
            con.execute(
                "INSERT INTO browser_visit(client_id, day, city, ip) VALUES (?,?,?,?)",
                (client_id, day, effective, ip or None),
            )
        con.commit()
        return True, effective, suggested, False
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        return False, None, suggested, True
    finally:
        con.close()


def note_visit(
    client_id: str | None,
    ip: str | None,
    city: str | None = None,
) -> tuple[bool, list[dict[str, Any]], int, str | None, str | None, bool]:
    """调用方必须已持有 lock。插入和读快照在同一临界区内完成。"""
    global stats_version
    rolled = ensure_stats_day()
    counted, assigned, suggested, need_city = record_visit(client_id, ip, city)
    if counted:
        stats_version += 1
    return counted or rolled, city_stats(), stats_version, assigned, suggested, need_city


def city_stats_message(items: list[dict[str, Any]] | None = None, version: int | None = None) -> dict[str, Any]:
    return {
        "type": "city_stats",
        "items": city_stats() if items is None else items,
        "version": stats_version if version is None else version,
        "day": stats_day or china_today(),
    }


def lookup_region(ip: str | None) -> str | None:
    if not ip or _geo is False or _geo is None:
        return None
    try:
        return format_region(_geo.search(str(ip).strip()))
    except Exception:
        return None


def backfill_visit_cities(con: sqlite3.Connection) -> None:
    # 城市改为用户自选后，不再用 IP 回填，避免号段归属把东莞访客观成广州。
    return


def backfill_regions(con: sqlite3.Connection) -> None:
    pending = con.execute(
        "SELECT id, sender_ip FROM danmu WHERE sender_ip IS NOT NULL AND sender_ip != '' AND (sender_region IS NULL OR sender_region = '')"
    ).fetchall()
    for row in pending:
        region = lookup_region(row["sender_ip"])
        if region:
            con.execute("UPDATE danmu SET sender_region = ? WHERE id = ?", (region, row["id"]))


def persist(item: dict[str, Any], sender_id: str | None = None, sender_ip: str | None = None) -> dict[str, Any]:
    load_geo()
    sender_region = lookup_region(sender_ip)
    con = db()
    cur = con.execute(
        "INSERT INTO danmu(name, text, avatar, color, bg_style, created_at, sender_id, sender_ip, sender_region) VALUES (?,?,?,?,?,?,?,?,?)",
        (
            item["name"],
            item["text"],
            item["avatar"],
            item["color"],
            item["bg_style"],
            time.time(),
            sender_id,
            sender_ip,
            sender_region,
        ),
    )
    con.commit()
    item = {
        **item,
        "id": cur.lastrowid,
        "created_at": time.time(),
        "sender_id": sender_id,
        "sender_ip": sender_ip,
        "sender_region": sender_region,
    }
    con.close()
    return item


def delete_item(item_id: int) -> bool:
    global loop_index, last_fired_key
    con = db()
    cur = con.execute("DELETE FROM danmu WHERE id = ?", (item_id,))
    con.commit()
    con.close()
    pos = next((i for i, it in enumerate(library) if it.get("id") == item_id), None)
    if pos is not None:
        library.pop(pos)
        if pos < loop_index:
            loop_index -= 1
        loop_index = loop_index % len(library) if library else 0
    hot_queue[:] = [e for e in hot_queue if e["item"].get("id") != item_id]
    if last_fired_key == f"id:{item_id}":
        last_fired_key = None
    return cur.rowcount > 0 or pos is not None


def clear_library() -> int:
    global loop_index, last_fired_key, lane_cursor
    con = db()
    cur = con.execute("DELETE FROM danmu")
    con.commit()
    con.close()
    library.clear()
    hot_queue.clear()
    loop_index = 0
    lane_cursor = 0
    last_fired_key = None
    return cur.rowcount


def library_message() -> dict[str, Any]:
    return {"type": "library", "count": len(library), "items": [public_item(it) for it in library[-50:]]}


def trim_library() -> list[int]:
    """调用方持有 lock。库按时间从旧到新排列，超过上限时淘汰最旧的。"""
    global loop_index, last_fired_key
    overflow = len(library) - LIBRARY_MAX
    if overflow <= 0:
        return []
    removed = library[:overflow]
    del library[:overflow]
    ids = [int(it["id"]) for it in removed if it.get("id") is not None]
    if ids:
        con = db()
        for start in range(0, len(ids), 400):
            chunk = ids[start:start + 400]
            con.execute(
                f"DELETE FROM danmu WHERE id IN ({','.join('?' for _ in chunk)})",
                chunk,
            )
        con.commit()
        con.close()
        gone = set(ids)
        hot_queue[:] = [entry for entry in hot_queue if entry["item"].get("id") not in gone]
        if last_fired_key and last_fired_key.startswith("id:"):
            try:
                if int(last_fired_key[3:]) in gone:
                    last_fired_key = None
            except ValueError:
                pass
    if loop_index >= overflow:
        loop_index -= overflow
    else:
        loop_index = 0
    if library:
        loop_index %= len(library)
    return ids


def item_key(item: dict[str, Any]) -> str:
    if item.get("id") is not None:
        return f"id:{item['id']}"
    return f"txt:{item.get('name')}|{item.get('text')}"


def enqueue_hot(item: dict[str, Any]) -> None:
    """入队。同一条已在热队列时刷新权重并视为最新，不叠成重复条目。"""
    global hot_seq
    hot_seq += 1
    key = item_key(item)
    for entry in hot_queue:
        if item_key(entry["item"]) == key:
            entry["item"] = item
            entry["boost"] = HOT_BOOST
            entry["seq"] = hot_seq
            return
    hot_queue.append({"item": item, "boost": HOT_BOOST, "seq": hot_seq})
    if len(hot_queue) > HOT_MAX:
        played = [e for e in hot_queue if e["boost"] < HOT_BOOST]
        victim = min(played or hot_queue, key=lambda e: e["seq"])
        hot_queue.remove(victim)


def enqueue_hot_batch(items: list[dict[str, Any]]) -> None:
    """批量按输入顺序排队：先入队的下一次优先，后到的实时弹幕仍能插到它们前面。"""
    for item in reversed(items):
        enqueue_hot(item)


def pop_hot(avoid_key: str | None) -> dict[str, Any] | None:
    if not hot_queue:
        return None
    # 还有别的热弹幕时避开刚播过的那条，避免同一句连占；只剩一条时连续播出，优先才看得出。
    candidates = [
        entry
        for entry in hot_queue
        if not (avoid_key and item_key(entry["item"]) == avoid_key)
    ]
    if not candidates:
        candidates = list(hot_queue)
    best = max(candidates, key=lambda entry: (entry["boost"], entry["seq"]))
    hot_queue.remove(best)
    best["boost"] -= 1
    if best["boost"] > 0:
        hot_queue.append(best)
    return best["item"]


def pop_loop(avoid_key: str | None) -> dict[str, Any] | None:
    if not library:
        return None
    hot_keys = {item_key(entry["item"]) for entry in hot_queue}
    n = len(library)
    fallback = None
    global loop_index
    for _ in range(n):
        item = library[loop_index % n]
        loop_index = (loop_index + 1) % n
        key = item_key(item)
        if key in hot_keys:
            continue
        if fallback is None:
            fallback = item
        if avoid_key and key == avoid_key:
            continue
        return item
    return fallback


def select_next() -> tuple[dict[str, Any], str] | None:
    """每个间隔只出一条。有热弹幕时按 HOT_WEIGHT:LOOP_WEIGHT 出队，并避开刚播过的那条。"""
    global lane_cursor
    hot_available = bool(hot_queue)
    loop_available = loop_enabled and bool(library)
    if not hot_available and not loop_available:
        return None
    if hot_available and loop_available:
        cycle = HOT_WEIGHT + LOOP_WEIGHT
        want_hot = (lane_cursor % cycle) < HOT_WEIGHT
        lane_cursor += 1
    else:
        want_hot = hot_available

    if want_hot:
        item = pop_hot(last_fired_key)
        if item is not None:
            return item, "priority"
        item = pop_loop(last_fired_key)
        if item is not None:
            return item, "loop"
        item = pop_hot(None)
        if item is not None:
            return item, "priority"
        return None

    item = pop_loop(last_fired_key)
    if item is not None:
        return item, "loop"
    item = pop_hot(last_fired_key) or pop_hot(None)
    if item is not None:
        return item, "priority"
    return None


async def publish_city_stats(items: list[dict[str, Any]], version: int) -> None:
    """临界区里确认这份快照仍是最新一代，再广播。更晚的访问会作废旧快照。"""
    async with lock:
        if version != stats_version:
            return
        payload = city_stats_message(items, version)
    await broadcast(payload)


async def city_stats_pusher() -> None:
    """新访客只标记变化，这里每隔 CITY_PUSH_INTERVAL 秒合并推一次，避免进场高峰时按人数平方放大。"""
    global city_dirty
    while True:
        await asyncio.sleep(CITY_PUSH_INTERVAL)
        if not city_dirty:
            continue
        async with lock:
            city_dirty = False
            payload = city_stats_message()
        await broadcast(payload)


def forget_client(ws: WebSocket) -> None:
    clients.discard(ws)
    client_ids.pop(ws, None)
    admin_clients.discard(ws)


async def _send_one(ws: WebSocket, raw: str) -> WebSocket | None:
    try:
        await asyncio.wait_for(ws.send_text(raw), SEND_TIMEOUT)
        return None
    except Exception:
        return ws


async def deliver(pairs: list[tuple[WebSocket, str]]) -> None:
    """并发发送；超时或出错的连接直接断开，不拖慢其他人。"""
    if not pairs:
        return
    results = await asyncio.gather(*(_send_one(ws, raw) for ws, raw in pairs))
    for ws in results:
        if ws is None:
            continue
        forget_client(ws)
        asyncio.create_task(_close_quietly(ws))


async def _close_quietly(ws: WebSocket) -> None:
    try:
        await asyncio.wait_for(ws.close(code=1011), 2)
    except Exception:
        pass


async def broadcast(message: dict[str, Any]) -> None:
    raw = json.dumps(message, ensure_ascii=False)
    await deliver([(ws, raw) for ws in list(clients)])


async def broadcast_admin(message: dict[str, Any]) -> None:
    raw = json.dumps(message, ensure_ascii=False)
    await deliver([(ws, raw) for ws in list(admin_clients) if ws in clients])


def mark_admin(ws: WebSocket) -> None:
    if ws in clients:
        admin_clients.add(ws)


def rate_limited(key: str) -> bool:
    now = time.monotonic()
    last = last_send_at.get(key)
    if last is not None and now - last < MIN_SEND_GAP:
        return True
    last_send_at[key] = now
    if len(last_send_at) > 20000:
        cutoff = now - 60
        for k in [k for k, t in last_send_at.items() if t < cutoff]:
            last_send_at.pop(k, None)
    return False


def remember_client(ws: WebSocket, value: Any) -> str | None:
    cid = clamp_client_id(value)
    if cid:
        client_ids[ws] = cid
    return cid


async def fire(item: dict[str, Any], source: str) -> None:
    body: dict[str, Any] = {"type": "fire", "source": source, "danmu": public_item(item)}
    plain = json.dumps(body, ensure_ascii=False)
    sender = item.get("sender_id")
    mine = json.dumps({**body, "mine": True}, ensure_ascii=False) if sender else plain
    await deliver([
        (ws, mine if sender and client_ids.get(ws) == sender else plain)
        for ws in list(clients)
    ])


async def reject(ws: WebSocket) -> None:
    await ws.send_text(json.dumps({"type": "error", "error": "unauthorized"}, ensure_ascii=False))


async def publish_today_if_rolled() -> None:
    async with lock:
        if not ensure_stats_day():
            return
        items = city_stats()
        version = stats_version
    await publish_city_stats(items, version)


async def scheduler() -> None:
    global last_fired_key
    next_at = time.monotonic() + FIRE_INTERVAL
    while True:
        delay = next_at - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
        await publish_today_if_rolled()
        batch: list[tuple[dict[str, Any], str]] = []
        async with lock:
            burst = HOT_BURST if len(hot_queue) > HOT_BURST_AT else 1
            for _ in range(burst):
                picked = select_next()
                if not picked:
                    break
                last_fired_key = item_key(picked[0])
                batch.append(picked)
        for item, source in batch:
            await fire(item, source)
        next_at += FIRE_INTERVAL
        # 落后时只补到下一次，避免暂停或卡顿后一次性补发一串
        if next_at < time.monotonic():
            next_at = time.monotonic() + FIRE_INTERVAL


@app.on_event("startup")
async def on_startup() -> None:
    init_db()
    asyncio.create_task(scheduler())
    asyncio.create_task(city_stats_pusher())


@app.get("/api/health")
async def health():
    return {
        "ok": True,
        "count": len(library),
        "loop": loop_enabled,
        "pending": len(hot_queue),
        "speed": danmu_speed,
        "clients": len(clients),
        "admins": len(admin_clients),
    }


@app.get("/api/cities")
async def get_cities():
    async with lock:
        rolled = ensure_stats_day()
        payload = {"items": city_stats(), "version": stats_version, "day": stats_day}
    if rolled:
        await publish_city_stats(payload["items"], payload["version"])
    return payload


@app.get("/api/danmu")
async def list_danmu():
    return {"items": [public_item(it) for it in library], "loop": loop_enabled}


@app.post("/api/danmu")
async def post_danmu(payload: dict[str, Any], request: Request):
    item = sanitize(payload)
    if not item:
        return {"ok": False, "error": "invalid"}
    removed: list[int] = []
    async with lock:
        item = persist(item, clamp_client_id(payload.get("client_id")), http_client_ip(request))
        library.append(item)
        enqueue_hot(item)
        removed = trim_library()
    if removed:
        await broadcast({"type": "remove", "ids": removed})
    return {"ok": True, "item": public_item(item)}


@app.post("/api/danmu/batch")
async def post_batch(payload: dict[str, Any], request: Request):
    require_admin(request.headers.get("x-admin-token"))
    items_in = payload.get("items") or []
    saved = []
    removed: list[int] = []
    async with lock:
        for raw in items_in:
            item = sanitize(raw if isinstance(raw, dict) else {})
            if not item:
                continue
            item = persist(item)
            library.append(item)
            saved.append(item)
        enqueue_hot_batch(saved)
        removed = trim_library()
    if removed:
        await broadcast({"type": "remove", "ids": removed})
    return {"ok": True, "count": len(saved), "items": [public_item(it) for it in saved]}


@app.delete("/api/danmu/{item_id}")
async def delete_danmu(item_id: int, request: Request):
    require_admin(request.headers.get("x-admin-token"))
    async with lock:
        ok = delete_item(item_id)
    if ok:
        await broadcast({"type": "remove", "id": item_id})
        await broadcast_admin(library_message())
    return {"ok": ok, "count": len(library)}


@app.delete("/api/danmu")
async def clear_danmu(request: Request):
    require_admin(request.headers.get("x-admin-token"))
    async with lock:
        removed = clear_library()
    await broadcast({"type": "clear_library"})
    await broadcast_admin(library_message())
    return {"ok": True, "removed": removed, "count": 0}


@app.post("/api/visits/clear")
async def clear_visits_api(request: Request):
    require_admin(request.headers.get("x-admin-token"))
    async with lock:
        items, version, day = clear_visits()
    await broadcast(city_stats_message(items, version))
    return {"ok": True, "items": items, "version": version, "day": day}


@app.post("/api/loop")
async def set_loop(payload: dict[str, Any], request: Request):
    require_admin(request.headers.get("x-admin-token"))
    global loop_enabled
    enabled = bool(payload.get("enabled", True))
    async with lock:
        loop_enabled = enabled
    await broadcast({"type": "loop", "enabled": loop_enabled})
    return {"ok": True, "loop": loop_enabled}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    global loop_enabled, danmu_speed, city_dirty
    await ws.accept()
    clients.add(ws)
    cities: list[dict[str, Any]] = []
    cities_version = 0
    cities_day = ""
    async with lock:
        ensure_stats_day()
        cities = city_stats()
        cities_version = stats_version
        cities_day = stats_day
    try:
        await ws.send_text(
            json.dumps(
                {
                    "type": "hello",
                    "loop": loop_enabled,
                    "speed": danmu_speed,
                    "count": len(library),
                    "items": [public_item(it) for it in library[-50:]],
                    "cities": cities,
                    "cities_version": cities_version,
                    "cities_day": cities_day,
                },
                ensure_ascii=False,
            )
        )
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            mtype = msg.get("type")
            if mtype == "identify":
                cid = remember_client(ws, msg.get("client_id"))
                if msg.get("checkin") is not False and cid:
                    async with lock:
                        changed, items, version, assigned, suggested, need_city = note_visit(
                            cid, ws_client_ip(ws), msg.get("city")
                        )
                        day = stats_day or china_today()
                    await ws.send_text(
                        json.dumps(
                            {
                                "type": "checkin_ack",
                                "city": assigned,
                                "suggested_city": suggested,
                                "need_city": need_city,
                                "day": day,
                                "version": version,
                                "items": items if changed else None,
                            },
                            ensure_ascii=False,
                        )
                    )
                    # 选城计入后立刻推榜，避免再等 3 秒合并窗口
                    if changed:
                        await broadcast(city_stats_message(items, version))
                continue
            if mtype == "send":
                raw_danmu = msg.get("danmu") if isinstance(msg.get("danmu"), dict) else msg
                item = sanitize(raw_danmu)
                if not item:
                    continue
                sender_id = clamp_client_id(raw_danmu.get("client_id") or msg.get("client_id"))
                remember_client(ws, sender_id)
                if rate_limited(sender_id or f"ws:{id(ws)}"):
                    await ws.send_text(json.dumps({"type": "error", "error": "rate_limited"}, ensure_ascii=False))
                    continue
                removed: list[int] = []
                async with lock:
                    item = persist(item, sender_id, ws_client_ip(ws))
                    library.append(item)
                    enqueue_hot(item)
                    removed = trim_library()
                await ws.send_text(json.dumps({"type": "sent", "id": item["id"]}, ensure_ascii=False))
                if removed:
                    await broadcast({"type": "remove", "ids": removed})
                await broadcast_admin(library_message())
            elif mtype == "batch":
                if not admin_authorized(msg.get("token")):
                    await reject(ws)
                    continue
                items_in = msg.get("items") or []
                saved = []
                removed: list[int] = []
                async with lock:
                    for raw_item in items_in:
                        item = sanitize(raw_item if isinstance(raw_item, dict) else {})
                        if not item:
                            continue
                        item = persist(item)
                        library.append(item)
                        saved.append(item)
                    enqueue_hot_batch(saved)
                    removed = trim_library()
                if removed:
                    await broadcast({"type": "remove", "ids": removed})
                mark_admin(ws)
                await broadcast_admin(library_message())
            elif mtype == "list":
                if not admin_authorized(msg.get("token")):
                    await reject(ws)
                    continue
                mark_admin(ws)
                await ws.send_text(
                    json.dumps(
                        {"type": "library_full", "count": len(library), "items": [admin_item(it) for it in library]},
                        ensure_ascii=False,
                    )
                )
            elif mtype == "delete":
                if not admin_authorized(msg.get("token")):
                    await reject(ws)
                    continue
                try:
                    item_id = int(msg.get("id"))
                except (TypeError, ValueError):
                    continue
                async with lock:
                    ok = delete_item(item_id)
                if ok:
                    await broadcast({"type": "remove", "id": item_id})
                    await broadcast_admin(library_message())
            elif mtype == "clear_library":
                if not admin_authorized(msg.get("token")):
                    await reject(ws)
                    continue
                async with lock:
                    clear_library()
                await broadcast({"type": "clear_library"})
                await broadcast_admin(library_message())
            elif mtype == "replay":
                # 从共享库再加权，不落库，仍走统一出队
                item = sanitize(msg.get("danmu") or msg)
                if item:
                    async with lock:
                        enqueue_hot(item)
            elif mtype == "loop":
                if not admin_authorized(msg.get("token")):
                    await reject(ws)
                    continue
                async with lock:
                    loop_enabled = bool(msg.get("enabled", True))
                await broadcast({"type": "loop", "enabled": loop_enabled})
            elif mtype == "clear_screen":
                if not admin_authorized(msg.get("token")):
                    await reject(ws)
                    continue
                await broadcast({"type": "clear_screen"})
            elif mtype == "clear_visits":
                if not admin_authorized(msg.get("token")):
                    await reject(ws)
                    continue
                mark_admin(ws)
                async with lock:
                    items, version, day = clear_visits()
                await broadcast(city_stats_message(items, version))
                await ws.send_text(
                    json.dumps(
                        {"type": "visits_cleared", "items": items, "version": version, "day": day},
                        ensure_ascii=False,
                    )
                )
            elif mtype == "speed":
                if not admin_authorized(msg.get("token")):
                    await reject(ws)
                    continue
                speed = clamp_speed(msg.get("value"))
                if speed is None:
                    continue
                async with lock:
                    danmu_speed = speed
                await broadcast({"type": "speed", "value": danmu_speed})
    except WebSocketDisconnect:
        pass
    finally:
        forget_client(ws)


@app.get("/admin")
@app.get("/admin/")
async def admin_page():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/")
async def root_page():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/assets", StaticFiles(directory=STATIC_DIR), name="assets")