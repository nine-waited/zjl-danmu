"""本机 WebSocket 压测：观众只收，少量发送者发弹幕。"""
import argparse
import asyncio
import json
import multiprocessing as mp
import random
import time
from pathlib import Path

import websockets

DEFAULT_URL = "wss://borders-feb-approach-mixture.trycloudflare.com/ws"


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((p / 100) * (len(ordered) - 1)))))
    return ordered[idx]


def worker(proc, viewers, senders, ramp_rate, procs, hold, send_interval, url, tag, out_dir, stop):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / f"proc-{proc}.jsonl"
    id_path = out_dir / f"proc-{proc}.ids"

    def log(event):
        event["t"] = time.time()
        event["proc"] = proc
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, ensure_ascii=False) + "\n")

    async def one(role, index):
        cid = f"lt{tag}p{proc}{role}{index}"
        started = time.perf_counter()
        pending = {}
        last_fire = None
        fires = 0
        libraries = 0
        nbytes = 0
        try:
            async with websockets.connect(
                url,
                open_timeout=20,
                close_timeout=5,
                max_size=2_000_000,
                ping_interval=20,
                ping_timeout=20,
                compression=None,
            ) as ws:
                raw = await asyncio.wait_for(ws.recv(), 20)
                nbytes += len(raw)
                hello = json.loads(raw)
                if hello.get("type") != "hello":
                    raise RuntimeError("no hello")
                log({"e": "ok", "role": role, "ms": round((time.perf_counter() - started) * 1000, 1)})
                if role == "s":
                    await ws.send(json.dumps({"type": "identify", "client_id": cid}))
                seq = 0
                next_send = time.perf_counter() + random.uniform(1, send_interval)
                while not stop.is_set():
                    timeout = 1.0
                    if role == "s":
                        timeout = max(0.05, min(timeout, next_send - time.perf_counter()))
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout)
                    except asyncio.TimeoutError:
                        raw = None
                    now = time.perf_counter()
                    if raw is not None:
                        nbytes += len(raw)
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            msg = {}
                        mtype = msg.get("type")
                        if mtype == "fire":
                            fires += 1
                            if last_fire is not None:
                                log({"e": "fire_gap", "ms": round((now - last_fire) * 1000, 1)})
                            last_fire = now
                            text = (msg.get("danmu") or {}).get("text")
                            if text in pending:
                                log({"e": "screen", "ms": round((now - pending.pop(text)) * 1000, 1)})
                        elif mtype == "library":
                            libraries += 1
                        elif mtype == "sent" and role == "s":
                            log({"e": "ack", "ms": round((now - pending.get("_ack", now)) * 1000, 1), "id": msg.get("id")})
                            if msg.get("id") is not None:
                                with id_path.open("a", encoding="utf-8") as fh:
                                    fh.write(str(msg["id"]) + "\n")
                    if role == "s" and now >= next_send and not stop.is_set():
                        seq += 1
                        text = f"LT-{tag}-{proc}-{seq}"
                        pending[text] = time.perf_counter()
                        pending["_ack"] = pending[text]
                        await ws.send(json.dumps({
                            "type": "send",
                            "client_id": cid,
                            "danmu": {
                                "name": f"LT-{tag}",
                                "text": text,
                                "avatar": "https://api.dicebear.com/7.x/avataaars/svg?seed=load",
                                "color": "#ffffff",
                                "bgStyle": "bg-black/70 border-pink-500/50",
                                "client_id": cid,
                            },
                        }, ensure_ascii=False))
                        next_send = time.perf_counter() + random.uniform(send_interval * 0.5, send_interval * 1.5)
                log({"e": "summary", "role": role, "fires": fires, "libraries": libraries, "bytes": nbytes})
        except websockets.ConnectionClosed as exc:
            log({"e": "drop", "role": role, "code": exc.code})
        except Exception as exc:
            log({"e": "fail", "role": role, "err": type(exc).__name__})

    async def main():
        total = viewers + senders
        gap = procs / ramp_rate if ramp_rate else 0
        tasks = []
        for i in range(total):
            role = "s" if i < senders else "v"
            tasks.append(asyncio.create_task(one(role, i)))
            if gap and i + 1 < total:
                await asyncio.sleep(gap)
        while not stop.is_set():
            await asyncio.sleep(0.4)
        await asyncio.sleep(1)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(main())


def split(n, procs, index):
    base, extra = divmod(n, procs)
    return base + (1 if index < extra else 0)


def run(args):
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    stop = mp.Event()
    procs = []
    for i in range(args.procs):
        proc = mp.Process(
            target=worker,
            args=(
                i,
                split(args.viewers, args.procs, i),
                split(args.senders, args.procs, i),
                args.ramp_rate,
                args.procs,
                args.hold,
                args.send_interval,
                args.url,
                args.tag,
                str(out_dir),
                stop,
            ),
        )
        proc.start()
        procs.append(proc)
    total = args.viewers + args.senders
    time.sleep(total / args.ramp_rate + args.hold)
    stop.set()
    for proc in procs:
        proc.join(90)
    ids = []
    for path in out_dir.glob("proc-*.ids"):
        ids.extend(path.read_text(encoding="utf-8").split())
    (out_dir / "sent_ids.txt").write_text("\n".join(ids), encoding="utf-8")
    print(f"done {out_dir} sent={len(ids)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--viewers", type=int, required=True)
    parser.add_argument("--senders", type=int, required=True)
    parser.add_argument("--ramp-rate", type=float, default=20)
    parser.add_argument("--hold", type=float, default=60)
    parser.add_argument("--send-interval", type=float, default=20)
    parser.add_argument("--procs", type=int, default=4)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--out", required=True)
    run(parser.parse_args())
