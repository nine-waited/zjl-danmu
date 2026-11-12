"""汇总一次压测的客户端 jsonl 和服务器采样。"""
import argparse
import csv
import json
from pathlib import Path


def pct(values, p):
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((p / 100) * (len(ordered) - 1)))))
    return ordered[idx]


def fmt(value):
    if value is None:
        return "-"
    return f"{value:.0f}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir")
    args = parser.parse_args()
    run = Path(args.run_dir)
    ok = fail = drop = 0
    codes = {}
    connect_ms, fire_gap, ack_ms, screen_ms = [], [], [], []
    bytes_sum = fires = libraries = 0
    for path in sorted(run.glob("proc-*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            ev = json.loads(line)
            kind = ev.get("e")
            if kind == "ok":
                ok += 1
                connect_ms.append(ev["ms"])
            elif kind == "fail":
                fail += 1
            elif kind == "drop":
                drop += 1
                codes[str(ev.get("code"))] = codes.get(str(ev.get("code")), 0) + 1
            elif kind == "fire_gap":
                fire_gap.append(ev["ms"])
            elif kind == "ack":
                ack_ms.append(ev["ms"])
            elif kind == "screen":
                screen_ms.append(ev["ms"])
            elif kind == "summary":
                bytes_sum += ev.get("bytes") or 0
                fires += ev.get("fires") or 0
                libraries += ev.get("libraries") or 0
    attempts = ok + fail
    fail_rate = (fail / attempts * 100) if attempts else 0
    drop_rate = (drop / ok * 100) if ok else 0
    rows = [
        f"连接成功 {ok} / 尝试 {attempts}，失败率 {fail_rate:.2f}%",
        f"建连耗时 p50 {fmt(pct(connect_ms, 50))} ms，p95 {fmt(pct(connect_ms, 95))} ms",
        f"掉线 {drop}，掉线率 {drop_rate:.2f}%，关闭码 {codes or '-'}",
        f"fire 间隔 p50 {fmt(pct(fire_gap, 50))} ms，p95 {fmt(pct(fire_gap, 95))} ms，样本 {len(fire_gap)}",
        f"发送回执 p50 {fmt(pct(ack_ms, 50))} ms，p95 {fmt(pct(ack_ms, 95))} ms，样本 {len(ack_ms)}",
        f"上屏延迟 p50 {fmt(pct(screen_ms, 50))} ms，p95 {fmt(pct(screen_ms, 95))} ms，max {fmt(max(screen_ms) if screen_ms else None)} ms，样本 {len(screen_ms)}",
        f"收到 fire {fires}，library {libraries}，入站约 {bytes_sum / 1_000_000:.1f} MB",
    ]
    server = run / "server.csv"
    if server.exists():
        with server.open(encoding="utf-8") as fh:
            samples = list(csv.DictReader(fh))
        if samples:
            cpus = [float(s["cpu"].rstrip("%") or 0) for s in samples if s.get("cpu")]
            conns = [int(float(s["conns"])) for s in samples if s.get("conns")]
            rows.append(
                f"服务器采样 {len(samples)} 次，容器 CPU 峰值 {max(cpus):.1f}%，8088 连接峰值 {max(conns) if conns else 0}"
            )
    text = "\n".join(rows)
    print(text)
    (run / "report.txt").write_text(text + "\n", encoding="utf-8")
    html = "<pre>" + text.replace("&", "&amp;").replace("<", "&lt;") + "</pre>"
    (run / "report.html").write_text(html, encoding="utf-8")
    stop = fail_rate > 2 or drop_rate > 1 or (pct(screen_ms, 95) or 0) > 60000
    print("STOP" if stop else "CONTINUE")


if __name__ == "__main__":
    main()
