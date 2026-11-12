#!/bin/bash
ts=$(date +%s)
stats=$(docker stats --no-stream --format '{{.CPUPerc}}|{{.MemUsage}}' danmu-api 2>/dev/null)
conns=$(ss -tn state established '( sport = :8088 )' | tail -n +2 | wc -l | tr -d ' ')
cf=$(ps -C cloudflared -o %cpu=,rss= | head -1 | tr -s ' ' | sed 's/^ //')
health=$(curl -s --max-time 3 http://127.0.0.1:8088/api/health)
tx=$(awk 'NR>2 {s+=$10} END {print s+0}' /proc/net/dev)
printf '%s|%s|%s|%s|%s|%s\n' "$ts" "$stats" "$conns" "$cf" "$health" "$tx"
