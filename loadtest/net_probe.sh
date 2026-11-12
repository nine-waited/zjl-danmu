#!/bin/bash
# 每 2 秒打印：eth0 出流量 Mbit/s、443 上 Send-Q 总和与最大值、8088 上 Send-Q 总和、health
dur=${1:-120}
prev=$(cat /sys/class/net/eth0/statistics/tx_bytes)
end=$(( $(date +%s) + dur ))
while [ "$(date +%s)" -lt "$end" ]; do
  sleep 2
  cur=$(cat /sys/class/net/eth0/statistics/tx_bytes)
  mbit=$(awk -v a="$prev" -v b="$cur" 'BEGIN{printf "%.1f", (b-a)*8/2/1e6}')
  prev=$cur
  q443=$(ss -tnH state established '( sport = :443 )' | awk '{n++; s+=$2; if($2>m)m=$2} END{printf "%d conns sendq_sum=%d max=%d", n, s, m}')
  q8088=$(ss -tnH state established '( sport = :8088 )' | awk '{s+=$2} END{printf "%d", s}')
  h=$(curl -s --max-time 2 http://127.0.0.1:8088/api/health)
  echo "$(date +%T) eth0_tx=${mbit}Mbit/s 443:[$q443] 8088_sendq=$q8088 $h"
done
