param(
    [Parameter(Mandatory = $true)][int]$Seconds,
    [Parameter(Mandatory = $true)][string]$OutFile
)
$sshKey = "$env:USERPROFILE\.ssh\id_ed25519_39"
"ts,cpu,mem,conns,cf_cpu,cf_rss_kb,health_count,pending,tx_bytes" | Set-Content -Encoding utf8 $OutFile
$deadline = (Get-Date).AddSeconds($Seconds)
while ((Get-Date) -lt $deadline) {
    $line = ssh -i $sshKey -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=8 -p 22 root@39.108.164.203 "bash /tmp/danmu-sample-once.sh"
    if ($line) {
        $parts = $line.Trim() -split "\|"
        $cpu = $parts[1]
        $mem = $parts[2]
        $conns = ($parts[3] -replace "\s", "")
        $cf = ($parts[4] -replace "\s+", " ").Trim() -split " "
        $cfCpu = $cf[0]
        $cfRss = $cf[1]
        $health = $parts[5] | ConvertFrom-Json -ErrorAction SilentlyContinue
        $tx = $parts[6]
        $count = if ($health) { $health.count } else { "" }
        $pending = if ($health) { $health.pending } else { "" }
        "$($parts[0]),$cpu,$mem,$conns,$cfCpu,$cfRss,$count,$pending,$tx" | Add-Content -Encoding utf8 $OutFile
    }
    Start-Sleep -Seconds 5
}
