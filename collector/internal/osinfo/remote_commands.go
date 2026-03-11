package osinfo

const (
	remoteHostnameCommand     = `hostname`
	remoteKernelCommand       = `uname -s`
	remoteArchCommand         = `uname -m`
	remoteCPUCoreCommand      = `nproc`
	remoteCPUCommand          = `read cpu u n s i w irq si st _ < /proc/stat; t1=$((u+n+s+i+w+irq+si+st)); i1=$i; w1=$w; sleep 1; read cpu u n s i w irq si st _ < /proc/stat; t2=$((u+n+s+i+w+irq+si+st)); i2=$i; w2=$w; dt=$((t2-t1)); di=$((i2-i1)); dw=$((w2-w1)); awk -v t="$dt" -v i="$di" -v w="$dw" 'BEGIN{if(t<=0){printf "0\t0";exit} printf "%.6f\t%.6f", ((t-i)/t)*100, (w/t)*100}'`
	remoteMemInfoCommand      = `cat /proc/meminfo`
	remoteFilesystemCommand   = `df -PTB1 | awk 'NR>1 {printf "%s\t%s\t%s\t%s\t%s\t%s\n", $1, $2, $3, $4, $6, $7}'`
	remoteDiskstatsCommand    = `cat /proc/diskstats`
	remoteUptimeCommand       = `cat /proc/uptime`
	remoteNetDevCommand       = `cat /proc/net/dev`
	remoteLoadAvgCommand      = `cat /proc/loadavg`
	remoteTHPCommand          = `cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || echo unknown`
	remoteFDUsageCommand      = `set -- $(cat /proc/sys/fs/file-nr); max=$(cat /proc/sys/fs/file-max); awk -v used="$1" -v max="$max" 'BEGIN{if(max<=0){printf "0";exit} printf "%.6f", (used/max)*100}'`
	remoteMySQLFDUsageCommand = `pid=$(pgrep -xo mysqld || pgrep -xo mariadbd || true); if [ -z "$pid" ]; then printf "0"; exit 0; fi; fds=$(find /proc/$pid/fd -mindepth 1 -maxdepth 1 2>/dev/null | wc -l | tr -d ' '); soft=$(awk '/Max open files/ {print $4; exit}' /proc/$pid/limits); if [ -z "$soft" ] || [ "$soft" = "unlimited" ]; then printf "0"; exit 0; fi; awk -v f="$fds" -v s="$soft" 'BEGIN{if(s<=0){printf "0";exit} printf "%.6f", (f/s)*100}'`
)
