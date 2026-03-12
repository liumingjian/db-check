#!/bin/bash
# Oracle和OS数据库监控数据采集脚本（Shell版本）
# 使用sqlplus连接数据库，使用ssh连接操作系统
# 保持原有采集逻辑不变

set -euo pipefail
IFS=$'\n\t'

# 脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 默认配置
DEFAULT_LOG_DIR="./logs"
DEFAULT_DATA_DIR="./logs"
DEFAULT_LOG_LEVEL="INFO"
DEFAULT_OS_PORT=22
DEFAULT_SQL_TIMEOUT=180  # SQL执行超时时间（秒），默认5分钟，0表示不设置超时

# 全局变量
DB_HOST=""
DB_PORT=""
DB_USERNAME=""
DB_PASSWORD=""
DBNAME=""
OS_HOST=""
OS_PORT=$DEFAULT_OS_PORT
OS_USERNAME=""
OS_PASSWORD=""
OS_SSH_KEY_PATH=""
OS_SSH_KEY_USER="root"
LOG_DIR=$DEFAULT_LOG_DIR
DATA_DIR=$DEFAULT_DATA_DIR
LOG_LEVEL=$DEFAULT_LOG_LEVEL
OS_COLLECT_INTERVAL=0
OS_COLLECT_DURATION=0
OS_COLLECT_COUNT=0
SQL_TIMEOUT=$DEFAULT_SQL_TIMEOUT  # SQL执行超时时间（秒），0表示不设置超时
LOCAL_MODE=false  # 本地采集模式标志，true表示本地采集，不需要SSH
LOCAL_MODE_FROM_ARG=false  # 是否通过命令行明确指定了本地模式

# 数据库状态全局变量（在采集过程中会被更新）
ARCHIVED_ENABLED=false
DB_RECOVERY_AREA_ENABLED=false
DB_TIME=0
BG_TIME=0

# 程序启动时间戳
PROGRAM_START_TIME=$(date +%Y%m%d_%H%M%S)

# 日志文件路径
LOG_FILE=""

# 临时文件目录
TMP_DIR="/tmp/oracle_collector_$$"
mkdir -p "$TMP_DIR"
trap "rm -rf $TMP_DIR" EXIT

# ==================== 工具函数 ====================

# 日志函数
log() {
    local level=$1
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    if [ -n "$LOG_FILE" ]; then
        echo "[$timestamp] - $level - $message" >> "$LOG_FILE"
    fi
    
    case $level in
        ERROR)
            echo "[ERROR] $message" >&2
            ;;
        WARNING)
            echo "[WARNING] $message" >&2
            ;;
        INFO)
            echo "[INFO] $message"
            ;;
        DEBUG)
            if [ "$LOG_LEVEL" = "DEBUG" ]; then
                echo "[DEBUG] $message"
            fi
            ;;
    esac
}

# 初始化日志
init_logger() {
    local log_path=$1
    local log_level=$2
    
    # 生成时间戳
    local timestamp_str=$(date +%Y%m%d_%H%M%S)
    
    # 解析日志路径
    if [ -d "$log_path" ]; then
        LOG_FILE="$log_path/db_collector_${timestamp_str}.log"
    elif [ ! -f "$log_path" ] && [[ ! "$log_path" =~ \.[^/]+$ ]]; then
        # 路径不存在且没有扩展名，视为目录
        mkdir -p "$log_path"
        LOG_FILE="$log_path/db_collector_${timestamp_str}.log"
    else
        # 文件路径，在文件名中插入时间戳
        local log_dir=$(dirname "$log_path")
        local log_filename=$(basename "$log_path")
        if [ -z "$log_dir" ] || [ "$log_dir" = "." ]; then
            log_dir="."
        fi
        local name="${log_filename%.*}"
        local ext="${log_filename##*.}"
        if [ -z "$name" ]; then
            name="db_collector"
        fi
        if [ "$name" = "$ext" ]; then
            ext="log"
        fi
        mkdir -p "$log_dir"
        LOG_FILE="$log_dir/${name}_${timestamp_str}.${ext}"
    fi
    
    # 创建日志文件
    touch "$LOG_FILE"
    
    log INFO "============================================================"
    log INFO "Oracle数据库监控数据采集脚本启动（Shell版本）"
    log INFO "日志文件位置: $(realpath "$LOG_FILE")"
    log INFO "============================================================"
}

# 检查命令是否存在
# 用法：
#   check_command cmd            # 未找到时输出ERROR日志
#   check_command cmd silent     # 未找到时不输出任何日志，由调用方自行处理
check_command() {
    local cmd=$1
    local level=${2:-ERROR}
    
    if ! command -v "$cmd" >/dev/null 2>&1; then
        if [ "$level" != "silent" ]; then
            log "$level" "命令 $cmd 未安装，请先安装"
        fi
        return 1
    fi
    return 0
}

# 检查必需的命令
check_required_commands() {
    local missing_commands=()
    
    if ! check_command sqlplus; then
        missing_commands+=("sqlplus")
    fi
    
    # 只有在非本地模式下才检查ssh命令
    if [ "$LOCAL_MODE" != "true" ]; then
        if ! check_command ssh; then
            missing_commands+=("ssh")
        fi
    fi
    
    
    if [ ${#missing_commands[@]} -gt 0 ]; then
        log ERROR "缺少必需的命令: ${missing_commands[*]}"
        return 1
    fi
    
    return 0
}

# ==================== SQLPlus相关函数 ====================

# 构建sqlplus连接字符串
build_sqlplus_conn() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local dbname=$5
    
    # 本地模式：只支持SID连接，不支持SERVICE_NAME
    if [ "$LOCAL_MODE" = "true" ]; then
        # 使用SID，设置ORACLE_SID环境变量
        export ORACLE_SID="$dbname"
        # 本地模式使用简单的连接方式
        echo "${username}/${password}"
    else
        # 远程模式：只支持通过服务名(SERVICE_NAME)连接，不支持SID
        # 使用SERVICE_NAME
        echo "${username}/${password}@${host}:${port}/${dbname}"
    fi
}

# 执行SQL查询（返回结果）
execute_sql() {
    local sql=$1
    local conn_str=$2
    local timeout=${3:-$SQL_TIMEOUT}  # 第三个参数为超时时间（秒），默认使用全局变量，0表示不设置超时
    local output_file="${TMP_DIR}/sql_result_$$.txt"
    
    # 创建临时SQL文件
    local sql_file="${TMP_DIR}/sql_$$.sql"
    cat > "$sql_file" <<EOFSQL
set pagesize 0
set linesize 32767
set feedback off
set heading on
set trimspool on
set trimout on
set colsep |
set wrap off
set recsep off
set arraysize 1
set sqlblanklines on
$sql;
exit
EOFSQL
    
    # 执行SQL（如果设置了超时且timeout命令可用，则使用超时）
    local exit_code=0
    local use_timeout=false
    local timeout_cmd=""
    
    # 如果超时时间大于0，尝试使用timeout命令
    if [ "$timeout" -gt 0 ] 2>/dev/null; then
        # 检测timeout命令（优先使用timeout，其次gtimeout）
        if command -v timeout >/dev/null 2>&1; then
            timeout_cmd="timeout"
            use_timeout=true
        elif command -v gtimeout >/dev/null 2>&1; then
            # macOS可能使用gtimeout
            timeout_cmd="gtimeout"
            use_timeout=true
        fi
    fi
    
    # 执行SQL命令
    if [ "$use_timeout" = true ] && [ -n "$timeout_cmd" ]; then
        # 使用timeout命令执行
        $timeout_cmd ${timeout}s sqlplus -S -L "$conn_str" @"$sql_file" > "$output_file" 2>&1
        exit_code=$?
        
        # 如果退出码是127（命令未找到），说明timeout命令实际不可用，回退到不使用timeout
        if [ $exit_code -eq 127 ]; then
            # 静默回退，不使用timeout
            sqlplus -S -L "$conn_str" @"$sql_file" > "$output_file" 2>&1
            exit_code=$?
        # timeout命令在超时时返回124
        elif [ $exit_code -eq 124 ]; then
            log ERROR "SQL执行超时（${timeout}秒）"
            rm -f "$output_file" "$sql_file"
            return 124
        fi
    else
        # 不使用timeout，直接执行
        sqlplus -S -L "$conn_str" @"$sql_file" > "$output_file" 2>&1
        exit_code=$?
    fi
    
    # 检查执行结果
    if [ $exit_code -eq 0 ]; then
        # 检查是否有Oracle错误
        if grep -qiE "ORA-|SP2-" "$output_file"; then
            log ERROR "SQL执行错误: $(head -5 "$output_file")"
            rm -f "$output_file" "$sql_file"
            return 1
        fi
        # 过滤掉空行和SQLPlus提示信息，确保每行正确分隔
        grep -vE "^(SQL>|Connected|Disconnected|^$)" "$output_file" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | grep -v "^$"
        rm -f "$output_file" "$sql_file"
        return 0
    else
        log ERROR "SQL执行失败（退出码: $exit_code）"
        if [ -f "$output_file" ]; then
            log ERROR "错误信息: $(head -10 "$output_file")"
            rm -f "$output_file"
        fi
        rm -f "$sql_file"
        return 1
    fi
}

# 执行SQL查询（带绑定变量，简化版本，使用变量替换）
execute_sql_with_bind() {
    local sql_template=$1
    local conn_str=$2
    shift 2
    local bind_vars=("$@")
    
    # 简单的变量替换（实际应用中可能需要更复杂的处理）
    local sql="$sql_template"
    for var in "${bind_vars[@]}"; do
        local key="${var%%=*}"
        local value="${var#*=}"
        # 替换 :key 为 value
        sql=$(echo "$sql" | sed "s/:${key}/${value}/g")
    done
    
    execute_sql "$sql" "$conn_str"
}

# 测试数据库连接
test_db_connection() {
    local conn_str=$1
    
    log INFO "测试数据库连接..."
    
    local result=$(execute_sql "SELECT 'CONNECTION_OK' FROM DUAL" "$conn_str")
    
    if echo "$result" | grep -q "CONNECTION_OK"; then
        log INFO "数据库连接成功"
        return 0
    else
        log ERROR "数据库连接失败"
        return 1
    fi
}

# ==================== SSH相关函数 ====================

# 执行SSH命令（本地模式下直接执行命令，不使用SSH）
execute_ssh() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    local command=$6
    local output_file="${TMP_DIR}/ssh_result_$$.txt"
    local error_file="${TMP_DIR}/ssh_error_$$.txt"
    
    # 如果是本地模式，直接执行命令
    if [ "$LOCAL_MODE" = "true" ]; then
        if eval "$command" > "$output_file" 2> "$error_file"; then
            # 命令成功执行
            cat "$output_file"
            rm -f "$output_file" "$error_file"
            return 0
        else
            # 命令执行失败
            local real_errors=$(cat "$error_file" | grep -v "^$")
            if [ -n "$real_errors" ]; then
                log ERROR "本地命令执行失败"
                log ERROR "错误信息: $real_errors"
            fi
            rm -f "$output_file" "$error_file"
            return 1
        fi
    fi
    
    # 远程模式：使用SSH执行命令
    # 构建SSH选项数组，避免选项解析错误
    local ssh_opts=(
        -o "StrictHostKeyChecking=no"
        -o "UserKnownHostsFile=/dev/null"
        -o "ConnectTimeout=10"
        -p "$port"
    )
    
    # 如果使用密钥认证
    if [ -n "$ssh_key_path" ] && [ -f "$ssh_key_path" ]; then
        ssh_opts+=(-i "$ssh_key_path")
        # 使用数组展开执行SSH命令，将stdout和stderr分开
        if ssh "${ssh_opts[@]}" "${username}@${host}" "$command" > "$output_file" 2> "$error_file"; then
            # 命令成功执行，过滤掉warning信息后输出
            grep -vE "(Permanently added)" "$output_file" | grep -v "^$"
            rm -f "$output_file" "$error_file"
            return 0
        else
            # 检查stderr中是否只有警告信息
            local real_errors=$(grep -vE "(Permanently added)" "$error_file" | grep -v "^$")
            if [ -z "$real_errors" ]; then
                # stderr中只有警告信息，认为命令成功执行
                grep -vE "(Permanently added)" "$output_file" | grep -v "^$"
                rm -f "$output_file" "$error_file"
                return 0
            else
                # 有真正的错误
                log ERROR "SSH命令执行失败"
                log ERROR "错误信息: $real_errors"
                rm -f "$output_file" "$error_file"
                return 1
            fi
        fi
    else
        # 使用密码认证（需要sshpass）
        if command -v sshpass >/dev/null 2>&1; then
            # 使用数组展开执行SSH命令，将stdout和stderr分开
            if sshpass -p "$password" ssh "${ssh_opts[@]}" "${username}@${host}" "$command" > "$output_file" 2> "$error_file"; then
                # 命令成功执行，过滤掉warning信息后输出
                grep -vE "(Permanently added)" "$output_file" | grep -v "^$"
                rm -f "$output_file" "$error_file"
                return 0
            else
                # 检查stderr中是否只有警告信息
                local real_errors=$(grep -vE "(Permanently added)" "$error_file" | grep -v "^$")
                if [ -z "$real_errors" ]; then
                    # stderr中只有警告信息，认为命令成功执行
                    grep -vE "(Permanently added)" "$output_file" | grep -v "^$"
                    rm -f "$output_file" "$error_file"
                    return 0
                else
                    # 有真正的错误
                    log ERROR "SSH命令执行失败"
                    log ERROR "错误信息: $real_errors"
                    rm -f "$output_file" "$error_file"
                    return 1
                fi
            fi
        else
            log ERROR "使用密码认证需要安装sshpass: apt-get install sshpass 或 yum install sshpass"
            return 1
        fi
    fi
}

# 测试SSH连接（本地模式下直接返回成功）
test_ssh_connection() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    
    # 如果是本地模式，直接返回成功
    if [ "$LOCAL_MODE" = "true" ]; then
        log INFO "本地模式：跳过SSH连接测试"
        return 0
    fi
    
    log INFO "测试SSH连接..."
    
    local result=$(execute_ssh "$host" "$port" "$username" "$password" "$ssh_key_path" "echo 'SSH_CONNECTION_OK'")
    
    if echo "$result" | grep -q "SSH_CONNECTION_OK"; then
        log INFO "SSH连接成功"
        return 0
    else
        log ERROR "SSH连接失败"
        return 1
    fi
}

# ==================== 原始数据存储函数 ====================

# 写入原始数据到文件（使用TSV格式，以制表符分隔）
write_raw_data() {
    local data_file=$1
    local data_section=$2
    local data_content=$3
    
    # 创建数据目录（如果不存在）
    local data_dir=$(dirname "$data_file")
    mkdir -p "$data_dir"
    
    # 如果文件不存在，创建它
    if [ ! -f "$data_file" ]; then
        touch "$data_file"
    fi
    
    # 写入数据：使用分隔符标记数据段
    echo "===SECTION:$data_section===" >> "$data_file"
    echo "$data_content" >> "$data_file"
    echo "===END_SECTION:$data_section===" >> "$data_file"
    
    log DEBUG "写入原始数据: section=$data_section, 文件=$data_file"
}

# 追加原始数据行（用于数组类型数据）
append_raw_data_line() {
    local data_file=$1
    local data_section=$2
    local data_line=$3
    
    # 创建数据目录（如果不存在）
    local data_dir=$(dirname "$data_file")
    mkdir -p "$data_dir"
    
    # 如果文件不存在，先写入段标记
    if [ ! -f "$data_file" ] || ! grep -q "===SECTION:$data_section===" "$data_file"; then
        echo "===SECTION:$data_section===" >> "$data_file"
    fi
    
    # 追加数据行
    echo "$data_line" >> "$data_file"
    
    log DEBUG "追加原始数据行: section=$data_section, 文件=$data_file"
}

# 结束数据段（用于数组类型数据）
end_raw_data_section() {
    local data_file=$1
    local data_section=$2
    
    log DEBUG "end_raw_data_section: 处理 section=$data_section, 文件=$data_file"
    # 如果段标记存在但结束标记不存在，添加结束标记
    if [ -f "$data_file" ]; then
        if grep -q "===SECTION:$data_section===" "$data_file"; then
            log DEBUG "end_raw_data_section: section $data_section 开始标记存在"
            if ! grep -q "===END_SECTION:$data_section===" "$data_file"; then
                log DEBUG "end_raw_data_section: 添加 section $data_section 结束标记"
                echo "===END_SECTION:$data_section===" >> "$data_file"
            else
                log DEBUG "end_raw_data_section: section $data_section 结束标记已存在，跳过"
            fi
        else
            log DEBUG "end_raw_data_section: section $data_section 开始标记不存在，无法添加结束标记"
        fi
    else
        log DEBUG "end_raw_data_section: 文件 $data_file 不存在"
    fi
}

# ==================== 数据存储结构 ====================

# 全局数据存储变量（使用文件存储）
ORACLE_DATA_FILE=""
OS_DATA_FILE=""

# 内存中的SQL文本字典和命令字典（避免频繁文件操作导致错乱）
declare -A SQLTEXT_DICT=()
declare -A SQL_COMMAND_DICT=()

# 内存中的用户名字典（避免频繁文件操作）
declare -A USERNAME_DICT=()

# 内存中的metric_data数组（避免频繁文件操作导致错乱）
declare -a METRIC_DATA_ARRAY=()

# 初始化Oracle数据存储结构
init_data_structure() {
    local data_file=$1
    local conn_str=$2
    
    ORACLE_DATA_FILE="$data_file"
    
    # 创建空的数据文件（原始数据格式）
    touch "$data_file"
    
    # 写入基本信息（使用简单的键值对格式）
    {
        echo "===SECTION:oracle-metadata==="
        echo "version="
        echo "cdb=false"
        echo "con_id=0"
        echo "dbid=0"
        echo "is_rac=false"
        echo "ash_begin_time="
        echo "ash_end_time="
        echo "db_time=$DB_TIME"
        echo "bg_time=$BG_TIME"
        echo "archived_enabled=$ARCHIVED_ENABLED"
        echo "db_recovery_area_enabled=$DB_RECOVERY_AREA_ENABLED"
        echo "host=$DB_HOST"
        echo "port=$DB_PORT"
        echo "dbname=$DBNAME"
        echo "username=$DB_USERNAME"
        echo "===END_SECTION:oracle-metadata==="
    } > "$data_file"
    
    log DEBUG "初始化数据存储结构: $data_file"
}

# 规范化数值格式
normalize_number() {
    local num=$1
    # 去除前后空格
    num=$(echo "$num" | xargs)
    # 如果为空，返回0
    [ -z "$num" ] && echo "0" && return
    # 如果以.开头（如.02），前面补0（变成0.02）
    if [[ "$num" =~ ^\. ]]; then
        echo "0$num"
    else
        echo "$num"
    fi
}

# ==================== Oracle采集函数 ====================
# 辅助函数：验证key匹配是否有效
_validate_key_match() {
    local line="$1"
    local match_start="$2"
    local match_end="$3"
    local target_key="$4"
    
    # 从匹配位置提取key
    local remaining="${line:$((match_start-1))}"
    local matched_key=""
    
    # 手动解析引号之间的内容
    if [[ "${remaining:0:1}" == '"' ]]; then
        local quote2_pos=0
        local escaped=0
        for ((i=1; i<${#remaining}; i++)); do
            local char="${remaining:$i:1}"
            if [ $escaped -eq 1 ]; then
                escaped=0
                continue
            fi
            if [ "$char" == '\' ]; then
                escaped=1
                continue
            fi
            if [ "$char" == '"' ]; then
                quote2_pos=$i
                break
            fi
        done
        
        if [ $quote2_pos -gt 0 ]; then
            matched_key="${remaining:1:$((quote2_pos-1))}"
            if [ "$matched_key" == "$target_key" ]; then
                return 0  # 匹配成功
            fi
        fi
    fi
    
    return 1  # 匹配失败
}

# 辅助函数：检查key前面是否是有效的JSON结构
_check_key_context() {
    local line="$1"
    local match_start="$2"
    
    if [ $match_start -le 1 ]; then
        return 0  # 在行首，有效
    fi
    
    local before_match="${line:0:$((match_start-1))}"
    local quote_count=$(echo "$before_match" | grep -o '"' | wc -l)
    
    # 如果引号数量是奇数，说明在字符串值中
    if [ $((quote_count % 2)) -eq 1 ]; then
        return 1  # 无效
    fi
    
    # 检查最后一个字符是否是{或,
    before_match=$(echo "$before_match" | sed 's/[[:space:]]*$//')
    if [ -n "$before_match" ]; then
        local last_char="${before_match: -1}"
        if [[ "$last_char" =~ [\{,] ]]; then
            return 0  # 有效
        else
            return 1  # 无效
        fi
    fi
    
    return 0  # 有效
}

# ==================== Oracle采集函数 ====================

# 获取ASH开始时间
get_ash_begin_time() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "获取ASH开始时间..."

    # 查询AWR快照
    local sql1="SELECT snap_id, to_char(begin_interval_time,'yyyy-mm-dd hh24:mi:ss'), to_char(t2.startup_time,'yyyy-mm-dd hh24:mi:ss'), to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss'), t1.instance_number, version
FROM dba_hist_snapshot t1, v\$instance t2 
WHERE t1.instance_number = t2.INSTANCE_NUMBER 
AND t1.begin_interval_time > sysdate - 1/4 
ORDER BY snap_id DESC"

    local result=$(execute_sql "$sql1" "$conn_str")
    local snap_id=""
    local instance_number=""
    local version=""
    local is_reboot=false
    local is_rac=false
    local cdb=false
    local ash_begin_time=""
    local ash_end_time=$(date '+%Y-%m-%d %H:%M:%S')
    
    # 过滤掉表头行（包含SNAP_ID的行）
    local data_lines=$(echo "$result" | grep -v "SNAP_ID" | grep -v "^$")
    
    if [ -n "$data_lines" ]; then
        # 计算数据行数
        local line_count=$(echo "$data_lines" | wc -l)
        
        if [ "$line_count" -ge 2 ]; then
            # 有多个快照，取最新的两个（第一行是最新的）
            local first_line=$(echo "$data_lines" | head -1)
            local second_line=$(echo "$data_lines" | sed -n '2p')
            
            # 解析字段：snap_id, begin_interval_time, startup_time, end_interval_time, instance_number, version
            # 字段顺序：1=snap_id, 2=begin_interval_time, 3=startup_time, 4=end_interval_time, 5=instance_number, 6=version
            snap_id=$(echo "$first_line" | cut -d'|' -f1 | xargs)
            ash_begin_time=$(echo "$first_line" | cut -d'|' -f2 | xargs)  # 使用begin_interval_time，不是startup_time
            instance_number=$(echo "$first_line" | cut -d'|' -f5 | xargs)
            version=$(echo "$first_line" | cut -d'|' -f6 | xargs)
            
            # 获取第二个快照的snap_id用于计算增量
            local prev_snap_id=$(echo "$second_line" | cut -d'|' -f1 | xargs)
            if [ -n "$prev_snap_id" ]; then
                # 有历史快照，可以计算增量
                snap_id="$prev_snap_id"  # 使用前一个快照ID作为基准
            fi
        elif [ "$line_count" -eq 1 ]; then
            # 只有一个快照，使用启动时间
            local first_line=$(echo "$data_lines" | head -1)
            snap_id=$(echo "$first_line" | cut -d'|' -f1 | xargs)
            ash_begin_time=$(echo "$first_line" | cut -d'|' -f2 | xargs)
            instance_number=$(echo "$first_line" | cut -d'|' -f5 | xargs)
            version=$(echo "$first_line" | cut -d'|' -f6 | xargs)
            is_reboot=true
        else
            # 没有快照数据，使用启动时间
            local sql2="SELECT to_char(startup_time,'yyyy-mm-dd hh24:mi:ss'), instance_number, version FROM v\$instance"
            local result2=$(execute_sql "$sql2" "$conn_str")
            if [ -n "$result2" ]; then
                local data_line2=$(echo "$result2" | grep -v "STARTUP_TIME" | grep -v "^$" | head -1)
                if [ -n "$data_line2" ]; then
                    ash_begin_time=$(echo "$data_line2" | cut -d'|' -f1 | xargs)
                    instance_number=$(echo "$data_line2" | cut -d'|' -f2 | xargs)
                    version=$(echo "$data_line2" | cut -d'|' -f3 | xargs)
                fi
            fi
            is_reboot=true
        fi
    else
        # 没有AWR数据，使用启动时间
        local sql2="SELECT to_char(startup_time,'yyyy-mm-dd hh24:mi:ss'), instance_number, version FROM v\$instance"
        local result2=$(execute_sql "$sql2" "$conn_str")
        if [ -n "$result2" ]; then
            local data_line2=$(echo "$result2" | grep -v "STARTUP_TIME" | grep -v "^$" | head -1)
            if [ -n "$data_line2" ]; then
                ash_begin_time=$(echo "$data_line2" | cut -d'|' -f1 | xargs)
                instance_number=$(echo "$data_line2" | cut -d'|' -f2 | xargs)
                version=$(echo "$data_line2" | cut -d'|' -f3 | xargs)
            fi
        fi
        is_reboot=true
    fi
    
    # 检查是否为RAC
    local sql3="SELECT COUNT(*) FROM gv\$instance"
    local result3=$(execute_sql "$sql3" "$conn_str")
    if [ -n "$result3" ]; then
        local inst_count=$(echo "$result3" | grep -v "COUNT" | head -1 | xargs)
        if [ "$inst_count" -gt 1 ]; then
            is_rac=true
        fi
    fi
    
    # 检查是否为CDB（Oracle 12+）
    local version_major=$(echo "$version" | cut -d'.' -f1)
    if [ "$version_major" -ge 12 ]; then
        local sql4="SELECT CDB FROM V\$DATABASE"
        local result4=$(execute_sql "$sql4" "$conn_str")
        if [ -n "$result4" ]; then
            local cdb_val=$(echo "$result4" | grep -v "CDB" | head -1 | xargs)
            if [ "$cdb_val" = "YES" ]; then
                cdb=true
            fi
        fi
    fi
    
    # 更新数据文件（原始数据格式）
    sed -i "s/^version=.*/version=$version/" "$data_file" 2>/dev/null || echo "version=$version" >> "$data_file"
    sed -i "s/^is_rac=.*/is_rac=$is_rac/" "$data_file" 2>/dev/null || echo "is_rac=$is_rac" >> "$data_file"
    sed -i "s/^cdb=.*/cdb=$cdb/" "$data_file" 2>/dev/null || echo "cdb=$cdb" >> "$data_file"
    sed -i "s/^ash_begin_time=.*/ash_begin_time=$ash_begin_time/" "$data_file" 2>/dev/null || echo "ash_begin_time=$ash_begin_time" >> "$data_file"
    sed -i "s/^ash_end_time=.*/ash_end_time=$ash_end_time/" "$data_file" 2>/dev/null || echo "ash_end_time=$ash_end_time" >> "$data_file"
    
    # 保存到全局变量（用于后续函数）- 使用export使其在子函数中可用
    export ASH_BEGIN_TIME="$ash_begin_time"
    export SNAP_ID="$snap_id"
    export IS_REBOOT="$is_reboot"
    export INSTANCE_NUMBER="$instance_number"
    export IS_RAC="$is_rac"
    export VERSION="$version"
    
    log INFO "ASH开始时间: $ash_begin_time, 快照ID: $snap_id, 是否重启: $is_reboot, 实例号: $instance_number, 是否RAC: $is_rac"
}

# 获取用户名字典（保存在内存中，不写入文件）
get_username_dict() {
    local conn_str=$1
    
    log DEBUG "获取用户名字典（保存在内存中）..."
    
    # 清空之前的字典
    USERNAME_DICT=()
    
    local sql="SELECT user_id, username FROM dba_users ORDER BY user_id"
    local result=""
    local sql_exit_code=0
    
    # 临时关闭错误退出，以便捕获错误
    set +e
    result=$(execute_sql "$sql" "$conn_str" 2>&1)
    sql_exit_code=$?
    set -e
    
    # 检查SQL执行结果
    if [ $sql_exit_code -ne 0 ]; then
        log ERROR "执行用户名字典查询失败，退出码: $sql_exit_code"
        log ERROR "错误信息: ${result:0:300}"
        return 1
    fi
    
    # 检查结果中是否包含错误信息
    if echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        log ERROR "用户名字典查询返回错误: ${result:0:300}"
        return 1
    fi
    
    if [ -n "$result" ]; then
        local count=0
        while IFS='|' read -r user_id username || [ -n "$user_id" ]; do
            # 跳过空行
            [ -z "$user_id" ] && [ -z "$username" ] && continue
            
            user_id=$(echo "$user_id" | xargs)
            username=$(echo "$username" | xargs)
            
            # 过滤掉表头行（包含USER_ID或USERNAME的行）和非数字的user_id，确保user_id是纯数字
            if [ -n "$user_id" ] && [ -n "$username" ] && \
               [[ "$user_id" =~ ^[0-9]+$ ]] && \
               [[ "$user_id" != "USER_ID" ]] && \
               [[ "$username" != "USERNAME" ]] && \
               [[ "$username" != "USER_ID" ]]; then
                USERNAME_DICT["$user_id"]="$username"
                count=$((count + 1))
            fi
        done <<< "$result"
        log INFO "用户名字典已加载到内存，共 $count 个用户"
        return 0
    else
        log WARN "未能获取用户名字典数据（查询结果为空）"
        return 0  # 返回0表示函数执行成功，只是没有数据
    fi
}

# 从内存中的用户名字典获取用户名
get_username_by_id() {
    local user_id=$1
    # 清理输入：去除前后空格
    user_id=$(echo "$user_id" | xargs)
    
    if [ -z "$user_id" ]; then
        echo "-"
        return
    fi
    
    # 从内存中的关联数组获取
    # 先尝试直接访问值（使用默认值避免 unbound variable 错误）
    local username="${USERNAME_DICT[$user_id]:-}"
    
    if [ -n "$username" ]; then
        echo "$username"
    else
        # 调试：如果找不到，尝试不同的检查方法
        # 方法1：使用 -v 检查（带引号）
        local found_by_v=false
        if [[ -v "USERNAME_DICT[$user_id]" ]]; then
            found_by_v=true
            username="${USERNAME_DICT[$user_id]}"
            log DEBUG "通过 -v 检查找到用户ID '$user_id' 对应的用户名: $username"
        fi
        
        # 方法2：遍历所有键进行匹配
        if [ -z "$username" ]; then
            for key in "${!USERNAME_DICT[@]}"; do
                if [ "$key" = "$user_id" ]; then
                    username="${USERNAME_DICT[$key]}"
                    log DEBUG "通过遍历找到用户ID '$user_id' 对应的用户名: $username"
                    break
                fi
            done
        fi
        
        if [ -n "$username" ]; then
            echo "$username"
        else
            # 如果还是找不到，打印调试信息
            log DEBUG "未找到用户ID '$user_id' 对应的用户名（字典中共 ${#USERNAME_DICT[@]} 个用户）"
            log DEBUG "尝试查找的键类型: [$(printf '%q' "$user_id")], 长度: ${#user_id}"
            # 打印字典的所有键值对（只打印前10个，避免日志过长）
            local dict_output=""
            local count=0
            for key in "${!USERNAME_DICT[@]}"; do
                if [ $count -lt 10 ]; then
                    if [ -z "$dict_output" ]; then
                        dict_output="${key}=${USERNAME_DICT[$key]}"
                    else
                        dict_output="${dict_output}, ${key}=${USERNAME_DICT[$key]}"
                    fi
                    count=$((count + 1))
                fi
                # 特别检查是否有键等于 user_id
                if [ "$key" = "$user_id" ]; then
                    log DEBUG "发现匹配的键: key=[$(printf '%q' "$key")], value=${USERNAME_DICT[$key]}"
                fi
            done
            log DEBUG "USERNAME_DICT 前10个键值对: {$dict_output}"
            echo "-"
        fi
    fi
}


# 采集Latch数据
collect_latch_data() {
    local conn_str=$1
    local data_file=$2
    local snap_id=$3
    local instance_number=$4
    local is_reboot=$5
    
    log INFO "采集Latch数据..."
    
    local sql=""
    local use_awr=false
    
    if [ "$is_reboot" != "true" ] && [ -n "$snap_id" ] && [ -n "$instance_number" ]; then
        # 使用AWR增量数据
        sql="SELECT * FROM (
SELECT t1.name, (t1.gets-t2.gets) gets, (t1.misses-t2.misses) misses, 
       (t1.sleeps-t2.sleeps) sleeps, (t1.immediate_gets-t2.immediate_gets) immediate_gets,
       (t1.immediate_misses-t2.immediate_misses) immediate_misses, 
       (t1.spin_gets-t2.spin_gets) spin_gets
FROM dba_hist_latch t2, v\$latch t1 
WHERE t2.snap_id=$snap_id AND t2.instance_number=$instance_number
AND t1.name=t2.latch_name AND t1.hash=t2.latch_hash 
ORDER BY 3 DESC) 
WHERE misses>0 AND rownum<101"
        use_awr=true
    else
        # 使用v$latch当前数据
        sql="SELECT * FROM (
SELECT NAME, GETS, MISSES, SLEEPS, IMMEDIATE_GETS, IMMEDIATE_MISSES, SPIN_GETS
FROM v\$latch
ORDER BY MISSES DESC)
WHERE MISSES>0 AND rownum <= 100"
    fi
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入原始数据（TSV格式：制表符分隔）
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:latch_data===" "$data_file"; then
            echo "===SECTION:latch_data===" >> "$data_file"
            # 写入表头（包含额外字段）
            append_raw_data_line "$data_file" "latch_data" "NAME|GETS|MISSES|SLEEPS|IMMEDIATE_GETS|IMMEDIATE_MISSES|SPIN_GETS|data_source|snap_id_list|is_reboot"
            log DEBUG "写入原始数据: section=latch_data, 文件=$data_file"
        fi
        
        while IFS='|' read -r name gets misses sleeps immediate_gets immediate_misses spin_gets; do
            name=$(echo "$name" | xargs)
            gets=$(echo "$gets" | xargs)
            misses=$(echo "$misses" | xargs)
            sleeps=$(echo "$sleeps" | xargs)
            immediate_gets=$(echo "$immediate_gets" | xargs)
            immediate_misses=$(echo "$immediate_misses" | xargs)
            spin_gets=$(echo "$spin_gets" | xargs)
            
            if [ -n "$name" ]; then
                # 处理NULL值
                [ -z "$gets" ] && gets="0"
                [ -z "$misses" ] && misses="0"
                [ -z "$sleeps" ] && sleeps="0"
                [ -z "$immediate_gets" ] && immediate_gets="0"
                [ -z "$immediate_misses" ] && immediate_misses="0"
                [ -z "$spin_gets" ] && spin_gets="0"
                
                # 去除字符串变量的前导和尾随空格
                name=$(echo "$name" | xargs)
                [ -z "$name" ] && name=""
                
                # 写入原始数据行（TSV格式）
                local data_source="V\$LATCH"
                [ "$use_awr" = true ] && data_source="AWR"
                local is_reboot_str="false"
                [ "$is_reboot" = "true" ] && is_reboot_str="true"
                
                append_raw_data_line "$data_file" "latch_data" "$name|$gets|$misses|$sleeps|$immediate_gets|$immediate_misses|$spin_gets|$data_source|$snap_id|$is_reboot_str"
            fi
        done <<< "$result"
        
        end_raw_data_section "$data_file" "latch_data"
        log INFO "Latch数据采集完成"
        return 0
    else
        log WARNING "未能获取Latch数据"
        return 1
    fi
}

# 采集等待事件数据
collect_event_info() {
    local conn_str=$1
    local data_file=$2
    local snap_id=$3
    local instance_number=$4
    local is_reboot=$5
    
    log INFO "采集等待事件数据..."
    
    local use_awr=false
    local sql_fg=""
    local sql_bg=""
    
    # 如果可以从AWR获取数据且没有重启，使用AWR增量数据
    if [ "$is_reboot" != "true" ] && [ -n "$snap_id" ] && [ -n "$instance_number" ]; then
        use_awr=true
        # 前台等待事件（使用AWR增量数据）
        sql_fg="SELECT event, waits, ROUND(wait_time/1000,2), ROUND(wait_time/waits,2) avg_wait_time FROM 
                (SELECT t1.event, (t1.total_waits_FG-t2.total_waits_FG) waits, ROUND((t1.time_waited_micro_FG - t2.time_waited_micro_FG)/1000,2) wait_time
                FROM v\$system_event t1, DBA_HIST_SYSTEM_EVENT t2 
                WHERE t2.snap_id=$snap_id AND t2.instance_number=$instance_number AND t2.wait_class NOT IN ('Idle')
                AND t1.event=t2.event_name AND t1.event_id=t2.event_id 
                ORDER BY 3 DESC) 
                WHERE waits > 0"
        # 后台等待事件（使用AWR增量数据）
        sql_bg="SELECT event, waits, ROUND(wait_time/1000,2), ROUND(wait_time/waits,2) avg_wait_time FROM 
                (SELECT t1.event, (t1.total_waits-t2.total_waits) waits, ROUND((t1.time_waited_micro - t2.time_waited_micro)/1000,2) wait_time
                FROM v\$system_event t1, DBA_HIST_SYSTEM_EVENT t2 
                WHERE t2.snap_id=$snap_id AND t2.instance_number=$instance_number AND t2.wait_class NOT IN ('Idle')
                AND t1.event=t2.event_name AND t1.event_id=t2.event_id 
                ORDER BY 3 DESC) 
                WHERE waits > 0"
    else
        use_awr=false
        # 前台等待事件（使用当前值）
        sql_fg="SELECT event, total_waits_FG, ROUND(waited_time/1000,2), ROUND(waited_time/total_waits_FG,2) FROM            
        (SELECT event, total_waits_FG, ROUND(time_waited_micro_FG/1000) waited_time FROM v\$system_event WHERE wait_class NOT IN ('Idle') ORDER BY 2 DESC) WHERE total_waits_FG > 0"
        # 后台等待事件（使用当前值）
        sql_bg="SELECT event, total_waits, ROUND(waited_time/1000,2), ROUND(waited_time/total_waits,2) FROM            
        (SELECT event, total_waits, ROUND(time_waited_micro/1000) waited_time FROM v\$system_event WHERE wait_class NOT IN ('Idle') ORDER BY 3 DESC) WHERE total_waits > 0"
    fi
    
    # 处理前台等待事件数据
    local fg_event_success=false
    local result_fg=$(execute_sql "$sql_fg" "$conn_str")
    if [ -n "$result_fg" ] && ! echo "$result_fg" | grep -qiE "ORA-|SP2-|ERROR"; then
        local record_count=0
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:fg_event_data===" "$data_file"; then
            echo "===SECTION:fg_event_data===" >> "$data_file"
            # 写入表头（包含额外字段）
            append_raw_data_line "$data_file" "fg_event_data" "NAME|WAITS|WAIT_TIME|AVG_WAIT_TIME|data_source|snap_id_list|is_reboot"
            log DEBUG "写入原始数据: section=fg_event_data, 文件=$data_file"
        fi
        
        while IFS='|' read -r event waits wait_time avg_wait_time; do
            event=$(echo "$event" | xargs)
            waits=$(echo "$waits" | xargs)
            wait_time=$(echo "$wait_time" | xargs)
            avg_wait_time=$(echo "$avg_wait_time" | xargs)
            
            # 跳过表头行和分隔线：检查第二个字段（waits）是否是数字，如果不是数字则跳过
            if ! echo "$waits" | grep -qE "^[0-9]+\.?[0-9]*$"; then
                continue
            fi
            
            if [ -n "$event" ]; then
                # 处理NULL值并规范化数字格式
                [ -z "$waits" ] && waits="0"
                wait_time=$(normalize_number "$wait_time")
                avg_wait_time=$(normalize_number "$avg_wait_time")
                
                # 写入原始数据行（TSV格式）
                local data_source="V\$SYSTEM_EVENT"
                [ "$use_awr" = true ] && data_source="AWR"
                local is_reboot_str="false"
                [ "$is_reboot" = "true" ] && is_reboot_str="true"
                local snap_id_str="$snap_id"
                [ "$use_awr" != true ] && snap_id_str=""
                
                append_raw_data_line "$data_file" "fg_event_data" "$event|$waits|$wait_time|$avg_wait_time|$data_source|$snap_id_str|$is_reboot_str"
                record_count=$((record_count + 1))
            fi
        done <<< "$result_fg"
        
        end_raw_data_section "$data_file" "fg_event_data"
        
        if [ "$record_count" -gt 0 ]; then
            local data_source="AWR历史数据"
            [ "$use_awr" != true ] && data_source="V\$SYSTEM_EVENT"
            log INFO "前台等待事件信息采集完成（数据来源：$data_source），共采集 $record_count 条记录"
            fg_event_success=true
        else
            log WARNING "未能获取前台等待事件信息或无数据"
        fi
    else
        log WARNING "未能获取前台等待事件信息或无数据"
    fi
    
    # 处理后台等待事件数据
    local bg_event_success=false
    local result_bg=$(execute_sql "$sql_bg" "$conn_str")
    if [ -n "$result_bg" ] && ! echo "$result_bg" | grep -qiE "ORA-|SP2-|ERROR"; then
        local record_count=0
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:bg_event_data===" "$data_file"; then
            echo "===SECTION:bg_event_data===" >> "$data_file"
            # 写入表头（包含额外字段）
            append_raw_data_line "$data_file" "bg_event_data" "NAME|WAITS|WAIT_TIME|AVG_WAIT_TIME|data_source|snap_id_list|is_reboot"
            log DEBUG "写入原始数据: section=bg_event_data, 文件=$data_file"
        fi
        
        while IFS='|' read -r event waits wait_time avg_wait_time; do
            event=$(echo "$event" | xargs)
            waits=$(echo "$waits" | xargs)
            wait_time=$(echo "$wait_time" | xargs)
            avg_wait_time=$(echo "$avg_wait_time" | xargs)
            
            # 跳过表头行和分隔线：检查第二个字段（waits）是否是数字，如果不是数字则跳过
            if ! echo "$waits" | grep -qE "^[0-9]+\.?[0-9]*$"; then
                continue
            fi
            
            if [ -n "$event" ]; then
                # 处理NULL值并规范化数字格式
                [ -z "$waits" ] && waits="0"
                wait_time=$(normalize_number "$wait_time")
                avg_wait_time=$(normalize_number "$avg_wait_time")
                
                # 写入原始数据行（TSV格式）
                local data_source="V\$SYSTEM_EVENT"
                [ "$use_awr" = true ] && data_source="AWR"
                local is_reboot_str="false"
                [ "$is_reboot" = "true" ] && is_reboot_str="true"
                local snap_id_str="$snap_id"
                [ "$use_awr" != true ] && snap_id_str=""
                
                append_raw_data_line "$data_file" "bg_event_data" "$event|$waits|$wait_time|$avg_wait_time|$data_source|$snap_id_str|$is_reboot_str"
                record_count=$((record_count + 1))
            fi
        done <<< "$result_bg"
        
        end_raw_data_section "$data_file" "bg_event_data"
        
        if [ "$record_count" -gt 0 ]; then
            local data_source="AWR历史数据"
            [ "$use_awr" != true ] && data_source="V\$SYSTEM_EVENT"
            log INFO "后台等待事件信息采集完成（数据来源：$data_source），共采集 $record_count 条记录"
            bg_event_success=true
        else
            log WARNING "未能获取后台等待事件信息或无数据"
        fi
    else
        log WARNING "未能获取后台等待事件信息或无数据"
    fi
    
    # 只要有一个成功就返回成功
    if [ "$fg_event_success" = true ] || [ "$bg_event_success" = true ]; then
        log INFO "等待事件数据采集完成"
    else
        log WARNING "等待事件数据采集失败"
    fi
}

# 采集指标数据
collect_metric_info() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集指标数据..."
    
    local sql="SELECT metric_name,
            ROUND(AVG(value), 2) as avg_value,
            ROUND(MAX(value), 2) as max_value,
            ROUND(MIN(value), 2) as min_value,
            ROUND(PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY value), 2) as percentile_95
        FROM V\$SYSMETRIC_HISTORY 
        WHERE metric_name IN (
            'Buffer Cache Hit Ratio','Redo Allocation Hit Ratio','Physical Reads Per Sec',
            'Physical Writes Per Sec','Physical Reads Direct Per Sec','Physical Writes Direct Per Sec',
            'Redo Generated Per Sec','Logons Per Sec','User Transaction Per Sec','Logical Reads Per Sec',
            'Leaf Node Splits Per Sec','Branch Node Splits Per Sec','Shared Pool Free %',
            'Average Active Sessions','Executions Per Sec','Hard Parse Count Per Sec','Soft Parse Ratio',
            'Full Index Scans Per Sec','Total Table Scans Per Sec','Long Table Scans Per Sec','Total Parse Count Per Sec',
            'Parse Failure Count Per Sec','Session Limit %','Memory Sorts Ratio','Library Cache Hit Ratio','Cursor Cache Hit Ratio','Execute Without Parse Ratio'
        )  and begin_time >= TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS')
        GROUP BY metric_name"
    # echo $sql
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 将数据追加到内存数组（避免频繁文件操作导致错乱）
        while IFS='|' read -r line; do
            if [ -z "$line" ] || echo "$line" | grep -qE "^(SELECT|FROM|WHERE|ORDER|GROUP|rownum|METRIC_NAME)"; then
                continue
            fi
            
            # 解析字段：metric_name|avg_value|max_value|min_value|percentile_95
            local fields=()
            IFS='|' read -ra fields <<< "$line"
            
            if [ ${#fields[@]} -ge 5 ]; then
                local metric_name=$(echo "${fields[0]}" | xargs)
                local avg_value=$(normalize_number "${fields[1]}")
                local max_value=$(normalize_number "${fields[2]}")
                local min_value=$(normalize_number "${fields[3]}")
                local percentile_95=$(normalize_number "${fields[4]}")
                
                # 获取指标ID
                local metric_id=$(get_metric_id "$metric_name" "oracle")
                
                if [ -n "$metric_id" ]; then
                    # 使用指标ID追加到内存数组（使用|分隔）
                    append_metric_data_to_memory "$metric_id|$avg_value|$max_value|$min_value|$percentile_95"
                else
                    # 如果找不到映射，使用原名称
                    append_metric_data_to_memory "$metric_name|$avg_value|$max_value|$min_value|$percentile_95"
                fi
            fi
        done <<< "$result"
        
        log INFO "指标数据采集完成"
    else
        log WARNING "未能获取指标数据"
    fi
}

# 采集数据库设置
collect_db_settings() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "采集数据库设置..."
    
    local sql="select name,value from v\$parameter where name in ('shared_pool_size','large_pool_size','java_pool_size',
        'pga_aggregate_target','sga_target','memory_max_target','memory_target','log_buffer','db_cache_size',
        'session_cached_cursors','open_cursors','processes','undo_retention','recyclebin','cursor_sharing','sessions','audit_trail','undo_tablespace',
        'parallel_force_local','parallel_max_servers','deferred_segment_creation','db_block_size')"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入原始数据（TSV格式）
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:db_settings===" "$data_file"; then
            echo "===SECTION:db_settings===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "db_settings" "name|value"
            log DEBUG "写入原始数据: section=db_settings, 文件=$data_file"
        fi
        
        while IFS='|' read -r name value; do
            name=$(echo "$name" | xargs)
            value=$(echo "$value" | xargs)
            
            if [ -n "$name" ]; then
                [ -z "$name" ] && name=""
                [ -z "$value" ] && value=""
                append_raw_data_line "$data_file" "db_settings" "$name|$value"
            fi
        done <<< "$result"
        end_raw_data_section "$data_file" "db_settings"
        
        log INFO "数据库设置采集完成"
    fi
}

# 采集容量信息
collect_capacity_info() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "采集容量信息..."
    
    # 获取CDB和版本信息（从数据文件或全局变量）
    local cdb=$(grep "^cdb=" "$data_file" 2>/dev/null | cut -d'=' -f2 || echo "false")
    [ -z "$cdb" ] && cdb="false"
    local version=$(grep "^version=" "$data_file" 2>/dev/null | cut -d'=' -f2 || echo "$VERSION")
    [ -z "$version" ] && version="$VERSION"
    local is_rac=$(grep "^is_rac=" "$data_file" 2>/dev/null | cut -d'=' -f2 || echo "$IS_RAC")
    [ -z "$is_rac" ] && is_rac="$IS_RAC"
    
    # 写入section开始标记（如果不存在）
    if [ ! -f "$data_file" ] || ! grep -q "===SECTION:capacity_data===" "$data_file"; then
        echo "===SECTION:capacity_data===" >> "$data_file"
        # 写入表头（根据CDB模式动态调整）
        if [ "$cdb" = "true" ]; then
            append_raw_data_line "$data_file" "capacity_data" "type|db_name|tablespace_name|max_space_mb|space_mb|used_space_mb|used_rate|used_rate_max|free_space_mb|status|auto_extend|con_id"
        else
            append_raw_data_line "$data_file" "capacity_data" "type|tablespace_name|max_space_mb|space_mb|used_space_mb|used_rate|used_rate_max|free_space_mb|status|auto_extend"
        fi
        log DEBUG "写入原始数据: section=capacity_data, 文件=$data_file"
    fi
    
    # 1. 采集表空间信息（区分CDB和非CDB）
    local sql_tablespace=""
    if [ "$cdb" = "true" ]; then
        # CDB模式：查询所有容器的表空间信息
        sql_tablespace="WITH data_files AS (
    SELECT con_id,
        TABLESPACE_NAME,
        ROUND(SUM(BYTES) / (1024 * 1024), 2) SPACE,
        ROUND(SUM(decode(MAXBYTES, 0, BYTES, MAXBYTES)) / (1024 * 1024), 2) MAXSPACE,
        max(autoextensible) AUTO_EXTEND
    FROM CDB_DATA_FILES
    GROUP BY con_id, TABLESPACE_NAME
),
free_space AS (
    SELECT dfs.con_id,
        dfs.TABLESPACE_NAME,
        round(sum(dfs.bytes)/(1024 * 1024), 2) FREE_SPACE
    FROM CDB_FREE_SPACE dfs
    GROUP BY dfs.con_id, dfs.TABLESPACE_NAME
),
undo_space AS (
    SELECT con_id,
        TABLESPACE_NAME,
        sum(bytes) TOTAL_BYTES
    FROM cdb_undo_extents
    WHERE STATUS = 'EXPIRED'
    GROUP BY con_id, TABLESPACE_NAME
)
SELECT nvl(P.name, 'CDB\$ROOT') as DB_NAME,
    D.TABLESPACE_NAME,
    D.MAXSPACE,
    D.SPACE,
    D.SPACE - NVL(F.FREE_SPACE, 0) - NVL(U.TOTAL_BYTES/(1024*1024), 0) USED_SPACE,
    ROUND((1 - (NVL(F.FREE_SPACE, 0) + NVL(U.TOTAL_BYTES/(1024*1024), 0)) / D.SPACE) * 100, 2) USED_RATE,
    ROUND(((D.SPACE - NVL(F.FREE_SPACE, 0) - NVL(U.TOTAL_BYTES/(1024*1024), 0)) / D.MAXSPACE) * 100, 2) USED_RATE_MAX,
    NVL(F.FREE_SPACE, 0) + NVL(U.TOTAL_BYTES/(1024*1024), 0) FREE_SPACE,
    E.STATUS,
    D.AUTO_EXTEND,
    D.con_id
FROM data_files D
LEFT JOIN free_space F ON (D.TABLESPACE_NAME = F.TABLESPACE_NAME AND D.con_id = F.con_id)
LEFT JOIN undo_space U ON (D.TABLESPACE_NAME = U.TABLESPACE_NAME AND D.con_id = U.con_id)
LEFT JOIN cdb_tablespaces E ON (D.TABLESPACE_NAME = E.TABLESPACE_NAME AND D.con_id = E.con_id)
LEFT JOIN v\$pdbs P ON (D.con_id = P.con_id)
ORDER BY D.con_id, D.TABLESPACE_NAME"
    else
        # 非CDB模式：查询表空间信息
        sql_tablespace="WITH data_files AS (
    SELECT TABLESPACE_NAME,
        ROUND(SUM(BYTES) / (1024 * 1024), 2) SPACE,
        ROUND(SUM(decode(MAXBYTES, 0, BYTES, MAXBYTES)) / (1024 * 1024), 2) MAXSPACE,
        max(autoextensible) AUTO_EXTEND
    FROM DBA_DATA_FILES
    GROUP BY TABLESPACE_NAME
),
free_space AS (
    SELECT dfs.TABLESPACE_NAME,
        round(sum(dfs.bytes)/(1024 * 1024), 2) FREE_SPACE
    FROM DBA_FREE_SPACE dfs
    GROUP BY dfs.TABLESPACE_NAME
),
undo_space AS (
    SELECT TABLESPACE_NAME,
        sum(bytes) TOTAL_BYTES
    FROM dba_undo_extents
    WHERE STATUS = 'EXPIRED'
    GROUP BY TABLESPACE_NAME
)
SELECT D.TABLESPACE_NAME,
    D.MAXSPACE,
    D.SPACE,
    D.SPACE - NVL(F.FREE_SPACE, 0) - NVL(U.TOTAL_BYTES/(1024*1024), 0) USED_SPACE,
    ROUND((1 - (NVL(F.FREE_SPACE, 0) + NVL(U.TOTAL_BYTES/(1024*1024), 0)) / D.SPACE) * 100, 2) USED_RATE,
    ROUND(((D.SPACE - NVL(F.FREE_SPACE, 0) - NVL(U.TOTAL_BYTES/(1024*1024), 0)) / D.MAXSPACE) * 100, 2) USED_RATE_MAX,
    NVL(F.FREE_SPACE, 0) + NVL(U.TOTAL_BYTES/(1024*1024), 0) FREE_SPACE,
    E.STATUS,
    D.AUTO_EXTEND
FROM data_files D
LEFT JOIN free_space F ON (D.TABLESPACE_NAME = F.TABLESPACE_NAME)
LEFT JOIN undo_space U ON (D.TABLESPACE_NAME = U.TABLESPACE_NAME)
LEFT JOIN dba_tablespaces E ON (D.TABLESPACE_NAME = E.TABLESPACE_NAME)
ORDER BY D.TABLESPACE_NAME"
    fi
    
    local result_tablespace=$(execute_sql "$sql_tablespace" "$conn_str")
    if [ -n "$result_tablespace" ] && ! echo "$result_tablespace" | grep -qiE "ORA-|SP2-|ERROR"; then
        while IFS='|' read -r line; do
            if [ -z "$line" ] || echo "$line" | grep -qE "^(SELECT|FROM|WHERE|ORDER|GROUP|WITH|DB_NAME|TABLESPACE_NAME)"; then
                continue
            fi
            
            local fields=()
            IFS='|' read -ra fields <<< "$line"
            
            if [ "$cdb" = "true" ]; then
                # CDB模式：包含容器ID
                if [ ${#fields[@]} -ge 11 ]; then
                    local db_name=$(echo "${fields[0]}" | xargs)
                    [ -z "$db_name" ] && db_name="CDB\$ROOT"
                    local tablespace_name=$(echo "${fields[1]}" | xargs)
                    local max_space=$(echo "${fields[2]}" | xargs)
                    local space=$(echo "${fields[3]}" | xargs)
                    local used_space=$(echo "${fields[4]}" | xargs)
                    local used_rate=$(normalize_number "$(echo "${fields[5]}" | xargs)")
                    local used_rate_max=$(normalize_number "$(echo "${fields[6]}" | xargs)")
                    local free_space=$(echo "${fields[7]}" | xargs)
                    local status=$(echo "${fields[8]}" | xargs)
                    local auto_extend=$(echo "${fields[9]}" | xargs)
                    local con_id=$(echo "${fields[10]}" | xargs)
                    
                    # 写入原始数据行（TSV格式，移除DB_NAME字段）
                    append_raw_data_line "$data_file" "capacity_data" "tablespace|$db_name|$tablespace_name|$max_space|$space|$used_space|$used_rate|$used_rate_max|$free_space|$status|$auto_extend|$con_id"
                fi
            else
                # 非CDB模式
                if [ ${#fields[@]} -ge 9 ]; then
                    local tablespace_name=$(echo "${fields[0]}" | xargs)
                    local max_space=$(echo "${fields[1]}" | xargs)
                    local space=$(echo "${fields[2]}" | xargs)
                    local used_space=$(echo "${fields[3]}" | xargs)
                    local used_rate=$(normalize_number "$(echo "${fields[4]}" | xargs)")
                    local used_rate_max=$(normalize_number "$(echo "${fields[5]}" | xargs)")
                    local free_space=$(echo "${fields[6]}" | xargs)
                    local status=$(echo "${fields[7]}" | xargs)
                    local auto_extend=$(echo "${fields[8]}" | xargs)
                    
                    # 写入原始数据行（TSV格式）
                    append_raw_data_line "$data_file" "capacity_data" "tablespace|$tablespace_name|$max_space|$space|$used_space|$used_rate|$used_rate_max|$free_space|$status|$auto_extend"
                fi
            fi
        done <<< "$result_tablespace"
    fi
    
    # 2. 归档空间使用率（如果开了归档）
    local sql_archive_mode="SELECT log_mode FROM v\$database"
    local result_archive_mode=$(execute_sql "$sql_archive_mode" "$conn_str")
    local archive_mode_enabled=false
    if [ -n "$result_archive_mode" ] && ! echo "$result_archive_mode" | grep -qiE "ORA-|SP2-|ERROR"; then
        local log_mode=$(echo "$result_archive_mode" | grep -v "LOG_MODE" | head -1 | xargs | tr '[:lower:]' '[:upper:]')
        if [ "$log_mode" = "ARCHIVELOG" ]; then
            archive_mode_enabled=true
            # 更新全局变量和数据文件（与Python版本一致）
            ARCHIVED_ENABLED=true
            export ARCHIVED_ENABLED
            sed -i "s/^archived_enabled=.*/archived_enabled=true/" "$data_file" 2>/dev/null || echo "archived_enabled=true" >> "$data_file"
        fi
    fi
    
    if [ "$archive_mode_enabled" = "true" ]; then
        # 获取归档目标目录，判断是否使用FRA
        local sql_archive_dest="SELECT value FROM v\$parameter WHERE name = 'log_archive_dest_1'"
        local result_archive_dest=$(execute_sql "$sql_archive_dest" "$conn_str")
        local archive_dest_value=""
        local archive_dest=""
        local use_fra=false
        
        if [ -n "$result_archive_dest" ] && ! echo "$result_archive_dest" | grep -qiE "ORA-|SP2-|ERROR"; then
            archive_dest_value=$(echo "$result_archive_dest" | grep -v "VALUE" | head -1 | xargs | tr '[:lower:]' '[:upper:]')
            if echo "$archive_dest_value" | grep -q "USE_DB_RECOVERY_FILE_DEST"; then
                use_fra=true
            elif echo "$archive_dest_value" | grep -q "LOCATION="; then
                archive_dest=$(echo "$archive_dest_value" | sed 's/.*LOCATION=\([^ ]*\).*/\1/' | xargs)
            fi
        fi
        log DEBUG "archive_dest: $archive_dest"
        log DEBUG "use_fra: $use_fra"
        # 查询归档日志统计信息
        local sql_archive_logs="SELECT 
    COUNT(*) as archive_count,
    ROUND(SUM(BLOCKS * BLOCK_SIZE) / (1024 * 1024), 2) as total_size_mb,
    MIN(FIRST_TIME) as oldest_archive_time,
    MAX(FIRST_TIME) as newest_archive_time
FROM v\$archived_log
WHERE DEST_ID = 1
    AND ARCHIVED = 'YES'
    AND DELETED = 'NO'"
        local result_archive_logs=$(execute_sql "$sql_archive_logs" "$conn_str")
        local archive_count=0
        local total_size_mb=0
        local oldest_archive_time=""
        local newest_archive_time=""
        
        if [ -n "$result_archive_logs" ] && ! echo "$result_archive_logs" | grep -qiE "ORA-|SP2-|ERROR"; then
            local archive_line=$(echo "$result_archive_logs" | grep -v "ARCHIVE_COUNT" | head -1)
            if [ -n "$archive_line" ]; then
                local archive_fields=()
                IFS='|' read -ra archive_fields <<< "$archive_line"
                if [ ${#archive_fields[@]} -ge 4 ]; then
                    archive_count=$(echo "${archive_fields[0]}" | xargs)
                    total_size_mb=$(echo "${archive_fields[1]}" | xargs)
                    oldest_archive_time=$(echo "${archive_fields[2]}" | xargs)
                    newest_archive_time=$(echo "${archive_fields[3]}" | xargs)
                fi
            fi
        fi
        
        if [ "$use_fra" = "true" ]; then
            # 如果使用FRA，查询FRA信息并合并归档日志统计
            # 检查是否已添加归档表头（FRA类型）
            if ! grep -q "^archive|name|" "$data_file" 2>/dev/null; then
                append_raw_data_line "$data_file" "capacity_data" "type|name|space_limit_mb|space_used_mb|space_reclaimable_mb|space_free_mb|used_rate|number_of_files|archive_count|total_size_mb|oldest_archive_time|newest_archive_time|use_fra|use_diskgroup"
            fi
            
            local sql_fra="SELECT 
    name,
    ROUND(space_limit / (1024 * 1024), 2) as space_limit_mb,
    ROUND(space_used / (1024 * 1024), 2) as space_used_mb,
    ROUND(space_reclaimable / (1024 * 1024), 2) as space_reclaimable_mb,
    ROUND((space_limit - space_used) / (1024 * 1024), 2) as space_free_mb,
    ROUND((space_used / space_limit) * 100, 2) as used_rate,
    number_of_files
FROM v\$recovery_file_dest
WHERE name IS NOT NULL"
            local result_fra=$(execute_sql "$sql_fra" "$conn_str")
            if [ -n "$result_fra" ] && ! echo "$result_fra" | grep -qiE "ORA-|SP2-|ERROR"; then
                while IFS='|' read -r line; do
                    if [ -z "$line" ] || echo "$line" | grep -qE "^(SELECT|FROM|WHERE|NAME)"; then
                        continue
                    fi
                    
                    local fra_fields=()
                    IFS='|' read -ra fra_fields <<< "$line"
                    if [ ${#fra_fields[@]} -ge 7 ]; then
                        local fra_name=$(echo "${fra_fields[0]}" | xargs)
                        local space_limit_mb=$(echo "${fra_fields[1]}" | xargs)
                        local space_used_mb=$(echo "${fra_fields[2]}" | xargs)
                        local space_reclaimable_mb=$(echo "${fra_fields[3]}" | xargs)
                        local space_free_mb=$(echo "${fra_fields[4]}" | xargs)
                        local used_rate=$(normalize_number "$(echo "${fra_fields[5]}" | xargs)")
                        log DEBUG "used_rate: $used_rate"
                        local number_of_files=$(echo "${fra_fields[6]}" | xargs)
                        log DEBUG "number_of_files: $number_of_files"
                        
                        local use_diskgroup="false"
                        if echo "$fra_name" | grep -q "^+" && [ "$is_rac" = "true" ]; then
                            use_diskgroup="true"
                        fi
                        
                        # 写入原始数据行（TSV格式）
                        [ -z "$oldest_archive_time" ] && oldest_archive_time=""
                        [ -z "$newest_archive_time" ] && newest_archive_time=""
                        append_raw_data_line "$data_file" "capacity_data" "archive|$fra_name|$space_limit_mb|$space_used_mb|$space_reclaimable_mb|$space_free_mb|$used_rate|$number_of_files|$archive_count|$total_size_mb|$oldest_archive_time|$newest_archive_time|true|$use_diskgroup"
                    fi
                done <<< "$result_fra"
            fi
        else
            # 如果不使用FRA，只保留归档日志信息
            if [ -n "$archive_dest" ]; then
                local use_diskgroup="false"
                if echo "$archive_dest" | grep -q "^+" && [ "$is_rac" = "true" ]; then
                    # 查询磁盘组信息
                    # 检查是否已添加归档表头（磁盘组类型）
                    if ! grep -q "^type|name|" "$data_file" 2>/dev/null; then
                        append_raw_data_line "$data_file" "capacity_data" "type|name|total_mb|used_mb|used_rate|free_mb|archive_count|archive_size_mb|oldest_archive_time|newest_archive_time|use_fra|use_diskgroup"
                    fi
                    
                    local diskgroup_name=$(echo "$archive_dest" | sed 's|^+||' | cut -d'/' -f1 | tr '[:lower:]' '[:upper:]')
                    log DEBUG "diskgroup_name: $diskgroup_name"
                    local sql_dg="SELECT TOTAL_MB, FREE_MB FROM v\$asm_diskgroup_stat WHERE NAME = '$diskgroup_name'"
                    local result_dg=$(execute_sql "$sql_dg" "$conn_str")
                    log DEBUG "result_dg: $result_dg"
                    if [ -n "$result_dg" ] && ! echo "$result_dg" | grep -qiE "ORA-|SP2-|ERROR"; then
                        local dg_line=$(echo "$result_dg" | grep -v "TOTAL_MB" | head -1)
                        if [ -n "$dg_line" ]; then
                            local dg_fields=()
                            log DEBUG "dg_line: $dg_line"
                            IFS='|' read -ra dg_fields <<< "$dg_line"
                            log DEBUG "dg_fields: ${dg_fields[@]}"
                            if [ ${#dg_fields[@]} -ge 2 ]; then
                                local total_mb=$(echo "${dg_fields[0]}" | xargs)
                                local free_mb=$(echo "${dg_fields[1]}" | xargs)
                                local used_mb=$((total_mb - free_mb))
                                local used_rate=$(normalize_number "$(awk "BEGIN {printf \"%.2f\", ($used_mb / $total_mb) * 100}")")
                                use_diskgroup="true"
                                
                                # 写入原始数据行（TSV格式）
                                [ -z "$oldest_archive_time" ] && oldest_archive_time="-"
                                [ -z "$newest_archive_time" ] && newest_archive_time="-"
                                append_raw_data_line "$data_file" "capacity_data" "archive|$archive_dest|$total_mb|$used_mb|$used_rate|$free_mb|$archive_count|$total_size_mb|$oldest_archive_time|$newest_archive_time|false|$use_diskgroup"
                            fi
                        fi
                    fi
                elif [ -n "$archive_dest" ] && ! echo "$archive_dest" | grep -q "+"; then
                    # 检查是否已添加归档表头（普通文件系统类型）
                    if ! grep -q "^type|name|" "$data_file" 2>/dev/null; then
                        append_raw_data_line "$data_file" "capacity_data" "type|name|archive_count|total_size_mb|oldest_archive_time|newest_archive_time|use_fra|use_diskgroup"
                    fi
                    
                    # 写入原始数据行（TSV格式）
                    [ -z "$oldest_archive_time" ] && oldest_archive_time="-"
                    [ -z "$newest_archive_time" ] && newest_archive_time="-"
                    append_raw_data_line "$data_file" "capacity_data" "archive|$archive_dest|$archive_count|$total_size_mb|$oldest_archive_time|$newest_archive_time|false|false"
                fi
            fi
        fi
    fi
    
    # 3. 采集磁盘组信息（如果存在ASM）
    local version_major=$(echo "$version" | cut -d'.' -f1)
    if [ -n "$version_major" ] && [ "$version_major" -ge 10 ]; then
        local sql_diskgroup=""
        if [ "$version_major" -ge 11 ]; then
            sql_diskgroup="SELECT
    GROUP_NUMBER,
    NAME,
    BLOCK_SIZE,
    STATE,
    TYPE,
    TOTAL_MB,
    FREE_MB,
    HOT_USED_MB,
    COLD_USED_MB,
    REQUIRED_MIRROR_FREE_MB,
    USABLE_FILE_MB,
    OFFLINE_DISKS,
    COMPATIBILITY,
    DATABASE_COMPATIBILITY,
    VOTING_FILES
FROM v\$asm_diskgroup_stat"
        else
            sql_diskgroup="SELECT
    GROUP_NUMBER,
    NAME,
    BLOCK_SIZE,
    STATE,
    TYPE,
    TOTAL_MB,
    FREE_MB,
    REQUIRED_MIRROR_FREE_MB,
    USABLE_FILE_MB,
    OFFLINE_DISKS,
    COMPATIBILITY,
    DATABASE_COMPATIBILITY
FROM v\$asm_diskgroup_stat"
        fi
        
        local result_diskgroup=$(execute_sql "$sql_diskgroup" "$conn_str")
        if [ -n "$result_diskgroup" ] && ! echo "$result_diskgroup" | grep -qiE "ORA-|SP2-|ERROR"; then
            # 检查是否已添加磁盘组表头
            if [ "$version_major" -ge 11 ]; then
                if ! grep -q "^type|group_number|name|block_size|state|redundancy_type|total_mb|free_mb|hot_used_mb|cold_used_mb|required_mirror_free_mb|usable_file_mb|offline_disks|compatibility|database_compatibility|voting_files" "$data_file" 2>/dev/null; then
                    append_raw_data_line "$data_file" "capacity_data" "type|group_number|name|block_size|state|redundancy_type|total_mb|free_mb|hot_used_mb|cold_used_mb|required_mirror_free_mb|usable_file_mb|offline_disks|compatibility|database_compatibility|voting_files"
                fi
            else
                if ! grep -q "^type|group_number|name|block_size|state|redundancy_type|total_mb|free_mb|required_mirror_free_mb|usable_file_mb|offline_disks|compatibility|database_compatibility" "$data_file" 2>/dev/null; then
                    append_raw_data_line "$data_file" "capacity_data" "type|group_number|name|block_size|state|redundancy_type|total_mb|free_mb|required_mirror_free_mb|usable_file_mb|offline_disks|compatibility|database_compatibility"
                fi
            fi
            
            while IFS='|' read -r line; do
                if [ -z "$line" ] || echo "$line" | grep -qE "^(SELECT|FROM|GROUP_NUMBER|NAME)"; then
                    continue
                fi
                
                local dg_fields=()
                IFS='|' read -ra dg_fields <<< "$line"
                
                if [ "$version_major" -ge 11 ]; then
                    if [ ${#dg_fields[@]} -ge 15 ]; then
                        # 写入原始数据行（TSV格式）
                        local dg_name=$(echo "${dg_fields[1]}" | xargs)
                        local dg_state=$(echo "${dg_fields[3]}" | xargs)
                        local dg_type=$(echo "${dg_fields[4]}" | xargs)
                        local dg_compat=$(echo "${dg_fields[12]}" | xargs)
                        local dg_db_compat=$(echo "${dg_fields[13]}" | xargs)
                        local dg_voting=$(echo "${dg_fields[14]}" | xargs)
                        append_raw_data_line "$data_file" "capacity_data" "disk_group|${dg_fields[0]}|$dg_name|${dg_fields[2]}|$dg_state|$dg_type|${dg_fields[5]}|${dg_fields[6]}|${dg_fields[7]}|${dg_fields[8]}|${dg_fields[9]}|${dg_fields[10]}|${dg_fields[11]}|$dg_compat|$dg_db_compat|$dg_voting"
                    fi
                else
                    # Oracle 10g版本（12个字段）
                    if [ ${#dg_fields[@]} -ge 12 ]; then
                        # 写入原始数据行（TSV格式）
                        local dg_name=$(echo "${dg_fields[1]}" | xargs)
                        local dg_state=$(echo "${dg_fields[3]}" | xargs)
                        local dg_type=$(echo "${dg_fields[4]}" | xargs)
                        local dg_compat=$(echo "${dg_fields[10]}" | xargs)
                        local dg_db_compat=$(echo "${dg_fields[11]}" | xargs)
                        append_raw_data_line "$data_file" "capacity_data" "disk_group|${dg_fields[0]}|$dg_name|${dg_fields[2]}|$dg_state|$dg_type|${dg_fields[5]}|${dg_fields[6]}|||${dg_fields[7]}|${dg_fields[8]}|${dg_fields[9]}|$dg_compat|$dg_db_compat"
                    fi
                fi
            done <<< "$result_diskgroup"
        fi
    fi
    
    # 结束数据段
    end_raw_data_section "$data_file" "capacity_data"
    
    # 统计各类容量信息的数量（用于日志输出）
    local tablespace_count=$(grep "^tablespace	" "$data_file" 2>/dev/null | wc -l | tr -d ' \n' || echo "0")
    local disk_group_count=$(grep "^disk_group	" "$data_file" 2>/dev/null | wc -l | tr -d ' \n' || echo "0")
    local archive_count=$(grep "^archive	" "$data_file" 2>/dev/null | wc -l | tr -d ' \n' || echo "0")
    # 确保变量是数字，如果为空则设为0
    tablespace_count=${tablespace_count:-0}
    disk_group_count=${disk_group_count:-0}
    archive_count=${archive_count:-0}
    local total_count=$((tablespace_count + disk_group_count + archive_count))
    
    local info_parts=()
    [ "$tablespace_count" -gt 0 ] && info_parts+=("表空间($tablespace_count)")
    [ "$disk_group_count" -gt 0 ] && info_parts+=("磁盘组($disk_group_count)")
    [ "$archive_count" -gt 0 ] && info_parts+=("归档($archive_count)")
    
    local info_str=""
    if [ ${#info_parts[@]} -gt 0 ]; then
        info_str=$(IFS=+; echo "${info_parts[*]}")
    else
        info_str="无数据"
    fi
    
    if [ "$total_count" -gt 0 ]; then
        log INFO "容量信息采集完成，共采集 $total_count 条记录（$info_str）"
    else
        log WARNING "容量信息采集完成，但未获取到数据"
    fi
}

# 采集时间模型信息
collect_time_model_info() {
    local conn_str=$1
    local data_file=$2
    local snap_id=$3
    local instance_number=$4
    local is_reboot=$5
    
    log INFO "采集时间模型信息..."
    
    local sql=""
    if [ "$is_reboot" != "true" ] && [ -n "$snap_id" ] && [ -n "$instance_number" ]; then
        sql="select t1.stat_name,round((t1.value-t2.value)/1000/1000,2) from V\$SYS_TIME_MODEL t1, dba_hist_sys_time_model t2 
  where t2.snap_id=$snap_id and t1.stat_name=t2.stat_name and t2.instance_number=$instance_number order by 2 desc"
    else
        sql="select stat_name,round(value/1000/1000,2) from V\$SYS_TIME_MODEL order by 2 desc"
    fi
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        local db_time=0
        
        # 使用关联数组存储时间模型数据（替代JSON）
        declare -A time_model_dict
        
        # 第一遍：查找DB time值，并更新全局变量（与Python版本一致）
        while IFS='|' read -r stat_name value; do
            stat_name=$(echo "$stat_name" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            value=$(echo "$value" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            # 如果值以小数点开头，在前面补0（如 .96 -> 0.96）
            if [[ "$value" =~ ^\.[0-9] ]]; then
                value="0$value"
            fi
            
            if [ -n "$stat_name" ] && [ -n "$value" ]; then
                # 更新全局变量（与Python版本一致）
                if [ "$stat_name" = "background elapsed time" ]; then
                    BG_TIME=$value
                    export BG_TIME
                    sed -i "s/^bg_time=.*/bg_time=$BG_TIME/" "$data_file" 2>/dev/null || echo "bg_time=$BG_TIME" >> "$data_file"
                fi
                if [ "$stat_name" = "DB time" ]; then
                    DB_TIME=$value
                    export DB_TIME
                    db_time=$value
                    sed -i "s/^db_time=.*/db_time=$DB_TIME/" "$data_file" 2>/dev/null || echo "db_time=$DB_TIME" >> "$data_file"
                fi
                
                # 存储到关联数组（使用stat_name作为键，value作为值）
                time_model_dict["$stat_name"]="$value"
            fi
        done <<< "$result"
        
        # 写入原始数据（TSV格式）
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:time_model_data===" "$data_file"; then
            echo "===SECTION:time_model_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "time_model_data" "stat_name|value|db_time_ratio"
            log DEBUG "写入原始数据: section=time_model_data, 文件=$data_file"
        fi
        
        while IFS='|' read -r stat_name value; do
            stat_name=$(echo "$stat_name" | xargs)
            value=$(echo "$value" | xargs)
            
            if [ -n "$stat_name" ]; then
                # 计算db_time_ratio
                local db_time_ratio=""
                if [ -n "$DB_TIME" ] && [ "$DB_TIME" != "0" ] && [ -n "$value" ] && [ "$value" != "0" ]; then
                    db_time_ratio=$(awk "BEGIN {printf \"%.2f\", $value * 100 / $DB_TIME}")
                else
                    db_time_ratio="0"
                fi
                
                append_raw_data_line "$data_file" "time_model_data" "$stat_name|$value|$db_time_ratio"
            fi
        done <<< "$result"
        end_raw_data_section "$data_file" "time_model_data"
        
        log INFO "时间模型信息采集完成"
    else
        log WARNING "未能获取时间模型信息"
    fi
}

# 采集长事务数据
collect_long_transaction() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "采集长事务数据..."
    
    local sql="SELECT sid, serial#, username, logon_time, program, s.status session_status,
       sql_id, event, t.xid, t.status transaction_status, t.start_time,
       ROUND((SYSDATE - t.start_date) * 24 * 60 * 60, 2) as transaction_duration
FROM v\$session s, v\$transaction t
WHERE s.taddr=t.addr 
AND ROUND((SYSDATE - t.start_date) * 24 * 60 * 60, 2) > 600"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 使用关联数组存储长事务数据（替代JSON）
        declare -A long_transaction_dict
        local has_data=false
        
        while IFS='|' read -r sid serial username logon_time program session_status sql_id event xid transaction_status start_time transaction_duration; do
            sid=$(echo "$sid" | xargs)
            serial=$(echo "$serial" | xargs)
            username=$(echo "$username" | xargs)
            logon_time=$(echo "$logon_time" | xargs)
            program=$(echo "$program" | xargs)
            session_status=$(echo "$session_status" | xargs | tr '[:lower:]' '[:upper:]')
            sql_id=$(echo "$sql_id" | xargs)
            event=$(echo "$event" | xargs)
            xid=$(echo "$xid" | xargs)
            transaction_status=$(echo "$transaction_status" | xargs | tr '[:lower:]' '[:upper:]')
            start_time=$(echo "$start_time" | xargs)
            transaction_duration=$(echo "$transaction_duration" | xargs)
            
            if [ -n "$sid" ] && [ -n "$serial" ]; then
                has_data=true
                
                # 去除所有字符串变量的前导和尾随空格，并将只包含空格的字符串设为空字符串
                username=$(echo "$username" | xargs)
                logon_time=$(echo "$logon_time" | xargs)
                program=$(echo "$program" | xargs)
                session_status=$(echo "$session_status" | xargs)
                sql_id=$(echo "$sql_id" | xargs)
                event=$(echo "$event" | xargs)
                xid=$(echo "$xid" | xargs)
                transaction_status=$(echo "$transaction_status" | xargs)
                start_time=$(echo "$start_time" | xargs)
                
                # 处理NULL值并规范化数字格式
                transaction_duration=$(normalize_number "$transaction_duration")
                [ -z "$transaction_duration" ] && transaction_duration="0"
                [ -z "$username" ] && username=""
                [ -z "$logon_time" ] && logon_time=""
                [ -z "$program" ] && program=""
                [ -z "$session_status" ] && session_status=""
                [ -z "$sql_id" ] && sql_id=""
                [ -z "$event" ] && event=""
                [ -z "$xid" ] && xid=""
                [ -z "$transaction_status" ] && transaction_status=""
                [ -z "$start_time" ] && start_time=""
                
                # 存储到关联数组（使用复合键）
                local key="${sid}_${serial}"
                long_transaction_dict["${key}_username"]="$username"
                long_transaction_dict["${key}_logon_time"]="$logon_time"
                long_transaction_dict["${key}_program"]="$program"
                long_transaction_dict["${key}_session_status"]="$session_status"
                long_transaction_dict["${key}_sql_id"]="$sql_id"
                long_transaction_dict["${key}_event"]="$event"
                long_transaction_dict["${key}_xid"]="$xid"
                long_transaction_dict["${key}_transaction_status"]="$transaction_status"
                long_transaction_dict["${key}_start_time"]="$start_time"
                long_transaction_dict["${key}_transaction_duration"]="$transaction_duration"
            fi
        done <<< "$result"
        
        if [ "$has_data" = true ]; then
            # 写入原始数据（TSV格式）
            # 写入section开始标记（如果不存在）
            if [ ! -f "$data_file" ] || ! grep -q "===SECTION:long_transaction_data===" "$data_file"; then
                echo "===SECTION:long_transaction_data===" >> "$data_file"
                # 写入表头
                append_raw_data_line "$data_file" "long_transaction_data" "SID|SERIAL#|USERNAME|LOGON_TIME|PROGRAM|SESSION_STATUS|SQL_ID|EVENT|XID|TRANSACTION_STATUS|START_TIME|TRANSACTION_DURATION"
                log DEBUG "写入原始数据: section=long_transaction_data, 文件=$data_file"
            fi
            
            while IFS='|' read -r sid serial username logon_time program session_status sql_id event xid transaction_status start_time transaction_duration; do
                sid=$(echo "$sid" | xargs)
                serial=$(echo "$serial" | xargs)
                username=$(echo "$username" | xargs)
                logon_time=$(echo "$logon_time" | xargs)
                program=$(echo "$program" | xargs)
                session_status=$(echo "$session_status" | xargs)
                sql_id=$(echo "$sql_id" | xargs)
                event=$(echo "$event" | xargs)
                xid=$(echo "$xid" | xargs)
                transaction_status=$(echo "$transaction_status" | xargs)
                start_time=$(echo "$start_time" | xargs)
                transaction_duration=$(normalize_number "$transaction_duration")
                
                if [ -n "$sid" ] && [ -n "$serial" ]; then
                    [ -z "$transaction_duration" ] && transaction_duration="0"
                    [ -z "$username" ] && username=""
                    [ -z "$logon_time" ] && logon_time=""
                    [ -z "$program" ] && program=""
                    [ -z "$session_status" ] && session_status=""
                    [ -z "$sql_id" ] && sql_id=""
                    [ -z "$event" ] && event=""
                    [ -z "$xid" ] && xid=""
                    [ -z "$transaction_status" ] && transaction_status=""
                    [ -z "$start_time" ] && start_time=""
                    
                    append_raw_data_line "$data_file" "long_transaction_data" "$sid|$serial|$username|$logon_time|$program|$session_status|$sql_id|$event|$xid|$transaction_status|$start_time|$transaction_duration"
                fi
            done <<< "$result"
            end_raw_data_section "$data_file" "long_transaction_data"
            log INFO "长事务数据采集完成"
        else
            log INFO "长事务数据采集完成，未发现长事务"
        fi
    fi
}

# 采集活跃会话信息
collect_active_session_info() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "采集活跃会话信息..."
    
    local sql="SELECT sid, serial#, username, machine, program, sql_id, sql_exec_start,
       ROUND((SYSDATE - sql_exec_start) * 24 * 60 * 60, 2) as exec_duration_seconds,
       event, state, seconds_in_wait
FROM v\$session
WHERE status='ACTIVE' AND type !='BACKGROUND' AND wait_class != 'Idle' 
AND sid != (SELECT sid FROM v\$mystat WHERE rownum=1)"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 使用关联数组存储活跃会话数据（替代JSON）
        declare -A active_session_dict
        local has_data=false
        
        while IFS='|' read -r sid serial username machine program sql_id sql_exec_start exec_duration_seconds event state seconds_in_wait; do
            sid=$(echo "$sid" | xargs)
            serial=$(echo "$serial" | xargs)
            username=$(echo "$username" | xargs)
            machine=$(echo "$machine" | xargs)
            program=$(echo "$program" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            sql_exec_start=$(echo "$sql_exec_start" | xargs)
            exec_duration_seconds=$(echo "$exec_duration_seconds" | xargs)
            event=$(echo "$event" | xargs)
            state=$(echo "$state" | xargs)
            seconds_in_wait=$(echo "$seconds_in_wait" | xargs)
            
            if [ -n "$sid" ] && [ -n "$serial" ]; then
                has_data=true
                
                # 去除所有字符串变量的前导和尾随空格，并将只包含空格的字符串设为空字符串
                username=$(echo "$username" | xargs)
                machine=$(echo "$machine" | xargs)
                program=$(echo "$program" | xargs)
                sql_id=$(echo "$sql_id" | xargs)
                sql_exec_start=$(echo "$sql_exec_start" | xargs)
                event=$(echo "$event" | xargs)
                state=$(echo "$state" | xargs)
                
                # 处理NULL值并规范化数字格式
                exec_duration_seconds=$(normalize_number "$exec_duration_seconds")
                [ -z "$exec_duration_seconds" ] && exec_duration_seconds="0"
                seconds_in_wait=$(normalize_number "$seconds_in_wait")
                [ -z "$seconds_in_wait" ] && seconds_in_wait="0"
                [ -z "$username" ] && username=""
                [ -z "$machine" ] && machine=""
                [ -z "$program" ] && program=""
                [ -z "$sql_id" ] && sql_id=""
                [ -z "$sql_exec_start" ] && sql_exec_start=""
                [ -z "$event" ] && event=""
                [ -z "$state" ] && state=""
                
                # 存储到关联数组（使用复合键）
                local key="${sid}_${serial}"
                active_session_dict["${key}_username"]="$username"
                active_session_dict["${key}_machine"]="$machine"
                active_session_dict["${key}_program"]="$program"
                active_session_dict["${key}_sql_id"]="$sql_id"
                active_session_dict["${key}_sql_exec_start"]="$sql_exec_start"
                active_session_dict["${key}_exec_duration_seconds"]="$exec_duration_seconds"
                active_session_dict["${key}_event"]="$event"
                active_session_dict["${key}_state"]="$state"
                active_session_dict["${key}_seconds_in_wait"]="$seconds_in_wait"
            fi
        done <<< "$result"
        
        if [ "$has_data" = true ]; then
            # 写入原始数据（TSV格式）
            # 写入section开始标记（如果不存在）
            if [ ! -f "$data_file" ] || ! grep -q "===SECTION:active_session_data===" "$data_file"; then
                echo "===SECTION:active_session_data===" >> "$data_file"
                # 写入表头
                append_raw_data_line "$data_file" "active_session_data" "SID|SERIAL#|USERNAME|MACHINE|PROGRAM|SQL_ID|SQL_EXEC_START|EXEC_DURATION_SECONDS|EVENT|STATE|SECONDS_IN_WAIT"
                log DEBUG "写入原始数据: section=active_session_data, 文件=$data_file"
            fi
            
            while IFS='|' read -r sid serial username machine program sql_id sql_exec_start exec_duration_seconds event state seconds_in_wait; do
                sid=$(echo "$sid" | xargs)
                serial=$(echo "$serial" | xargs)
                username=$(echo "$username" | xargs)
                machine=$(echo "$machine" | xargs)
                program=$(echo "$program" | xargs)
                sql_id=$(echo "$sql_id" | xargs)
                sql_exec_start=$(echo "$sql_exec_start" | xargs)
                exec_duration_seconds=$(normalize_number "$exec_duration_seconds")
                event=$(echo "$event" | xargs)
                state=$(echo "$state" | xargs)
                seconds_in_wait=$(normalize_number "$seconds_in_wait")
                
                if [ -n "$sid" ] && [ -n "$serial" ]; then
                    [ -z "$exec_duration_seconds" ] && exec_duration_seconds="0"
                    [ -z "$seconds_in_wait" ] && seconds_in_wait="0"
                    [ -z "$username" ] && username=""
                    [ -z "$machine" ] && machine=""
                    [ -z "$program" ] && program=""
                    [ -z "$sql_id" ] && sql_id=""
                    [ -z "$sql_exec_start" ] && sql_exec_start=""
                    [ -z "$event" ] && event=""
                    [ -z "$state" ] && state=""
                    
                    append_raw_data_line "$data_file" "active_session_data" "$sid|$serial|$username|$machine|$program|$sql_id|$sql_exec_start|$exec_duration_seconds|$event|$state|$seconds_in_wait"
                fi
            done <<< "$result"
            end_raw_data_section "$data_file" "active_session_data"
            log INFO "活跃会话信息采集完成"
        else
            log INFO "活跃会话信息采集完成，未发现活跃会话"
        fi
    fi
}

# 采集历史会话信息
collect_history_session_info() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集历史会话信息..."
    
    local sql="SELECT session_type, user_id, program, machine,
       COUNT(DISTINCT session_id || ',' || session_serial#) as distinct_sessions,
       COUNT(*) as total_samples
FROM v\$active_session_history
WHERE sample_time > TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS')
GROUP BY session_type, user_id, program, machine
ORDER BY 5 DESC, 6 DESC"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 使用关联数组存储历史会话数据（替代JSON）
        declare -A history_session_dict
        local history_session_keys=()
        local has_data=false
        
        while IFS='|' read -r session_type user_id program machine distinct_sessions total_samples; do
            session_type=$(echo "$session_type" | xargs)
            user_id=$(echo "$user_id" | xargs)
            program=$(echo "$program" | xargs)
            machine=$(echo "$machine" | xargs)
            distinct_sessions=$(echo "$distinct_sessions" | xargs)
            total_samples=$(echo "$total_samples" | xargs)
            
            if [ -n "$session_type" ]; then
                has_data=true
                
                # 从内存中的用户名字典获取用户名
                local user_name=$(get_username_by_id "$user_id")
                
                # 处理NULL值
                [ -z "$distinct_sessions" ] && distinct_sessions="0"
                [ -z "$total_samples" ] && total_samples="0"
                [ -z "$user_name" ] && user_name=""
                [ -z "$program" ] && program=""
                [ -z "$machine" ] && machine=""
                
                # 存储到关联数组（使用复合键）
                local key="${session_type}_${user_id}_${program}_${machine}"
                history_session_dict["${key}_session_type"]="$session_type"
                history_session_dict["${key}_user_name"]="$user_name"
                history_session_dict["${key}_program"]="$program"
                history_session_dict["${key}_machine"]="$machine"
                history_session_dict["${key}_distinct_sessions"]="$distinct_sessions"
                history_session_dict["${key}_total_samples"]="$total_samples"
                
                # 维护键的顺序数组
                history_session_keys+=("$key")
            fi
        done <<< "$result"
        
        if [ "$has_data" = true ]; then
            # 写入原始数据（TSV格式）
            # 写入section开始标记（如果不存在）
            if [ ! -f "$data_file" ] || ! grep -q "===SECTION:history_session_data===" "$data_file"; then
                echo "===SECTION:history_session_data===" >> "$data_file"
                # 写入表头
                append_raw_data_line "$data_file" "history_session_data" "SESSION_TYPE|USER_NAME|PROGRAM|MACHINE|DISTINCT_SESSIONS|TOTAL_SAMPLES"
                log DEBUG "写入原始数据: section=history_session_data, 文件=$data_file"
            fi
            
            # 从关联数组中读取数据并写入文件
            for key in "${history_session_keys[@]}"; do
                local session_type="${history_session_dict[${key}_session_type]}"
                local user_name="${history_session_dict[${key}_user_name]}"
                local program="${history_session_dict[${key}_program]}"
                local machine="${history_session_dict[${key}_machine]}"
                local distinct_sessions="${history_session_dict[${key}_distinct_sessions]}"
                local total_samples="${history_session_dict[${key}_total_samples]}"
                
                append_raw_data_line "$data_file" "history_session_data" "$session_type|$user_name|$program|$machine|$distinct_sessions|$total_samples"
            done
            end_raw_data_section "$data_file" "history_session_data"
            log INFO "历史会话信息采集完成"
        else
            log INFO "历史会话信息采集完成，未发现历史会话"
        fi
    fi
}

# 获取SQL总等待次数和TOP 3等待事件
get_total_wait_count() {
    local conn_str=$1
    local sql_id=$2
    local ash_begin_time=$3
    
    if [ -z "$sql_id" ] || [ "$sql_id" = "NULL" ] || [ "$sql_id" = "null" ]; then
        echo "0|无"
        return
    fi
    
    local sql="SELECT event, COUNT(*) cnt 
FROM v\$active_session_history 
WHERE sample_time BETWEEN TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS') AND SYSDATE 
AND sql_id='$sql_id' 
GROUP BY event 
ORDER BY 2 DESC"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        local total_wait_cnt=0
        local top_3_event_list=""
        local event_count=0
        
        # 第一遍：计算总等待次数
        while IFS='|' read -r event cnt; do
            event=$(echo "$event" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$cnt" ]; then
                total_wait_cnt=$((total_wait_cnt + cnt))
            fi
        done <<< "$result"
        
        # 第二遍：构建TOP 3事件
        while IFS='|' read -r event cnt; do
            event=$(echo "$event" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$cnt" ] && [ "$event_count" -lt 3 ] && [ "$total_wait_cnt" -gt 0 ]; then
                local event_pct=$(awk "BEGIN {printf \"%.2f\", $cnt * 100 / $total_wait_cnt}")
                if [ -z "$top_3_event_list" ]; then
                    top_3_event_list="${event}(等待${cnt}次，占比${event_pct}%)"
                else
                    top_3_event_list="${top_3_event_list}；${event}(等待${cnt}次，占比${event_pct}%)"
                fi
                event_count=$((event_count + 1))
            fi
        done <<< "$result"
        
        if [ -z "$top_3_event_list" ]; then
            top_3_event_list="无"
        fi
        
        echo "${total_wait_cnt}|${top_3_event_list}"
    else
        echo "0|无"
    fi
}

# 采集高执行次数SQL
collect_higher_executions_sql() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集高执行次数SQL..."
    
    local sql="SELECT * FROM 
  (SELECT sql_id, COUNT(sql_exec_id) total_cnt, SUM(duration_seconds) FROM (
    SELECT sql_id, sql_exec_id, ROUND((CAST(MAX(sample_time) AS DATE) - CAST(MIN(sample_time) AS DATE)) * 24 * 60 * 60) as duration_seconds 
    FROM v\$active_session_history 
    WHERE sample_time BETWEEN TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS') AND SYSDATE 
    AND sql_id IS NOT NULL AND sql_exec_id IS NOT NULL 
    GROUP BY sql_id, sql_exec_id
  ) GROUP BY sql_id ORDER BY 2 DESC) 
WHERE total_cnt > 10 AND ROWNUM < 11"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        local total_count=0
        local total_seconds=0
        local temp_file=$(mktemp)
        
        # 第一遍：计算总数和总秒数，获取SQL文本，保存到临时文件
        while IFS='|' read -r sql_id count sql_elapsed_time; do
            sql_id=$(echo "$sql_id" | xargs)
            count=$(echo "$count" | xargs)
            sql_elapsed_time=$(echo "$sql_elapsed_time" | xargs)
            
            if [ -z "$sql_id" ] || [ "$count" = "0" ]; then
                continue
            fi
            
            # 检查sql_id是否已经在sqltext_dict中（与Python版本一致：if sql_id not in self.sqltext_dict）
            local existing_sqltext=$(get_raw_dict_value "$data_file" "sqltext_dict" "$sql_id")
            local sqltext=""
            local command_name=""
            
            if [ -z "$existing_sqltext" ]; then
                # 如果不在dict中，则获取SQL文本和命令名称
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
            else
                # 如果已在dict中，从dict中获取（与Python版本一致）
                sqltext="$existing_sqltext"
                command_name=$(get_raw_dict_value "$data_file" "sql_command_dict" "$sql_id")
            fi
            
            # 计算每次执行的平均耗时
            local sql_elapsed_time_per_exec=$(awk "BEGIN {printf \"%.2f\", $sql_elapsed_time / $count}")
            
            total_count=$((total_count + count))
            total_seconds=$(awk "BEGIN {printf \"%.2f\", $total_seconds + $sql_elapsed_time}")
            
            # 保存到临时文件：sql_id|count|sql_elapsed_time|sql_elapsed_time_per_exec|command_name
            echo "${sql_id}|${count}|${sql_elapsed_time}|${sql_elapsed_time_per_exec}|${command_name}" >> "$temp_file"
        done <<< "$result"
        
        # 第二遍：计算比例，获取等待信息，写入原始数据
        if [ "$total_count" -gt 0 ] && [ -s "$temp_file" ]; then
            # 写入section开始标记（如果不存在）
            if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_execution_sql_dict===" "$data_file"; then
                echo "===SECTION:top_execution_sql_dict===" >> "$data_file"
                # 写入表头
                append_raw_data_line "$data_file" "top_execution_sql_dict" "sql_id|executions|elapsed_time|elapsed_time_per_exec|command_name|executions_ratio|elapsed_time_ratio|total_wait_count|top_3_event"
                log DEBUG "写入原始数据: section=top_execution_sql_dict, 文件=$data_file"
            fi
            
            while IFS='|' read -r sql_id count sql_elapsed_time sql_elapsed_time_per_exec command_name; do
                sql_id=$(echo "$sql_id" | xargs)
                count=$(echo "$count" | xargs)
                sql_elapsed_time=$(echo "$sql_elapsed_time" | xargs)
                sql_elapsed_time_per_exec=$(echo "$sql_elapsed_time_per_exec" | xargs)
                command_name=$(echo "$command_name" | xargs)
                
                # 计算比例
                local executions_ratio=$(awk "BEGIN {printf \"%.2f\", $count * 100 / $total_count}")
                local elapsed_time_ratio=$(awk "BEGIN {printf \"%.2f\", $sql_elapsed_time * 100 / ($total_seconds > 0 ? $total_seconds : 1)}")
                
                # 获取等待次数和TOP 3事件
                local wait_info=$(get_total_wait_count "$conn_str" "$sql_id" "$ash_begin_time")
                local total_wait_count=$(echo "$wait_info" | cut -d'|' -f1)
                local top_3_event=$(echo "$wait_info" | cut -d'|' -f2)
                
                # 写入原始数据行（TSV格式：制表符分隔）
                append_raw_data_line "$data_file" "top_execution_sql_dict" "$sql_id|$count|$sql_elapsed_time|$sql_elapsed_time_per_exec|$command_name|$executions_ratio|$elapsed_time_ratio|$total_wait_count|$top_3_event"
            done < "$temp_file"
            
            end_raw_data_section "$data_file" "top_execution_sql_dict"
            
            rm -f "$temp_file"
            
            log INFO "高执行次数SQL采集完成"
        else
            rm -f "$temp_file"
            log WARNING "未能获取高执行次数SQL数据"
        fi
    else
        log WARNING "未能获取高执行次数SQL数据"
    fi
}

# 采集高执行时间SQL
collect_higher_elapsed_time_sql() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集高执行时间SQL..."
    
    local sql="SELECT * FROM  
  (SELECT sql_id, COUNT(sql_exec_id), SUM(duration_seconds) total_time FROM (
    SELECT sql_id, sql_exec_id, ROUND((CAST(MAX(sample_time) AS DATE) - CAST(MIN(sample_time) AS DATE)) * 24 * 60 * 60) as duration_seconds 
    FROM v\$active_session_history 
    WHERE sample_time BETWEEN TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS') AND SYSDATE 
    AND sql_id IS NOT NULL AND sql_exec_id IS NOT NULL 
    GROUP BY sql_id, sql_exec_id
  ) GROUP BY sql_id ORDER BY 3 DESC) 
WHERE total_time > 10 AND ROWNUM < 11"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        local total_count=0
        local total_seconds=0
        local temp_file=$(mktemp)
        
        # 第一遍：计算总数和总秒数，获取SQL文本，保存到临时文件
        while IFS='|' read -r sql_id count sql_elapsed_time; do
            sql_id=$(echo "$sql_id" | xargs)
            count=$(echo "$count" | xargs)
            sql_elapsed_time=$(echo "$sql_elapsed_time" | xargs)
            
            if [ -z "$sql_id" ] || [ "$count" = "0" ]; then
                continue
            fi
            
            # 检查sql_id是否已经在sqltext_dict中（与Python版本一致：if sql_id not in self.sqltext_dict）
            local existing_sqltext=$(get_raw_dict_value "$data_file" "sqltext_dict" "$sql_id")
            local sqltext=""
            local command_name=""
            
            if [ -z "$existing_sqltext" ]; then
                # 如果不在dict中，则获取SQL文本和命令名称
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
            else
                # 如果已在dict中，从dict中获取（与Python版本一致）
                sqltext="$existing_sqltext"
                command_name=$(get_raw_dict_value "$data_file" "sql_command_dict" "$sql_id")
            fi
            
            # 计算每次执行的平均耗时
            local sql_elapsed_time_per_exec=$(awk "BEGIN {printf \"%.2f\", $sql_elapsed_time / $count}")
            
            total_count=$((total_count + count))
            total_seconds=$(awk "BEGIN {printf \"%.2f\", $total_seconds + $sql_elapsed_time}")
            
            # 保存到临时文件：sql_id|count|sql_elapsed_time|sql_elapsed_time_per_exec|command_name
            echo "${sql_id}|${count}|${sql_elapsed_time}|${sql_elapsed_time_per_exec}|${command_name}" >> "$temp_file"
        done <<< "$result"
        
        # 第二遍：计算比例，获取等待信息，写入原始数据
        if [ "$total_count" -gt 0 ] && [ -s "$temp_file" ]; then
            # 写入section开始标记（如果不存在）
            if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_elapsed_time_sql_dict===" "$data_file"; then
                echo "===SECTION:top_elapsed_time_sql_dict===" >> "$data_file"
                # 写入表头
                append_raw_data_line "$data_file" "top_elapsed_time_sql_dict" "sql_id|executions|elapsed_time|elapsed_time_per_exec|command_name|executions_ratio|elapsed_time_ratio|total_wait_count|top_3_event"
                log DEBUG "写入原始数据: section=top_elapsed_time_sql_dict, 文件=$data_file"
            fi
            
            while IFS='|' read -r sql_id count sql_elapsed_time sql_elapsed_time_per_exec command_name; do
                sql_id=$(echo "$sql_id" | xargs)
                count=$(echo "$count" | xargs)
                sql_elapsed_time=$(echo "$sql_elapsed_time" | xargs)
                sql_elapsed_time_per_exec=$(echo "$sql_elapsed_time_per_exec" | xargs)
                command_name=$(echo "$command_name" | xargs)
                
                # 计算比例
                local executions_ratio=$(awk "BEGIN {printf \"%.2f\", $count * 100 / $total_count}")
                local elapsed_time_ratio=$(awk "BEGIN {printf \"%.2f\", $sql_elapsed_time * 100 / ($total_seconds > 0 ? $total_seconds : 1)}")
                
                # 获取等待次数和TOP 3事件
                local wait_info=$(get_total_wait_count "$conn_str" "$sql_id" "$ash_begin_time")
                local total_wait_count=$(echo "$wait_info" | cut -d'|' -f1)
                local top_3_event=$(echo "$wait_info" | cut -d'|' -f2)
                
                # 写入原始数据行（TSV格式：制表符分隔）
                append_raw_data_line "$data_file" "top_elapsed_time_sql_dict" "$sql_id|$count|$sql_elapsed_time|$sql_elapsed_time_per_exec|$command_name|$executions_ratio|$elapsed_time_ratio|$total_wait_count|$top_3_event"
            done < "$temp_file"
            
            end_raw_data_section "$data_file" "top_elapsed_time_sql_dict"
            
            rm -f "$temp_file"
            
            log INFO "高执行时间SQL采集完成"
        else
            rm -f "$temp_file"
            log WARNING "未能获取高执行时间SQL数据"
        fi
    else
        log WARNING "未能获取高执行时间SQL数据"
    fi
}

# 采集数据库快速恢复区信息
collect_db_recovery_area_info() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "采集数据库快速恢复区信息..."
    
    # 查询快速恢复区参数
    local sql="select name, value from v\$parameter where name in ('db_recovery_file_dest','db_recovery_file_dest_size')"
    local result=$(execute_sql "$sql" "$conn_str")
    
    local db_recovery_file_dest=""
    local db_recovery_file_dest_size=0
    local percent_space_used=0
    
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 解析参数值
        while IFS='|' read -r line; do
            if [ -z "$line" ] || echo "$line" | grep -qE "^(SELECT|FROM|WHERE|NAME|VALUE)"; then
                continue
            fi
            local name=$(echo "$line" | awk -F'|' '{print $1}' | xargs)
            local value=$(echo "$line" | awk -F'|' '{print $2}' | xargs)
            if [ "$name" = "db_recovery_file_dest" ]; then
                db_recovery_file_dest="$value"
            elif [ "$name" = "db_recovery_file_dest_size" ]; then
                db_recovery_file_dest_size="$value"
            fi
        done <<< "$result"
        
        # 如果快速恢复区已启用，更新全局变量和数据文件（与Python版本一致）
        if [ -n "$db_recovery_file_dest" ]; then
            DB_RECOVERY_AREA_ENABLED=true
            export DB_RECOVERY_AREA_ENABLED
            sed -i "s/^db_recovery_area_enabled=.*/db_recovery_area_enabled=true/" "$data_file" 2>/dev/null || echo "db_recovery_area_enabled=true" >> "$data_file"
            local sql2="select sum(percent_space_used) from v\$recovery_area_usage"
            local result2=$(execute_sql "$sql2" "$conn_str")
            if [ -n "$result2" ] && ! echo "$result2" | grep -qiE "ORA-|SP2-|ERROR"; then
                percent_space_used=$(echo "$result2" | grep -oE '[0-9]+\.?[0-9]*' | head -1)
            fi
            
            # 写入原始数据
            if [ -n "$db_recovery_file_dest" ] && [ -n "$db_recovery_file_dest_size" ]; then
                # 写入section开始标记（如果不存在）
                if [ ! -f "$data_file" ] || ! grep -q "===SECTION:db_recovery_area_data===" "$data_file"; then
                    echo "===SECTION:db_recovery_area_data===" >> "$data_file"
                    # 写入表头
                    append_raw_data_line "$data_file" "db_recovery_area_data" "db_recovery_file_dest|db_recovery_file_dest_size|percent_space_used"
                    log DEBUG "写入原始数据: section=db_recovery_area_data, 文件=$data_file"
                fi
                
                append_raw_data_line "$data_file" "db_recovery_area_data" "$db_recovery_file_dest|$db_recovery_file_dest_size|$percent_space_used"
                end_raw_data_section "$data_file" "db_recovery_area_data"
                log INFO "数据库快速恢复区信息采集完成"
            fi
        fi
    fi
}

# 采集阻塞链数据
collect_blocked_chain() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "开始采集阻塞链数据..."
    
    # 1. 从v$session查找当前阻塞关系
    _collect_current_blocking_chains "$conn_str" "$data_file"
    
    # 2. 从ash_session_history统计阻塞情况
    _collect_blocking_statistics "$conn_str" "$data_file"
    
    log INFO "阻塞链数据采集完成"
}

# 从v$session查找当前阻塞关系
_collect_current_blocking_chains() {
    local conn_str=$1
    local data_file=$2
    
    local sql="SELECT 
            s1.sid AS waiting_sid,
            s1.serial# AS waiting_serial,
            s1.username AS waiting_username,
            s1.program AS waiting_program,
            s1.machine AS waiting_machine,
            s1.status AS waiting_status,
            s1.sql_id AS waiting_sql_id,
            s1.event AS waiting_event,
            s1.seconds_in_wait AS waiting_seconds,
            s1.logon_time AS waiting_logon_time,
            s1.sql_exec_start AS waiting_sql_start,
            s1.blocking_session AS blocking_sid,
            s2.serial# AS blocking_serial,
            s2.status AS blocking_status,
            s2.username AS blocking_username,
            s2.program AS blocking_program,
            s2.machine AS blocking_machine,
            s2.sql_id AS blocking_sql_id,
            s2.event AS blocking_event,
            s2.seconds_in_wait AS blocking_seconds,
            s2.logon_time AS blocking_logon_time,
            s2.sql_exec_start AS blocking_sql_start,
            CASE 
                WHEN s1.event LIKE '%enq%' THEN '锁等待'
                WHEN s1.event LIKE '%latch%' THEN 'Latch等待'
                WHEN s1.event LIKE '%buffer%' THEN '缓冲区等待'
                ELSE '其他等待'
            END AS block_type,
            'v\$session' AS source_table
        FROM v\$session s1
        LEFT JOIN v\$session s2 ON s1.blocking_session = s2.sid
        WHERE s1.blocking_session IS NOT NULL and s1.event like 'enq%'
        AND s1.status = 'ACTIVE'
        AND s1.seconds_in_wait > 5
        ORDER BY s1.seconds_in_wait DESC, s1.sid"
    
    log DEBUG "执行阻塞链查询SQL..."
    local result=$(execute_sql "$sql" "$conn_str")
    local result_line_count=$(echo "$result" | grep -v "^$" | grep -v "WAITING_SID" | wc -l | xargs)
    log DEBUG "阻塞链查询结果行数: $result_line_count"
    
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        log INFO "找到阻塞链数据，开始处理..."
        # 构建阻塞关系字典（使用临时文件）
        local temp_dict=$(mktemp)
        
        # 第一遍：构建阻塞关系
        while IFS='|' read -r waiting_sid waiting_serial waiting_username waiting_program waiting_machine waiting_status waiting_sql_id waiting_event waiting_seconds waiting_logon_time waiting_sql_start blocking_sid blocking_serial blocking_status blocking_username blocking_program blocking_machine blocking_sql_id blocking_event blocking_seconds blocking_logon_time blocking_sql_start block_type source_table; do
            waiting_sid=$(echo "$waiting_sid" | xargs)
            waiting_serial=$(echo "$waiting_serial" | xargs)
            blocking_sid=$(echo "$blocking_sid" | xargs)
            blocking_serial=$(echo "$blocking_serial" | xargs)
            
            if [ -n "$waiting_sid" ] && [ -n "$blocking_sid" ]; then
                blocking_session="${blocking_sid}-${blocking_serial}"
                waiting_session="${waiting_sid}-${waiting_serial}"
                echo "${blocking_session}|${waiting_session}" >> "$temp_dict"
            fi
        done <<< "$result"
        
        # 第二遍：如果阻塞者本身也被阻塞，则向上查找父阻塞者，合并等待列表
        # 这个逻辑与Python版本一致：对于每个阻塞者，检查它是否也在等待（通过检查它的sid和serial是否在result中作为waiting_session）
        # 如果它在等待，找到它的父阻塞者，将子阻塞者的等待列表合并到父阻塞者
        local temp_dict2=$(mktemp)
        cp "$temp_dict" "$temp_dict2"
        
        # 获取所有唯一的阻塞者
        local unique_blockers=$(mktemp)
        cut -d'|' -f1 "$temp_dict2" | sort -u > "$unique_blockers"
        
        while IFS= read -r blocking_session; do
            blocking_sid=$(echo "$blocking_session" | cut -d'-' -f1)
            blocking_serial=$(echo "$blocking_session" | cut -d'-' -f2)
            
            # 检查该阻塞者是否也在等待（即它本身也被阻塞）
            # 通过检查result中是否有waiting_sid=blocking_sid and waiting_serial=blocking_serial的行
            local is_waiting=false
            local parent_blocking_session=""
            while IFS='|' read -r waiting_sid waiting_serial waiting_username waiting_program waiting_machine waiting_status waiting_sql_id waiting_event waiting_seconds waiting_logon_time waiting_sql_start blocking_sid2 blocking_serial2 blocking_status blocking_username blocking_program blocking_machine blocking_sql_id blocking_event blocking_seconds blocking_logon_time blocking_sql_start block_type source_table; do
                waiting_sid=$(echo "$waiting_sid" | xargs)
                waiting_serial=$(echo "$waiting_serial" | xargs)
                blocking_sid2=$(echo "$blocking_sid2" | xargs)
                blocking_serial2=$(echo "$blocking_serial2" | xargs)
                
                if [ "$waiting_sid" = "$blocking_sid" ] && [ "$waiting_serial" = "$blocking_serial" ]; then
                    is_waiting=true
                    parent_blocking_session="${blocking_sid2}-${blocking_serial2}"
                    break
                fi
            done <<< "$result"
            
            # 如果阻塞者本身也在等待，找到它的父阻塞者，将子阻塞者的等待列表合并到父阻塞者
            if [ "$is_waiting" = true ] && [ -n "$parent_blocking_session" ]; then
                # 将当前阻塞者的所有等待会话添加到父阻塞者
                while IFS='|' read -r b_sess w_sess; do
                    if [ "$b_sess" = "$blocking_session" ]; then
                        echo "${parent_blocking_session}|${w_sess}" >> "$temp_dict2"
                    fi
                done < "$temp_dict"
                # 从字典中移除当前阻塞者（因为它已经被合并到父阻塞者）
                grep -v "^${blocking_session}|" "$temp_dict2" > "${temp_dict2}.tmp" && mv "${temp_dict2}.tmp" "$temp_dict2"
            fi
        done < "$unique_blockers"
        
        rm -f "$unique_blockers"
        mv "$temp_dict2" "$temp_dict"
        
        # 第三遍：写入原始数据，包含根阻塞者信息
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:blocked_chain_data===" "$data_file"; then
            echo "===SECTION:blocked_chain_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "blocked_chain_data" "WAITING_SID|WAITING_SERIAL#|WAITING_USERNAME|WAITING_PROGRAM|WAITING_MACHINE|WAITING_STATUS|WAITING_SQL_ID|WAITING_EVENT|WAITING_SECONDS|WAITING_LOGON_TIME|WAITING_SQL_START|BLOCKING_SID|BLOCKING_SERIAL#|BLOCKING_STATUS|BLOCKING_USERNAME|BLOCKING_PROGRAM|BLOCKING_MACHINE|BLOCKING_SQL_ID|BLOCKING_EVENT|BLOCKING_SECONDS|BLOCKING_LOGON_TIME|BLOCKING_SQL_START|BLOCK_TYPE|ROOT_BLOCKER_SID|ROOT_BLOCKER_SERIAL#"
            log DEBUG "写入原始数据: section=blocked_chain_data, 文件=$data_file"
        fi
        
        while IFS='|' read -r waiting_sid waiting_serial waiting_username waiting_program waiting_machine waiting_status waiting_sql_id waiting_event waiting_seconds waiting_logon_time waiting_sql_start blocking_sid blocking_serial blocking_status blocking_username blocking_program blocking_machine blocking_sql_id blocking_event blocking_seconds blocking_logon_time blocking_sql_start block_type source_table; do

            waiting_sid=$(echo "$waiting_sid" | xargs)
            waiting_serial=$(echo "$waiting_serial" | xargs)
            blocking_sid=$(echo "$blocking_sid" | xargs)
            blocking_serial=$(echo "$blocking_serial" | xargs)
            
            if [ -n "$waiting_sid" ] && [ -n "$blocking_sid" ]; then
                blocking_session="${blocking_sid}-${blocking_serial}"
                waiting_session="${waiting_sid}-${waiting_serial}"
                
                # 查找根阻塞者：在阻塞关系字典中找到包含该等待会话的根阻塞者
                local root_blocker_sid=""
                local root_blocker_serial=""
                
                # 遍历阻塞关系字典，找到包含该等待会话的根阻塞者
                while IFS='|' read -r b_sess w_sess; do
                    if [ "$w_sess" = "$waiting_session" ]; then
                        # 检查这个阻塞者是否也被阻塞
                        local is_blocker_waiting=false
                        while IFS='|' read -r b2 w2; do
                            if [ "$w2" = "$b_sess" ]; then
                                is_blocker_waiting=true
                                break
                            fi
                        done < "$temp_dict"
                        
                        # 如果阻塞者没有被阻塞，它就是根阻塞者
                        if [ "$is_blocker_waiting" = false ]; then
                            root_blocker_sid=$(echo "$b_sess" | cut -d'-' -f1)
                            root_blocker_serial=$(echo "$b_sess" | cut -d'-' -f2)
                            break
                        fi
                    fi
                done < "$temp_dict"
                
                # 如果没找到，使用当前阻塞者
                if [ -z "$root_blocker_sid" ]; then
                    root_blocker_sid="$blocking_sid"
                    root_blocker_serial="$blocking_serial"
                fi
                
                # 确保数字字段有效（如果为空或非数字，设为0）
                [ -z "$waiting_sid" ] && waiting_sid="0"
                [ -z "$waiting_serial" ] && waiting_serial="0"
                [ -z "$blocking_sid" ] && blocking_sid="0"
                [ -z "$blocking_serial" ] && blocking_serial="0"
                [ -z "$root_blocker_sid" ] && root_blocker_sid="0"
                [ -z "$root_blocker_serial" ] && root_blocker_serial="0"
                [ -z "$waiting_seconds" ] && waiting_seconds="0"
                [ -z "$blocking_seconds" ] && blocking_seconds="0"
                
                # 验证数字字段是否为有效数字，如果不是则设为0
                case "$waiting_sid" in
                    ''|*[!0-9]*) waiting_sid="0" ;;
                esac
                case "$waiting_serial" in
                    ''|*[!0-9]*) waiting_serial="0" ;;
                esac
                case "$blocking_sid" in
                    ''|*[!0-9]*) blocking_sid="0" ;;
                esac
                case "$blocking_serial" in
                    ''|*[!0-9]*) blocking_serial="0" ;;
                esac
                case "$root_blocker_sid" in
                    ''|*[!0-9]*) root_blocker_sid="0" ;;
                esac
                case "$root_blocker_serial" in
                    ''|*[!0-9]*) root_blocker_serial="0" ;;
                esac
                case "$waiting_seconds" in
                    ''|*[!0-9]*) waiting_seconds="0" ;;
                esac
                case "$blocking_seconds" in
                    ''|*[!0-9]*) blocking_seconds="0" ;;
                esac
                
                # 去除所有字符串变量的前导和尾随空格，并将只包含空格的字符串设为空字符串
                waiting_username=$(echo "$waiting_username" | xargs)
                waiting_program=$(echo "$waiting_program" | xargs)
                waiting_machine=$(echo "$waiting_machine" | xargs)
                waiting_status=$(echo "$waiting_status" | xargs)
                waiting_sql_id=$(echo "$waiting_sql_id" | xargs)
                waiting_event=$(echo "$waiting_event" | xargs)
                waiting_logon_time=$(echo "$waiting_logon_time" | xargs)
                waiting_sql_start=$(echo "$waiting_sql_start" | xargs)
                blocking_username=$(echo "$blocking_username" | xargs)
                blocking_program=$(echo "$blocking_program" | xargs)
                blocking_machine=$(echo "$blocking_machine" | xargs)
                blocking_status=$(echo "$blocking_status" | xargs)
                blocking_sql_id=$(echo "$blocking_sql_id" | xargs)
                blocking_event=$(echo "$blocking_event" | xargs)
                blocking_logon_time=$(echo "$blocking_logon_time" | xargs)
                blocking_sql_start=$(echo "$blocking_sql_start" | xargs)
                block_type=$(echo "$block_type" | xargs)
                source_table=$(echo "$source_table" | xargs)
                
                # 处理NULL值和空字符串（确保空值统一为空字符串）
                [ -z "$waiting_username" ] && waiting_username=""
                [ -z "$waiting_program" ] && waiting_program=""
                [ -z "$waiting_machine" ] && waiting_machine=""
                [ -z "$waiting_status" ] && waiting_status=""
                [ -z "$waiting_sql_id" ] && waiting_sql_id=""
                [ -z "$waiting_event" ] && waiting_event=""
                [ -z "$waiting_logon_time" ] && waiting_logon_time=""
                [ -z "$waiting_sql_start" ] && waiting_sql_start=""
                [ -z "$blocking_username" ] && blocking_username=""
                [ -z "$blocking_program" ] && blocking_program=""
                [ -z "$blocking_machine" ] && blocking_machine=""
                [ -z "$blocking_status" ] && blocking_status=""
                [ -z "$blocking_sql_id" ] && blocking_sql_id=""
                [ -z "$blocking_event" ] && blocking_event=""
                [ -z "$blocking_logon_time" ] && blocking_logon_time=""
                [ -z "$blocking_sql_start" ] && blocking_sql_start=""
                [ -z "$block_type" ] && block_type=""
                [ -z "$source_table" ] && source_table=""
                
                # 写入原始数据行（TSV格式：制表符分隔）
                append_raw_data_line "$data_file" "blocked_chain_data" "$waiting_sid|$waiting_serial|$waiting_username|$waiting_program|$waiting_machine|$waiting_status|$waiting_sql_id|$waiting_event|$waiting_seconds|$waiting_logon_time|$waiting_sql_start|$blocking_sid|$blocking_serial|$blocking_status|$blocking_username|$blocking_program|$blocking_machine|$blocking_sql_id|$blocking_event|$blocking_seconds|$blocking_logon_time|$blocking_sql_start|$block_type|$root_blocker_sid|$root_blocker_serial"
            fi
        done <<< "$result"
        
        local data_count=$(grep -c "^blocked_chain_data" "$data_file" 2>/dev/null || echo "0")
        log INFO "已写入 $data_count 条阻塞链数据到文件"
        
        end_raw_data_section "$data_file" "blocked_chain_data"
        
        # 清理临时文件
        rm -f "$temp_dict"
        log INFO "阻塞链数据写入完成"
    else
        # 即使结果为空，也要输出section标记和表头
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:blocked_chain_data===" "$data_file"; then
            echo "===SECTION:blocked_chain_data===" >> "$data_file"
        fi
        # 输出结束标记
        end_raw_data_section "$data_file" "blocked_chain_data"
        if echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
            log WARN "阻塞链查询返回错误: $(echo "$result" | head -3 | tr '\n' ' ')"
        else
            log INFO "当前没有满足条件的阻塞链数据（需要：enq事件、ACTIVE状态、等待时间>5秒）"
        fi
    fi
}

# 从ash_session_history统计阻塞情况
_collect_blocking_statistics() {
    local conn_str=$1
    local data_file=$2
    
    # 确保使用全局变量ASH_BEGIN_TIME
    if [ -z "$ASH_BEGIN_TIME" ]; then
        log WARNING "ASH_BEGIN_TIME未设置，无法统计阻塞情况"
        return
    fi
    
    # 确保使用全局变量ASH_BEGIN_TIME
    if [ -z "$ASH_BEGIN_TIME" ]; then
        log WARNING "ASH_BEGIN_TIME未设置，无法统计阻塞情况"
        return
    fi
    
    local sql="SELECT 
    ROUND(AVG(cnt), 2) as avg_cnt,
    MAX(cnt) as max_cnt,
    ROUND(PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY cnt), 2) as percentile_95,
    MAX(sample_id) KEEP (DENSE_RANK LAST ORDER BY cnt) as sample_id,
    MAX(sample_time) KEEP (DENSE_RANK LAST ORDER BY cnt) as sample_time
FROM (
    SELECT 
        sample_time,sample_id,
        COUNT(*) as cnt
    FROM v\$active_session_history 
    WHERE session_state='WAITING' and event like 'enq%'
        AND sample_time > TO_DATE('$ASH_BEGIN_TIME', 'YYYY-MM-DD HH24:MI:SS')
    GROUP BY sample_time,sample_id)"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        local avg_cnt=0
        local max_cnt=0
        local percentile_95=0
        local max_cnt_sample_id=0
        local max_cnt_sample_time=""
        
        while IFS='|' read -r avg max_val p95 sample_id sample_time; do
            avg_cnt=$(normalize_number "$avg")
            max_cnt=$(normalize_number "$max_val")
            percentile_95=$(normalize_number "$p95")
            max_cnt_sample_id=$(normalize_number "$sample_id")
            max_cnt_sample_time=$(echo "$sample_time" | xargs)
            
            [ -z "$avg_cnt" ] && avg_cnt="0"
            [ -z "$max_cnt" ] && max_cnt="0"
            [ -z "$percentile_95" ] && percentile_95="0"
            [ -z "$max_cnt_sample_id" ] && max_cnt_sample_id="0"
        done <<< "$result"
        
        # 添加到metric_data（存储到内存，格式：metric_id|avg_value|max_value|min_value|percentile_95）
        # 注意：min_value 字段缺失，使用空值
        append_metric_data_to_memory "2180518|$avg_cnt|$max_cnt||$percentile_95"
        
        # 如果找到最大阻塞时间点，分析该时间点的会话阻塞关系
        if [ -n "$max_cnt_sample_id" ] && [ "$max_cnt_sample_id" -gt 0 ]; then
            local blocking_sql="SELECT 
                    s1.session_id AS waiting_session_id,
                    s1.session_serial# AS waiting_session_serial,
                    s1.user_id AS waiting_user_id,
                    s1.program AS waiting_program,
                    s1.machine AS waiting_machine,
                    s1.event AS waiting_event,
                    s1.sql_id as waiting_sql_id,
                    to_char(s1.sql_exec_start, 'YYYY-MM-DD HH24:MI:SS') as waiting_sql_exec_start,
                    s1.blocking_session AS blocking_session_id,
                    s1.blocking_session_serial# AS blocking_session_serial,
                    s2.user_id AS blocking_user_id,
                    s2.program AS blocking_program,
                    s2.machine AS blocking_machine,
                    s2.event AS blocking_event,
                    s2.sql_id as blocking_sql_id,
                    to_char(s2.sql_exec_start, 'YYYY-MM-DD HH24:MI:SS') as blocking_sql_exec_start
                FROM v\$active_session_history s1
                LEFT JOIN v\$active_session_history s2 
                    ON s1.blocking_session = s2.session_id 
                    AND s1.blocking_session_serial# = s2.session_serial# 
                    AND s1.sample_id = s2.sample_id
                WHERE s1.sample_id = $max_cnt_sample_id
                    AND s1.blocking_session IS NOT NULL
                ORDER BY s1.session_id"
            log DEBUG "blocking_sql: $blocking_sql"
            local blocking_result=$(execute_sql "$blocking_sql" "$conn_str")
            log DEBUG "blocking_result: $blocking_result"
            if [ -n "$blocking_result" ] && ! echo "$blocking_result" | grep -qiE "ORA-|SP2-|ERROR"; then
                log INFO "=== 最大阻塞时间点 ($max_cnt_sample_time) 的会话阻塞关系 ==="
                
                # 构建阻塞关系字典并分析根阻塞者（类似当前阻塞链的逻辑）
                local temp_dict=$(mktemp)
                
                while IFS='|' read -r waiting_session_id waiting_session_serial waiting_user_id waiting_program waiting_machine waiting_event waiting_sql_id waiting_sql_exec_start blocking_session_id blocking_session_serial blocking_user_id blocking_program blocking_machine blocking_event blocking_sql_id blocking_sql_exec_start; do
                    waiting_session_id=$(echo "$waiting_session_id" | xargs)
                    waiting_session_serial=$(echo "$waiting_session_serial" | xargs)
                    blocking_session_id=$(echo "$blocking_session_id" | xargs)
                    blocking_session_serial=$(echo "$blocking_session_serial" | xargs)
                    
                    if [ -n "$waiting_session_id" ] && [ -n "$blocking_session_id" ]; then
                        blocking_session="${blocking_session_id}-${blocking_session_serial}"
                        waiting_session="${waiting_session_id}-${waiting_session_serial}"
                        echo "${blocking_session}|${waiting_session}" >> "$temp_dict"
                    fi
                done <<< "$blocking_result"
                
                # 第二遍：如果阻塞者本身也被阻塞，则向上查找父阻塞者，合并等待列表
                # 这个逻辑与Python版本一致：对于每个阻塞者，检查它是否也在等待
                local temp_dict2=$(mktemp)
                cp "$temp_dict" "$temp_dict2"
                
                # 获取所有唯一的阻塞者
                local unique_blockers=$(mktemp)
                cut -d'|' -f1 "$temp_dict2" | sort -u > "$unique_blockers"
                
                while IFS= read -r blocking_session; do
                    blocking_sid=$(echo "$blocking_session" | cut -d'-' -f1)
                    blocking_serial=$(echo "$blocking_session" | cut -d'-' -f2)
                    
                    # 检查该阻塞者是否也在等待（即它本身也被阻塞）
                    # 通过检查blocking_result中是否有waiting_session_id=blocking_sid and waiting_session_serial=blocking_serial的行
                    local is_waiting=false
                    local parent_blocking_session=""
                    while IFS='|' read -r waiting_session_id waiting_session_serial waiting_user_id waiting_program waiting_machine waiting_event waiting_sql_id waiting_sql_exec_start blocking_session_id blocking_session_serial blocking_user_id blocking_program blocking_machine blocking_event blocking_sql_id blocking_sql_exec_start; do
                        waiting_session_id=$(echo "$waiting_session_id" | xargs)
                        waiting_session_serial=$(echo "$waiting_session_serial" | xargs)
                        blocking_session_id=$(echo "$blocking_session_id" | xargs)
                        blocking_session_serial=$(echo "$blocking_session_serial" | xargs)
                        
                        if [ "$waiting_session_id" = "$blocking_sid" ] && [ "$waiting_session_serial" = "$blocking_serial" ]; then
                            is_waiting=true
                            parent_blocking_session="${blocking_session_id}-${blocking_session_serial}"
                            break
                        fi
                    done <<< "$blocking_result"
                    
                    # 如果阻塞者本身也在等待，找到它的父阻塞者，将子阻塞者的等待列表合并到父阻塞者
                    if [ "$is_waiting" = true ] && [ -n "$parent_blocking_session" ]; then
                        # 将当前阻塞者的所有等待会话添加到父阻塞者
                        while IFS='|' read -r b_sess w_sess; do
                            if [ "$b_sess" = "$blocking_session" ]; then
                                echo "${parent_blocking_session}|${w_sess}" >> "$temp_dict2"
                            fi
                        done < "$temp_dict"
                        # 从字典中移除当前阻塞者（因为它已经被合并到父阻塞者）
                        grep -v "^${blocking_session}|" "$temp_dict2" > "${temp_dict2}.tmp" && mv "${temp_dict2}.tmp" "$temp_dict2"
                    fi
                done < "$unique_blockers"
                
                rm -f "$unique_blockers"
                mv "$temp_dict2" "$temp_dict"
                
                # 写入原始数据
                # 写入section开始标记（如果不存在）
                if [ ! -f "$data_file" ] || ! grep -q "===SECTION:blocked_chain_his_data===" "$data_file"; then
                    echo "===SECTION:blocked_chain_his_data===" >> "$data_file"
                    # 写入表头
                    append_raw_data_line "$data_file" "blocked_chain_his_data" "WAITING_SID|WAITING_SERIAL#|WAITING_USERNAME|WAITING_PROGRAM|WAITING_MACHINE|WAITING_EVENT|WAITING_SQL_ID|WAITING_SQL_EXEC_START|BLOCKING_SID|BLOCKING_SERIAL#|BLOCKING_USERNAME|BLOCKING_PROGRAM|BLOCKING_MACHINE|BLOCKING_EVENT|BLOCKING_SQL_ID|BLOCKING_SQL_EXEC_START|ROOT_BLOCKER_SID|ROOT_BLOCKER_SERIAL#"
                    log DEBUG "写入原始数据: section=blocked_chain_his_data, 文件=$data_file"
                fi
                
                while IFS='|' read -r waiting_session_id waiting_session_serial waiting_user_id waiting_program waiting_machine waiting_event waiting_sql_id waiting_sql_exec_start blocking_session_id blocking_session_serial blocking_user_id blocking_program blocking_machine blocking_event blocking_sql_id blocking_sql_exec_start; do
                    waiting_session_id=$(echo "$waiting_session_id" | xargs)
                    waiting_session_serial=$(echo "$waiting_session_serial" | xargs)
                    waiting_user_id=$(echo "$waiting_user_id" | xargs)
                    blocking_session_id=$(echo "$blocking_session_id" | xargs)
                    blocking_session_serial=$(echo "$blocking_session_serial" | xargs)
                    blocking_user_id=$(echo "$blocking_user_id" | xargs)
                    
                    if [ -n "$waiting_session_id" ] && [ -n "$blocking_session_id" ]; then
                        blocking_session="${blocking_session_id}-${blocking_session_serial}"
                        waiting_session="${waiting_session_id}-${waiting_session_serial}"
                        
                        # 查找根阻塞者：在阻塞关系字典中找到包含该等待会话的根阻塞者
                        local root_blocker_sid=""
                        local root_blocker_serial=""
                        
                        # 遍历阻塞关系字典，找到包含该等待会话的根阻塞者
                        while IFS='|' read -r b_sess w_sess; do
                            if [ "$w_sess" = "$waiting_session" ]; then
                                # 检查这个阻塞者是否也被阻塞
                                local is_blocker_waiting=false
                                while IFS='|' read -r b2 w2; do
                                    if [ "$w2" = "$b_sess" ]; then
                                        is_blocker_waiting=true
                                        break
                                    fi
                                done < "$temp_dict"
                                
                                # 如果阻塞者没有被阻塞，它就是根阻塞者
                                if [ "$is_blocker_waiting" = false ]; then
                                    root_blocker_sid=$(echo "$b_sess" | cut -d'-' -f1)
                                    root_blocker_serial=$(echo "$b_sess" | cut -d'-' -f2)
                                    break
                                fi
                            fi
                        done < "$temp_dict"
                        
                        # 如果没找到，使用当前阻塞者
                        if [ -z "$root_blocker_sid" ]; then
                            root_blocker_sid="$blocking_session_id"
                            root_blocker_serial="$blocking_session_serial"
                        fi
                        
                        # 从内存中的用户名字典获取用户名
                        local waiting_username=$(get_username_by_id "$waiting_user_id")
                        local blocking_username=$(get_username_by_id "$blocking_user_id")
                        # 确保数字字段有效（如果为空或非数字，设为0）
                        [ -z "$waiting_session_id" ] && waiting_session_id="0"
                        [ -z "$waiting_session_serial" ] && waiting_session_serial="0"
                        [ -z "$blocking_session_id" ] && blocking_session_id="0"
                        [ -z "$blocking_session_serial" ] && blocking_session_serial="0"
                        [ -z "$root_blocker_sid" ] && root_blocker_sid="0"
                        [ -z "$root_blocker_serial" ] && root_blocker_serial="0"
                        
                        # 验证数字字段是否为有效数字，如果不是则设为0
                        case "$waiting_session_id" in
                            ''|*[!0-9]*) waiting_session_id="0" ;;
                        esac
                        case "$waiting_session_serial" in
                            ''|*[!0-9]*) waiting_session_serial="0" ;;
                        esac
                        case "$blocking_session_id" in
                            ''|*[!0-9]*) blocking_session_id="0" ;;
                        esac
                        case "$blocking_session_serial" in
                            ''|*[!0-9]*) blocking_session_serial="0" ;;
                        esac
                        case "$root_blocker_sid" in
                            ''|*[!0-9]*) root_blocker_sid="0" ;;
                        esac
                        case "$root_blocker_serial" in
                            ''|*[!0-9]*) root_blocker_serial="0" ;;
                        esac
                        
                        # 去除所有字符串变量的前导和尾随空格，并将只包含空格的字符串设为空字符串
                        waiting_username=$(echo "$waiting_username" | xargs)
                        waiting_program=$(echo "$waiting_program" | xargs)
                        waiting_machine=$(echo "$waiting_machine" | xargs)
                        waiting_event=$(echo "$waiting_event" | xargs)
                        waiting_sql_id=$(echo "$waiting_sql_id" | xargs)
                        waiting_sql_exec_start=$(echo "$waiting_sql_exec_start" | xargs)
                        blocking_username=$(echo "$blocking_username" | xargs)
                        blocking_program=$(echo "$blocking_program" | xargs)
                        blocking_machine=$(echo "$blocking_machine" | xargs)
                        blocking_event=$(echo "$blocking_event" | xargs)
                        blocking_sql_id=$(echo "$blocking_sql_id" | xargs)
                        blocking_sql_exec_start=$(echo "$blocking_sql_exec_start" | xargs)
                        
                        # 处理NULL值和空字符串（确保空值统一为空字符串）
                        [ -z "$waiting_username" ] && waiting_username=""
                        [ -z "$waiting_program" ] && waiting_program=""
                        [ -z "$waiting_machine" ] && waiting_machine=""
                        [ -z "$waiting_event" ] && waiting_event=""
                        [ -z "$waiting_sql_id" ] && waiting_sql_id=""
                        [ -z "$waiting_sql_exec_start" ] && waiting_sql_exec_start=""
                        [ -z "$blocking_username" ] && blocking_username=""
                        [ -z "$blocking_program" ] && blocking_program=""
                        [ -z "$blocking_machine" ] && blocking_machine=""
                        [ -z "$blocking_event" ] && blocking_event=""
                        [ -z "$blocking_sql_id" ] && blocking_sql_id=""
                        [ -z "$blocking_sql_exec_start" ] && blocking_sql_exec_start=""
                        
                        # 写入原始数据行（TSV格式：制表符分隔）
                        append_raw_data_line "$data_file" "blocked_chain_his_data" "$waiting_session_id|$waiting_session_serial|$waiting_username|$waiting_program|$waiting_machine|$waiting_event|$waiting_sql_id|$waiting_sql_exec_start|$blocking_session_id|$blocking_session_serial|$blocking_username|$blocking_program|$blocking_machine|$blocking_event|$blocking_sql_id|$blocking_sql_exec_start|$root_blocker_sid|$root_blocker_serial"
                    fi
                done <<< "$blocking_result"
                
                end_raw_data_section "$data_file" "blocked_chain_his_data"
                
                # 清理临时文件
                rm -f "$temp_dict"
            fi
        fi
    fi
}

# 采集IO类型数据
collect_io_type_data() {
    local conn_str=$1
    local data_file=$2
    local snap_id=$3
    local instance_number=$4
    local is_reboot=$5
    local ash_begin_time=$6
    
    log INFO "采集IO类型数据..."
    
    local sql=""
    if [ "$is_reboot" != "true" ] && [ -n "$snap_id" ] && [ -n "$instance_number" ]; then
        sql="SELECT t2.function_name, t2.data_read-t1.data_read, t2.read_reqs-t1.read_reqs,
       t2.data_write-t1.data_write, t2.write_req-t1.write_req,
       t2.number_of_waits-t1.number_of_waits, (t2.wait_time-t1.wait_time)*10
FROM (
SELECT function_name, (small_read_megabytes+large_read_megabytes) data_read,
       (small_read_reqs + large_read_reqs) read_reqs,
       (small_write_megabytes+ large_write_megabytes) data_write,
       (small_write_reqs + large_write_reqs) write_req,
       number_of_waits, wait_time
FROM dba_hist_iostat_function
WHERE snap_id=$snap_id AND instance_number=$instance_number) t1,
(SELECT function_name, (small_read_megabytes+large_read_megabytes) data_read,
       (small_read_reqs + large_read_reqs) read_reqs,
       (small_write_megabytes+ large_write_megabytes) data_write,
       (small_write_reqs + large_write_reqs) write_req,
       number_of_waits, wait_time
FROM v\$iostat_function) t2
WHERE t1.function_name=t2.function_name
ORDER BY 2 DESC"
    else
        sql="SELECT function_name, (small_read_megabytes+large_read_megabytes) data_read,
       (small_read_reqs + large_read_reqs) read_reqs,
       (small_write_megabytes+ large_write_megabytes) data_write,
       (small_write_reqs + large_write_reqs) write_req,
       number_of_waits, wait_time*10
FROM v\$iostat_function
ORDER BY 2 DESC"
    fi
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:io_type_data===" "$data_file"; then
            echo "===SECTION:io_type_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "io_type_data" "FUNCTION_NAME|DATA_READ|DATA_READ_PER_SEC|READ_REQS_PER_SEC|DATA_WRITE|DATA_WRITE_PER_SEC|WRITE_REQ_PER_SEC|NUMBER_OF_WAITS|WAIT_TIME_AVG"
            log DEBUG "写入原始数据: section=io_type_data, 文件=$data_file"
        fi
        
        # 计算delta_time（与Python版本一致）
        local current_time=$(date +%s)
        local sample_time=$(date -d "$ash_begin_time" +%s 2>/dev/null || echo "$current_time")
        local delta_time=$((current_time - sample_time))
        [ "$delta_time" -le 0 ] && delta_time=1
        
        while IFS='|' read -r function_name data_read read_reqs data_write write_req number_of_waits wait_time; do
            function_name=$(echo "$function_name" | xargs)
            data_read=$(echo "$data_read" | xargs)
            read_reqs=$(echo "$read_reqs" | xargs)
            data_write=$(echo "$data_write" | xargs)
            write_req=$(echo "$write_req" | xargs)
            number_of_waits=$(echo "$number_of_waits" | xargs)
            wait_time=$(echo "$wait_time" | xargs)
            
            if [ -n "$function_name" ]; then
                # 处理NULL值
                [ -z "$data_read" ] && data_read="0"
                [ -z "$read_reqs" ] && read_reqs="0"
                [ -z "$data_write" ] && data_write="0"
                [ -z "$write_req" ] && write_req="0"
                [ -z "$number_of_waits" ] && number_of_waits="0"
                [ -z "$wait_time" ] && wait_time="0"
                
                # 计算每秒值（与Python版本一致）
                local data_read_per_sec=$(normalize_number "$(awk "BEGIN {printf \"%.2f\", $data_read / $delta_time}")")
                local read_reqs_per_sec=$(normalize_number "$(awk "BEGIN {printf \"%.2f\", $read_reqs / $delta_time}")")
                local data_write_per_sec=$(normalize_number "$(awk "BEGIN {printf \"%.2f\", $data_write / $delta_time}")")
                local write_req_per_sec=$(normalize_number "$(awk "BEGIN {printf \"%.2f\", $write_req / $delta_time}")")
                
                # 计算平均等待时间
                local wait_time_avg="0"
                if [ "$number_of_waits" != "0" ] && [ -n "$number_of_waits" ]; then
                    wait_time_avg=$(normalize_number "$(awk "BEGIN {printf \"%.2f\", $wait_time / $number_of_waits}")")
                fi
                
                # 写入原始数据行（TSV格式：制表符分隔）
                append_raw_data_line "$data_file" "io_type_data" "$function_name|$data_read|$data_read_per_sec|$read_reqs_per_sec|$data_write|$data_write_per_sec|$write_req_per_sec|$number_of_waits|$wait_time_avg"
            fi
        done <<< "$result"
        
        end_raw_data_section "$data_file" "io_type_data"
        log INFO "IO类型数据采集完成"
    fi
}

# 采集表扫描数据
collect_table_scan_data() {
    local conn_str=$1
    local data_file=$2
    local snap_id=$3
    local instance_number=$4
    local is_reboot=$5
    
    log INFO "采集表扫描数据..."
    
    local sql=""
    if [ "$is_reboot" != "true" ] && [ -n "$snap_id" ] && [ -n "$instance_number" ]; then
        sql="select stat_name,t1.value - t2.value from v\$sysstat t1, dba_hist_sysstat t2 where t2.snap_id=$snap_id and t2.instance_number=$instance_number and stat_name in ('table scans (direct read)','table scan rows gotten','table scan blocks gotten','table scans (long tables)','table scans (short tables)','table fetch continued row')
                and t1.name=t2.stat_name order by 2 desc"
    else
        sql="select name,value from v\$sysstat where name in ('table scans (direct read)','table scan rows gotten','table scan blocks gotten','table scans (long tables)','table scans (short tables)','table fetch continued row')"
    fi
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:table_scan_data===" "$data_file"; then
            echo "===SECTION:table_scan_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "table_scan_data" "NAME|VALUE|VALUE_PER_SEC"
            log DEBUG "写入原始数据: section=table_scan_data, 文件=$data_file"
        fi
        
        # 计算delta_time（与Python版本一致）
        local current_time=$(date +%s)
        # 从文件中读取ash_begin_time（键值对格式：ash_begin_time=...）
        local ash_begin_time=$(grep "^ash_begin_time=" "$data_file" 2>/dev/null | cut -d'=' -f2- | head -1 || echo "")
        local sample_time=$(date -d "$ash_begin_time" +%s 2>/dev/null || echo "$current_time")
        local delta_time=$((current_time - sample_time))
        [ "$delta_time" -le 0 ] && delta_time=1
        
        while IFS='|' read -r name value; do
            name=$(echo "$name" | xargs)
            value=$(echo "$value" | xargs)
            
            if [ -n "$name" ]; then
                # 处理NULL值
                [ -z "$value" ] && value="0"
                
                # 计算每秒值（与Python版本一致）
                local value_per_sec=$(normalize_number "$(awk "BEGIN {printf \"%.2f\", $value / $delta_time}")")
                
                # 写入原始数据行（TSV格式：制表符分隔）
                append_raw_data_line "$data_file" "table_scan_data" "$name|$value|$value_per_sec"
            fi
        done <<< "$result"
        
        end_raw_data_section "$data_file" "table_scan_data"
        log INFO "表扫描数据采集完成"
    fi
}

# 采集REDO日志信息
collect_redo_log_info() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "采集REDO日志信息..."
    
    local sql="select group#,thread#,members,blocksize,bytes/1024/1024 size_mb,status from v\$log"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:redo_log_info===" "$data_file"; then
            echo "===SECTION:redo_log_info===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "redo_log_info" "group_number|thread_number|members|blocksize|size_mb|status"
            log DEBUG "写入原始数据: section=redo_log_info, 文件=$data_file"
        fi
        
        while IFS='|' read -r group_number thread_number members blocksize size_mb status; do
            group_number=$(echo "$group_number" | xargs)
            thread_number=$(echo "$thread_number" | xargs)
            members=$(echo "$members" | xargs)
            blocksize=$(echo "$blocksize" | xargs)
            size_mb=$(echo "$size_mb" | xargs)
            status=$(echo "$status" | xargs)
            
            if [ -n "$group_number" ]; then
                # 处理NULL值
                [ -z "$thread_number" ] && thread_number="0"
                [ -z "$members" ] && members="0"
                [ -z "$blocksize" ] && blocksize="0"
                [ -z "$size_mb" ] && size_mb="0"
                
                # 写入原始数据行（TSV格式：制表符分隔）
                append_raw_data_line "$data_file" "redo_log_info" "$group_number|$thread_number|$members|$blocksize|$size_mb|$status"
            fi
        done <<< "$result"
        
        end_raw_data_section "$data_file" "redo_log_info"
        log INFO "REDO日志信息采集完成"
    fi
}

# 采集过期用户信息
collect_expired_user_info() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "采集过期用户信息..."
    
    local sql="select username,account_status,to_char(lock_date,'yyyy-mm-dd hh24:mi:ss') lock_date, to_char(expiry_date,'yyyy-mm-dd hh24:mi:ss') expiry_date from dba_users where expiry_date < sysdate+15 and username not in ('XS\$NULL','ANONYMOUS','CTXSYS')"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:expired_user_info===" "$data_file"; then
            echo "===SECTION:expired_user_info===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "expired_user_info" "USERNAME|ACCOUNT_STATUS|LOCK_DATE|EXPIRY_DATE"
            log DEBUG "写入原始数据: section=expired_user_info, 文件=$data_file"
        fi
        
        while IFS='|' read -r username account_status lock_date expiry_date; do
            username=$(echo "$username" | xargs)
            account_status=$(echo "$account_status" | xargs)
            lock_date=$(echo "$lock_date" | xargs)
            expiry_date=$(echo "$expiry_date" | xargs)
            
            if [ -n "$username" ]; then
                # 处理NULL值
                [ -z "$lock_date" ] && lock_date=""
                [ -z "$expiry_date" ] && expiry_date=""
                
                # 写入原始数据行（TSV格式：制表符分隔）
                append_raw_data_line "$data_file" "expired_user_info" "$username|$account_status|$lock_date|$expiry_date"
            fi
        done <<< "$result"
        
        end_raw_data_section "$data_file" "expired_user_info"
        log INFO "过期用户信息采集完成"
    fi
}

# 采集普通用户拥有DBA权限
collect_user_with_dba_privilege() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "采集普通用户拥有DBA权限..."
    
    local sql="select GRANTEE from dba_role_privs where granted_role='DBA' AND GRANTEE NOT IN ('SYS','SYSTEM')"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:user_with_dba_privilege_info===" "$data_file"; then
            echo "===SECTION:user_with_dba_privilege_info===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "user_with_dba_privilege_info" "USERNAME"
            log DEBUG "写入原始数据: section=user_with_dba_privilege_info, 文件=$data_file"
        fi
        
        while IFS='|' read -r grantee; do
            grantee=$(echo "$grantee" | xargs)
            if [ -n "$grantee" ] && ! echo "$grantee" | grep -qE "^(GRANTEE|SELECT|FROM|WHERE)"; then
                # 写入原始数据行（TSV格式：制表符分隔）
                append_raw_data_line "$data_file" "user_with_dba_privilege_info" "$grantee"
            fi
        done <<< "$result"
        
        end_raw_data_section "$data_file" "user_with_dba_privilege_info"
        log INFO "普通用户拥有DBA权限采集完成"
    fi
}

# 采集失效对象信息
collect_invalid_object_info() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "采集失效对象信息..."
    
    # 第一步：查询每个对象类型的统计信息
    local sql1="select object_type,count(*) from dba_objects where status != 'VALID' group by object_Type order by 2 desc"
    local result1=$(execute_sql "$sql1" "$conn_str")
    
    # 写入section开始标记（如果不存在）
    if [ ! -f "$data_file" ] || ! grep -q "===SECTION:invalid_object_info===" "$data_file"; then
        echo "===SECTION:invalid_object_info===" >> "$data_file"
        # 写入表头
        append_raw_data_line "$data_file" "invalid_object_info" "OBJECT_TYPE|COUNT"
        log DEBUG "写入原始数据: section=invalid_object_info, 文件=$data_file"
    fi
    
    if [ -n "$result1" ] && ! echo "$result1" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 第一阶段：先写入所有对象类型的统计信息（只有对象类型和数量）
        while IFS='|' read -r object_type count; do
            object_type=$(echo "$object_type" | xargs)
            count=$(echo "$count" | xargs)
            
            if [ -n "$object_type" ] && [ -n "$count" ] && ! echo "$object_type" | grep -qE "^(OBJECT_TYPE|SELECT|FROM|WHERE|GROUP|ORDER)"; then
                # 处理NULL值
                [ -z "$count" ] && count="0"
                
                # 写入统计信息（只有对象类型和数量）
                append_raw_data_line "$data_file" "invalid_object_info" "$object_type|$count"
            fi
        done <<< "$result1"
        
        # 第二阶段：写入每个对象类型的详细信息
        local detail_header_written=0
        while IFS='|' read -r object_type count; do
            object_type=$(echo "$object_type" | xargs)
            count=$(echo "$count" | xargs)
            
            if [ -n "$object_type" ] && [ -n "$count" ] && ! echo "$object_type" | grep -qE "^(OBJECT_TYPE|SELECT|FROM|WHERE|GROUP|ORDER)"; then
                # 处理NULL值
                [ -z "$count" ] && count="0"
                
                # 查询该类型的详细信息（最多20条）
                if [ "$count" -gt 0 ]; then
                    # 写入详细信息的表头（只写一次）
                    if [ "$detail_header_written" -eq 0 ]; then
                        append_raw_data_line "$data_file" "invalid_object_info" "OWNER|OBJECT_NAME|OBJECT_ID|OBJECT_TYPE|CREATED|LAST_DDL_TIME|STATUS"
                        detail_header_written=1
                    fi
                    
                    local sql2="select owner,object_name,object_id,object_type,created,to_char(last_ddl_time,'yyyy-mm-dd hh24:mi:ss') last_ddl_time,status from dba_objects where object_type='$object_type' and status != 'VALID' and rownum<20"
                    local result2=$(execute_sql "$sql2" "$conn_str")
                    
                    if [ -n "$result2" ] && ! echo "$result2" | grep -qiE "ORA-|SP2-|ERROR"; then
                        while IFS='|' read -r owner object_name object_id obj_type created last_ddl_time status; do
                            owner=$(echo "$owner" | xargs)
                            object_name=$(echo "$object_name" | xargs)
                            object_id=$(echo "$object_id" | xargs)
                            obj_type=$(echo "$obj_type" | xargs)
                            created=$(echo "$created" | xargs)
                            last_ddl_time=$(echo "$last_ddl_time" | xargs)
                            status=$(echo "$status" | xargs)
                            
                            if [ -n "$owner" ] && ! echo "$owner" | grep -qE "^(OWNER|SELECT|FROM|WHERE)"; then
                                # 写入详细信息行（不包含对象类型和数量，这些是统计信息）
                                # 格式：owner|object_name|object_id|obj_type|created|last_ddl_time|status
                                append_raw_data_line "$data_file" "invalid_object_info" "$owner|$object_name|$object_id|$obj_type|$created|$last_ddl_time|$status"
                            fi
                        done <<< "$result2"
                    fi
                fi
            fi
        done <<< "$result1"
    fi
    
    end_raw_data_section "$data_file" "invalid_object_info"
    
    log INFO "失效对象信息采集完成"
}

# 获取SQL文本和命令类型（辅助函数）
get_sqltext() {
    local conn_str=$1
    local sql_id=$2
    
    if [ -z "$sql_id" ] || [ "$sql_id" = "NULL" ] || [ "$sql_id" = "null" ]; then
        echo "||"
        return
    fi
    
    local sql="SELECT sql_fulltext, command_name 
FROM v\$sqlarea t1, V\$SQLCOMMAND t2 
WHERE sql_id = '$sql_id' AND t1.command_type=t2.COMMAND_TYPE
AND ROWNUM = 1"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 解析结果：sql_fulltext|command_name
        local sqltext=$(echo "$result" | head -1 | cut -d'|' -f1 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        local command_name=$(echo "$result" | head -1 | cut -d'|' -f2 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        
        if [ -z "$sqltext" ]; then
            echo "已刷出共享池|已刷出共享池"
        else
            # 转义特殊字符
            sqltext=$(echo "$sqltext" | sed 's/"/\\"/g' | sed "s/'/\\'/g")
            echo "${sqltext}|${command_name}"
        fi
    else
        echo "已刷出共享池|已刷出共享池"
    fi
}


# 从原始数据格式中获取字典值（用于sqltext_dict和sql_command_dict）
# 参数：$1=data_file, $2=section_name (如 "sqltext_dict" 或 "sql_command_dict"), $3=key (如 sql_id)
# 返回：对应的值，如果不存在则返回空字符串
get_raw_dict_value() {
    local data_file=$1
    local section_name=$2
    local key=$3
    
    # 优先从内存字典中查找
    if [ "$section_name" = "sqltext_dict" ]; then
        # 使用 -v 检查键是否存在（bash 4.2+），如果存在则返回值
        if [[ -v SQLTEXT_DICT[$key] ]]; then
            echo "${SQLTEXT_DICT[$key]}"
            return
        fi
    elif [ "$section_name" = "sql_command_dict" ]; then
        if [[ -v SQL_COMMAND_DICT[$key] ]]; then
            echo "${SQL_COMMAND_DICT[$key]}"
            return
        fi
    fi
    
    # 如果内存中没有，再从文件中查找
    if [ ! -f "$data_file" ]; then
        echo ""
        return
    fi
    
    # 在指定的section中查找key对应的值
    local in_section=false
    local found_value=""
    
    while IFS= read -r line || [ -n "$line" ]; do
        if echo "$line" | grep -q "^===SECTION:${section_name}==="; then
            in_section=true
            continue
        fi
        if echo "$line" | grep -q "^===END_SECTION:${section_name}==="; then
            in_section=false
            continue
        fi
        if [ "$in_section" = true ] && [ -n "$line" ]; then
            # 解析格式：key|value（使用|分隔）
            local line_key=$(echo "$line" | awk 'BEGIN{FS="|"}{print $1}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            local line_value=$(echo "$line" | awk 'BEGIN{FS="|"}{for(i=2;i<=NF;i++) {if(i>2) printf "|"; printf "%s", $i}}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            if [ "$line_key" = "$key" ]; then
                found_value="$line_value"
                break
            fi
        fi
    done < "$data_file"
    
    echo "$found_value"
}

# 更新sqltext_dict和sql_command_dict（存储在内存中，避免频繁文件操作导致错乱）
update_sqltext_dict() {
    local data_file=$1
    local sql_id=$2
    local sqltext=$3
    local command_name=$4
    
    if [ -z "$sql_id" ] || [ "$sql_id" = "NULL" ] || [ "$sql_id" = "null" ]; then
        return
    fi
    
    # 检查sql_id是否已经在内存字典中（使用 -v 检查键是否存在）
    if [[ ! -v SQLTEXT_DICT[$sql_id] ]]; then
        log DEBUG "update_sqltext_dict: 开始处理 sql_id=$sql_id（存储到内存）"
        # 如果不存在，则添加到内存字典中
        SQLTEXT_DICT[$sql_id]="$sqltext"
        SQL_COMMAND_DICT[$sql_id]="$command_name"
        log DEBUG "update_sqltext_dict: 处理完成 sql_id=$sql_id"
    else
        log DEBUG "update_sqltext_dict: sql_id=$sql_id 已存在于内存字典中，跳过"
    fi
}

# 追加metric_data到内存数组（避免频繁文件操作导致错乱）
append_metric_data_to_memory() {
    local data_line=$1
    
    if [ -n "$data_line" ]; then
        METRIC_DATA_ARRAY+=("$data_line")
        log DEBUG "append_metric_data_to_memory: 追加metric_data到内存，当前总数: ${#METRIC_DATA_ARRAY[@]}"
    fi
}

# 将内存中的sqltext_dict、sql_command_dict和metric_data写入文件（在Oracle采集结束时调用）
flush_sqltext_dicts_to_file() {
    local data_file=$1
    
    log INFO "开始将内存中的sqltext_dict、sql_command_dict和metric_data写入文件"
    
    # 写入sqltext_dict section
    if [ ${#SQLTEXT_DICT[@]} -gt 0 ]; then
        # 写入sqltext_dict section
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:sqltext_dict===" "$data_file"; then
            echo "===SECTION:sqltext_dict===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "sqltext_dict" "SQL_ID|SQL_TEXT"
            log DEBUG "flush_sqltext_dicts_to_file: 创建sqltext_dict section"
        else
            # 如果section已存在，删除结束标记（如果存在）
            if grep -q "===END_SECTION:sqltext_dict===" "$data_file"; then
                sed -i '/^===END_SECTION:sqltext_dict===$/d' "$data_file"
                log DEBUG "flush_sqltext_dicts_to_file: 删除sqltext_dict结束标记"
            fi
            # 检查section开始标记后的第一行是否是表头，如果不是则添加表头
            local section_line=$(grep -n "===SECTION:sqltext_dict===" "$data_file" | head -1 | cut -d: -f1)
            local next_line=$((section_line + 1))
            local header_line=$(sed -n "${next_line}p" "$data_file")
            if [ "$header_line" != "SQL_ID|SQL_TEXT" ]; then
                # 在section开始标记后插入表头
                sed -i "${section_line}a SQL_ID|SQL_TEXT" "$data_file"
                log DEBUG "flush_sqltext_dicts_to_file: 添加sqltext_dict表头"
            fi
        fi
        
        # 写入所有sqltext_dict数据（使用|分隔：sql_id|sqltext）
        local count=0
        for sql_id in "${!SQLTEXT_DICT[@]}"; do
            append_raw_data_line "$data_file" "sqltext_dict" "$sql_id|${SQLTEXT_DICT[$sql_id]}"
            count=$((count + 1))
        done
        log DEBUG "flush_sqltext_dicts_to_file: 写入 $count 条sqltext_dict记录"
        
        # 关闭sqltext_dict section
        end_raw_data_section "$data_file" "sqltext_dict"
    else
        log DEBUG "flush_sqltext_dicts_to_file: sqltext_dict为空，跳过"
    fi
    
    # 写入sql_command_dict section
    if [ ${#SQL_COMMAND_DICT[@]} -gt 0 ]; then
        # 写入sql_command_dict section
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:sql_command_dict===" "$data_file"; then
            echo "===SECTION:sql_command_dict===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "sql_command_dict" "SQL_ID|COMMAND_NAME"
            log DEBUG "flush_sqltext_dicts_to_file: 创建sql_command_dict section"
        else
            # 如果section已存在，删除结束标记（如果存在）
            if grep -q "===END_SECTION:sql_command_dict===" "$data_file"; then
                sed -i '/^===END_SECTION:sql_command_dict===$/d' "$data_file"
                log DEBUG "flush_sqltext_dicts_to_file: 删除sql_command_dict结束标记"
            fi
            # 检查section开始标记后的第一行是否是表头，如果不是则添加表头
            local section_line=$(grep -n "===SECTION:sql_command_dict===" "$data_file" | head -1 | cut -d: -f1)
            local next_line=$((section_line + 1))
            local header_line=$(sed -n "${next_line}p" "$data_file")
            if [ "$header_line" != "SQL_ID|COMMAND_NAME" ]; then
                # 在section开始标记后插入表头
                sed -i "${section_line}a SQL_ID|COMMAND_NAME" "$data_file"
                log DEBUG "flush_sqltext_dicts_to_file: 添加sql_command_dict表头"
            fi
        fi
        
        # 写入所有sql_command_dict数据（使用|分隔：sql_id|command_name）
        local count=0
        for sql_id in "${!SQL_COMMAND_DICT[@]}"; do
            append_raw_data_line "$data_file" "sql_command_dict" "$sql_id|${SQL_COMMAND_DICT[$sql_id]}"
            count=$((count + 1))
        done
        log DEBUG "flush_sqltext_dicts_to_file: 写入 $count 条sql_command_dict记录"
        
        # 关闭sql_command_dict section
        end_raw_data_section "$data_file" "sql_command_dict"
    else
        log DEBUG "flush_sqltext_dicts_to_file: sql_command_dict为空，跳过"
    fi
    
    # 写入metric_data section
    if [ ${#METRIC_DATA_ARRAY[@]} -gt 0 ]; then
        # 写入metric_data section
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:metric_data===" "$data_file"; then
            echo "===SECTION:metric_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "metric_data" "metric_id|avg_value|max_value|min_value|percentile_95"
            log DEBUG "flush_sqltext_dicts_to_file: 创建metric_data section"
        else
            # 如果section已存在，删除结束标记（如果存在）
            if grep -q "===END_SECTION:metric_data===" "$data_file"; then
                sed -i '/^===END_SECTION:metric_data===$/d' "$data_file"
                log DEBUG "flush_sqltext_dicts_to_file: 删除metric_data结束标记"
            fi
            # 检查section开始标记后的第一行是否是表头，如果不是则添加表头
            local section_line=$(grep -n "===SECTION:metric_data===" "$data_file" | head -1 | cut -d: -f1)
            local next_line=$((section_line + 1))
            local header_line=$(sed -n "${next_line}p" "$data_file")
            if [ "$header_line" != "metric_id|avg_value|max_value|min_value|percentile_95" ]; then
                # 在section开始标记后插入表头
                sed -i "${section_line}a metric_id|avg_value|max_value|min_value|percentile_95" "$data_file"
                log DEBUG "flush_sqltext_dicts_to_file: 添加metric_data表头"
            fi
        fi
        
        # 写入所有metric_data数据
        local count=0
        for data_line in "${METRIC_DATA_ARRAY[@]}"; do
            append_raw_data_line "$data_file" "metric_data" "$data_line"
            count=$((count + 1))
        done
        log DEBUG "flush_sqltext_dicts_to_file: 写入 $count 条metric_data记录"
        
        # 关闭metric_data section
        end_raw_data_section "$data_file" "metric_data"
    else
        log DEBUG "flush_sqltext_dicts_to_file: metric_data为空，跳过"
    fi
    
    log INFO "内存中的sqltext_dict、sql_command_dict和metric_data已成功写入文件"
}

# 采集高redo等待SQL数据
collect_top_redo_wait_sql() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集高redo等待SQL数据..."
    
    local sql="select * from 
                    (select event,sql_id,count(*) from v\$active_session_history where sample_time > TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS') and event in ('latch: redo copy','latch: redo allocation','log file sync','log buffer space')
                    and sql_id is not null group by event,sql_id order by 3 desc) where rownum<11"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入原始数据（TSV格式）
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_redo_wait_sql_data===" "$data_file"; then
            echo "===SECTION:top_redo_wait_sql_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "top_redo_wait_sql_data" "EVENT|SQL_ID|COMMAND_TYPE|COUNT"
            log DEBUG "写入原始数据: section=top_redo_wait_sql_data, 文件=$data_file"
        fi
        
        while IFS='|' read -r event sql_id count; do
            event=$(echo "$event" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            count=$(echo "$count" | xargs)
            
            if [ -n "$event" ] && [ -n "$sql_id" ] && [ -n "$count" ]; then
                # 获取SQL文本（与Python版本一致：直接调用_getSqltext并更新）
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                local command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                
                # 处理NULL值
                [ -z "$event" ] && event=""
                [ -z "$command_name" ] && command_name=""
                [ -z "$count" ] && count="0"
                
                # 写入原始数据行
                append_raw_data_line "$data_file" "top_redo_wait_sql_data" "$event|$sql_id|$command_name|$count"
            fi
        done <<< "$result"
        
        end_raw_data_section "$data_file" "top_redo_wait_sql_data"
        log INFO "高redo等待SQL数据采集完成"
    else
        log WARNING "未能获取高redo等待SQL数据"
    fi
}

# 采集热块相关的SQL数据
collect_hot_block_sql() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集热块相关的SQL数据..."
    
    local sql="select * from 
                (select event,sql_id,count(*) cnt from v\$active_session_history where sample_time > TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS') and event in ('buffer busy waits','latch: cache buffers chains','enq: TX - allocate ITL entry','read by other session','latch: In memory undo latch')
                and sql_id is not null group by event,sql_id order by 3 desc) where cnt > 10 and rownum<11"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入原始数据（TSV格式）
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_hot_block_sql_data===" "$data_file"; then
            echo "===SECTION:top_hot_block_sql_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "top_hot_block_sql_data" "EVENT|SQL_ID|COUNT"
            log DEBUG "写入原始数据: section=top_hot_block_sql_data, 文件=$data_file"
        fi
        
        while IFS='|' read -r event sql_id cnt; do
            event=$(echo "$event" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$sql_id" ] && [ -n "$cnt" ]; then
                # 获取SQL文本（与Python版本一致：直接调用_getSqltext并更新）
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                local command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                
                # 处理NULL值
                [ -z "$event" ] && event=""
                [ -z "$cnt" ] && cnt="0"
                
                # 写入原始数据行
                append_raw_data_line "$data_file" "top_hot_block_sql_data" "$event|$sql_id|$cnt"
            fi
        done <<< "$result"
        
        end_raw_data_section "$data_file" "top_hot_block_sql_data"
        log INFO "热块相关的SQL数据采集完成"
    else
        log WARNING "未能获取热块相关的SQL数据"
    fi
}

# 采集高闩锁等待SQL数据
collect_top_latch_wait_sql() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集高闩锁等待SQL数据..."
    
    local sql="select * from 
                (select event,sql_id,count(*) cnt from v\$active_session_history where sample_time > TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS') and event like 'latch%'
                and sql_id is not null group by event,sql_id order by 3 desc) where cnt > 10 and rownum<11"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_latch_wait_sql_data===" "$data_file"; then
            echo "===SECTION:top_latch_wait_sql_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "top_latch_wait_sql_data" "EVENT|SQL_ID|COMMAND_TYPE|COUNT"
            log DEBUG "写入原始数据: section=top_latch_wait_sql_data, 文件=$data_file"
        fi
        
        local record_count=0
        while IFS='|' read -r event sql_id cnt; do
            event=$(echo "$event" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$sql_id" ] && [ -n "$cnt" ]; then
                # 获取SQL文本（与Python版本一致：直接调用_getSqltext并更新）
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                local command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                
                # 写入TSV格式数据：EVENT\tSQL_ID\tCOMMAND_TYPE\tCOUNT
                append_raw_data_line "$data_file" "top_latch_wait_sql_data" "$event|$sql_id|$command_name|$cnt"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "top_latch_wait_sql_data"
        
        log INFO "高闩锁等待SQL数据采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取高闩锁等待SQL数据"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_latch_wait_sql_data===" "$data_file"; then
            write_raw_data "$data_file" "top_latch_wait_sql_data" ""
        fi
    fi
}

# 采集高队列锁等待SQL数据
collect_top_enq_wait_sql() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集高队列锁等待SQL数据..."
    
    local sql="select * from 
                (select event,sql_id,count(*) cnt from v\$active_session_history where sample_time > TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS') and event like 'enq%'
                and sql_id is not null group by event,sql_id order by 3 desc) where cnt > 10 and rownum<11"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        
        while IFS='|' read -r event sql_id cnt; do
            event=$(echo "$event" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$sql_id" ] && [ -n "$cnt" ]; then
                # 获取SQL文本（与Python版本一致：直接调用_getSqltext并更新）
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                local command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                # echo "sql_id: "$sql_id
                # echo "sqltext: "$sqltext
                # echo "command_name: "$command_name
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                
                # 转义特殊字符
                sqltext=$(echo "$sqltext" | sed 's/"/\\"/g')
                event=$(echo "$event" | sed 's/"/\\"/g')
                command_name=$(echo "$command_name" | sed 's/"/\\"/g')
            fi
        done <<< "$result"
        
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_enq_wait_sql_data===" "$data_file"; then
            echo "===SECTION:top_enq_wait_sql_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "top_enq_wait_sql_data" "EVENT|SQL_ID|COMMAND_TYPE|COUNT"
            log DEBUG "写入原始数据: section=top_enq_wait_sql_data, 文件=$data_file"
        fi
        
        local record_count=0
        while IFS='|' read -r event sql_id cnt; do
            event=$(echo "$event" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$sql_id" ] && [ -n "$cnt" ]; then
                # 获取SQL文本（与Python版本一致：直接调用_getSqltext并更新）
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                local command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                
                # 写入TSV格式数据：EVENT\tSQL_ID\tCOMMAND_TYPE\tCOUNT
                append_raw_data_line "$data_file" "top_enq_wait_sql_data" "$event|$sql_id|$command_name|$cnt"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "top_enq_wait_sql_data"
        
        log INFO "高队列锁等待SQL数据采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取高队列锁等待SQL数据"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_enq_wait_sql_data===" "$data_file"; then
            write_raw_data "$data_file" "top_enq_wait_sql_data" ""
        fi
    fi
}

# 采集序列等待SQL数据
collect_top_seq_wait_sql() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集序列等待SQL数据..."
    
    local sql="select * from 
                    (select event,sql_id,count(*) cnt from v\$active_session_history where sample_time > TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS') and event 
                    in ('enq: SQ - contention', 'enq: SV contention') and sql_id is not null group by event,sql_id order by 3 desc) where cnt > 10 and rownum<11"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        
        while IFS='|' read -r event sql_id cnt; do
            event=$(echo "$event" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$sql_id" ] && [ -n "$cnt" ]; then
                # 获取SQL文本（与Python版本一致：直接调用_getSqltext并更新）
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                local command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                
                # 转义特殊字符
                sqltext=$(echo "$sqltext" | sed 's/"/\\"/g')
                event=$(echo "$event" | sed 's/"/\\"/g')
                command_name=$(echo "$command_name" | sed 's/"/\\"/g')
            fi
        done <<< "$result"
        
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_seq_wait_sql_data===" "$data_file"; then
            echo "===SECTION:top_seq_wait_sql_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "top_seq_wait_sql_data" "EVENT|SQL_ID|COMMAND_TYPE|COUNT"
            log DEBUG "写入原始数据: section=top_seq_wait_sql_data, 文件=$data_file"
        fi
        
        local record_count=0
        while IFS='|' read -r event sql_id cnt; do
            event=$(echo "$event" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$sql_id" ] && [ -n "$cnt" ]; then
                # 获取SQL文本（与Python版本一致：直接调用_getSqltext并更新）
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                local command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                
                # 写入TSV格式数据：EVENT\tSQL_ID\tCOMMAND_TYPE\tCOUNT
                append_raw_data_line "$data_file" "top_seq_wait_sql_data" "$event|$sql_id|$command_name|$cnt"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "top_seq_wait_sql_data"
        
        log INFO "序列等待SQL数据采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取序列等待SQL数据"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_seq_wait_sql_data===" "$data_file"; then
            write_raw_data "$data_file" "top_seq_wait_sql_data" ""
        fi
    fi
}

# 采集UNDO历史统计数据
collect_undo_stats_history() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集UNDO历史统计数据..."
    
    local sql="SELECT begin_time, end_time, undoblks, txncount, maxquerylen, maxqueryid, 
       maxconcurrency, activeblks, unexpiredblks, expiredblks, tuned_undoretention
FROM v\$undostat
WHERE begin_time > TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS')
ORDER BY begin_time"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:undo_stats_history===" "$data_file"; then
            echo "===SECTION:undo_stats_history===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "undo_stats_history" "BEGIN_TIME|END_TIME|UNDO_BLOCKS|TRANSACTIONS|MAX_QUERY_TIME|MAX_QUERY_ID|MAX_CONCURRENCY|ACTIVE_BLOCKS|UNEXPIRED_BLOCKS|EXPIRED_BLOCKS|TUNED_UNDO_RETENTION|MAX_QUERY_COMAND_TYPE"
            log DEBUG "写入原始数据: section=undo_stats_history, 文件=$data_file"
        fi
        
        local record_count=0
        # 用于临时存储需要更新字典的sql_id信息（格式：sql_id|sqltext|command_type）
        local temp_dict_updates=""
        
        while IFS='|' read -r begin_time end_time undoblks txncount maxquerylen maxqueryid maxconcurrency activeblks unexpiredblks expiredblks tuned_undoretention; do
            begin_time=$(echo "$begin_time" | xargs)
            end_time=$(echo "$end_time" | xargs)
            undoblks=$(echo "$undoblks" | xargs)
            txncount=$(echo "$txncount" | xargs)
            maxquerylen=$(echo "$maxquerylen" | xargs)
            maxqueryid=$(echo "$maxqueryid" | xargs)
            maxconcurrency=$(echo "$maxconcurrency" | xargs)
            activeblks=$(echo "$activeblks" | xargs)
            unexpiredblks=$(echo "$unexpiredblks" | xargs)
            expiredblks=$(echo "$expiredblks" | xargs)
            tuned_undoretention=$(echo "$tuned_undoretention" | xargs)
            
            if [ -n "$begin_time" ]; then
                # 处理maxqueryid，获取SQL文本（与Python版本一致：if row[5] is not None）
                local max_query_command_type="-"
                if [ -n "$maxqueryid" ] && [ "$maxqueryid" != "NULL" ] && [ "$maxqueryid" != "null" ]; then
                    local sqltext_info=$(get_sqltext "$conn_str" "$maxqueryid")
                    local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                    max_query_command_type=$(echo "$sqltext_info" | cut -d'|' -f2)
                    
                    if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                        sqltext="已刷出共享池"
                        max_query_command_type="已刷出共享池"
                    fi
                    
                    # 将需要更新的字典信息保存到临时变量中，稍后批量更新
                    if [ -n "$temp_dict_updates" ]; then
                        temp_dict_updates="$temp_dict_updates"$'\n'"$maxqueryid|$sqltext|$max_query_command_type"
                    else
                        temp_dict_updates="$maxqueryid|$sqltext|$max_query_command_type"
                    fi
                    
                    if [ -z "$max_query_command_type" ] || [ "$max_query_command_type" = "已刷出共享池" ]; then
                        max_query_command_type="已刷出共享池"
                    fi
                fi
                
                # 处理NULL值，使用空字符串或0
                [ -z "$undoblks" ] && undoblks="0"
                [ -z "$txncount" ] && txncount="0"
                [ -z "$maxquerylen" ] && maxquerylen="0"
                [ -z "$maxconcurrency" ] && maxconcurrency="0"
                [ -z "$activeblks" ] && activeblks="0"
                [ -z "$unexpiredblks" ] && unexpiredblks="0"
                [ -z "$expiredblks" ] && expiredblks="0"
                [ -z "$tuned_undoretention" ] && tuned_undoretention="0"
                [ -z "$maxqueryid" ] && maxqueryid="-"
                [ -z "$max_query_command_type" ] && max_query_command_type="-"
                
                # 写入TSV格式数据：BEGIN_TIME\tEND_TIME\tUNDO_BLOCKS\tTRANSACTIONS\tMAX_QUERY_TIME\tMAX_QUERY_ID\tMAX_CONCURRENCY\tACTIVE_BLOCKS\tUNEXPIRED_BLOCKS\tEXPIRED_BLOCKS\tTUNED_UNDO_RETENTION\tMAX_QUERY_COMAND_TYPE
                append_raw_data_line "$data_file" "undo_stats_history" "$begin_time|$end_time|$undoblks|$txncount|$maxquerylen|$maxqueryid|$maxconcurrency|$activeblks|$unexpiredblks|$expiredblks|$tuned_undoretention|$max_query_command_type"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "undo_stats_history"
        
        # 在关闭undo_stats_history section后，批量更新字典（避免嵌套问题）
        if [ -n "$temp_dict_updates" ]; then
            while IFS='|' read -r sql_id sqltext command_type; do
                if [ -n "$sql_id" ] && [ "$sql_id" != "NULL" ] && [ "$sql_id" != "null" ]; then
                    update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_type"
                fi
            done <<< "$temp_dict_updates"
        fi
        
        log INFO "UNDO历史统计数据采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取UNDO历史统计数据"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:undo_stats_history===" "$data_file"; then
            write_raw_data "$data_file" "undo_stats_history" ""
        fi
    fi
}


# 采集RAC相关的统计数据
collect_rac_statistics_data() {
    local conn_str=$1
    local data_file=$2
    local snap_id=$3
    local instance_number=$4
    local is_reboot=$5
    local ash_begin_time=$6
    
    log INFO "采集RAC相关的统计数据..."
    
    # 计算时间差（秒）
    local current_time=$(date +%s)
    local ash_time_epoch=$(date -d "$ash_begin_time" +%s 2>/dev/null || echo "$current_time")
    local delta_time=$((current_time - ash_time_epoch))
    if [ "$delta_time" -le 0 ]; then
        delta_time=1
    fi
    
    # 获取blocksize（默认8192）
    local blocksize=8192
    local sql_blocksize="SELECT value FROM v\$parameter WHERE name='db_block_size'"
    local result_blocksize=$(execute_sql "$sql_blocksize" "$conn_str")
    if [ -n "$result_blocksize" ] && ! echo "$result_blocksize" | grep -qiE "ORA-|SP2-|ERROR"; then
        local blocksize_val=$(echo "$result_blocksize" | grep -v "VALUE" | head -1 | xargs)
        if [ -n "$blocksize_val" ] && [ "$blocksize_val" -gt 0 ]; then
            blocksize=$blocksize_val
        fi
    fi
    
    # SQL1: 查询v$sysstat统计信息
    local sql1=""
    if [ "$is_reboot" != "true" ] && [ -n "$snap_id" ] && [ -n "$instance_number" ]; then
        sql1="SELECT t1.name, t1.value-t2.value
FROM v\$sysstat t1, dba_hist_sysstat t2
WHERE t1.name IN ('gc cr blocks served','gc cr block flush time','gc cr blocks flushed',
                  'gc cr block build time','gc cr blocks built','gc current blocks served',
                  'gc current block pin time','gc current blocks pinned','gc current block flush time',
                  'DBWR fusion writes','gc current blocks flushed','gc cr blocks received',
                  'gc cr block receive time','gc current blocks received','gc blocks lost',
                  'gc current block receive time','session logical reads','physical reads',
                  'gcs messages sent','ges messages sent')
AND t1.name=t2.stat_name
AND t2.snap_id=$snap_id AND t2.instance_number=$instance_number"
    else
        sql1="SELECT t1.name, t1.value
FROM v\$sysstat t1
WHERE t1.name IN ('gc cr blocks served','gc cr block flush time','gc cr blocks flushed',
                  'gc cr block build time','gc cr blocks built','gc current blocks served',
                  'gc current block pin time','gc current blocks pinned','gc current block flush time',
                  'DBWR fusion writes','gc current blocks flushed','gc cr blocks received',
                  'gc cr block receive time','gc current blocks received','gc blocks lost',
                  'gc current block receive time','session logical reads','physical reads',
                  'gcs messages sent','ges messages sent')"
    fi
    
    local result1=$(execute_sql "$sql1" "$conn_str")
    
    # 初始化变量
    local session_logical_reads=0
    local physical_reads=0
    local dbwr_fusion_writes=0
    local gc_blocks_served=0
    local gc_blocks_received=0
    local gc_blocks_lost=0
    local cr_blocks_received=0
    local current_blocks_received=0
    local cr_block_receive_time=0
    local current_block_receive_time=0
    local cr_block_flush_time=0
    local current_block_flush_time=0
    local cr_blocks_flushed=0
    local current_blocks_flushed=0
    local cr_block_build_time=0
    local current_block_pin_time=0
    local cr_blocks_built=0
    local current_blocks_pinned=0
    local gcs_messages_sent=0
    local ges_messages_sent=0
    
    if [ -n "$result1" ] && ! echo "$result1" | grep -qiE "ORA-|SP2-|ERROR"; then
        while IFS='|' read -r name value; do
            name=$(echo "$name" | xargs)
            value=$(echo "$value" | xargs)
            [ -z "$value" ] && value=0
            
            case "$name" in
                "gc cr blocks served"|"gc current blocks served")
                    gc_blocks_served=$((gc_blocks_served + value))
                    ;;
                "gc cr blocks received")
                    cr_blocks_received=$((cr_blocks_received + value))
                    gc_blocks_received=$((gc_blocks_received + value))
                    ;;
                "gc current blocks received")
                    current_blocks_received=$((current_blocks_received + value))
                    gc_blocks_received=$((gc_blocks_received + value))
                    ;;
                "gc blocks lost")
                    gc_blocks_lost=$((gc_blocks_lost + value))
                    ;;
                "gc cr block receive time")
                    cr_block_receive_time=$((cr_block_receive_time + value))
                    ;;
                "gc current block receive time")
                    current_block_receive_time=$((current_block_receive_time + value))
                    ;;
                "gc cr block flush time")
                    cr_block_flush_time=$((cr_block_flush_time + value))
                    ;;
                "gc current block flush time")
                    current_block_flush_time=$((current_block_flush_time + value))
                    ;;
                "gc cr blocks flushed")
                    cr_blocks_flushed=$((cr_blocks_flushed + value))
                    ;;
                "gc current blocks flushed")
                    current_blocks_flushed=$((current_blocks_flushed + value))
                    ;;
                "gc cr block build time")
                    cr_block_build_time=$((cr_block_build_time + value))
                    ;;
                "gc current block pin time")
                    current_block_pin_time=$((current_block_pin_time + value))
                    ;;
                "gc cr blocks built")
                    cr_blocks_built=$((cr_blocks_built + value))
                    ;;
                "gc current blocks pinned")
                    current_blocks_pinned=$((current_blocks_pinned + value))
                    ;;
                "DBWR fusion writes")
                    dbwr_fusion_writes=$((dbwr_fusion_writes + value))
                    ;;
                "session logical reads")
                    session_logical_reads=$((session_logical_reads + value))
                    ;;
                "physical reads")
                    physical_reads=$((physical_reads + value))
                    ;;
                "gcs messages sent")
                    gcs_messages_sent=$((gcs_messages_sent + value))
                    ;;
                "ges messages sent")
                    ges_messages_sent=$((ges_messages_sent + value))
                    ;;
            esac
        done <<< "$result1"
    fi
    
    # 计算平均值
    local avg_cr_blocks_receive_time=0
    local avg_current_blocks_receive_time=0
    local avg_cr_blocks_flush_time=0
    local avg_current_blocks_flush_time=0
    local avg_cr_blocks_build_time=0
    local avg_current_blocks_pin_time=0
    
    if [ "$cr_blocks_received" -gt 0 ]; then
        avg_cr_blocks_receive_time=$(awk "BEGIN {printf \"%.2f\", ($cr_block_receive_time / $cr_blocks_received) * 10}")
    fi
    if [ "$current_blocks_received" -gt 0 ]; then
        avg_current_blocks_receive_time=$(awk "BEGIN {printf \"%.2f\", ($current_block_receive_time / $current_blocks_received) * 10}")
    fi
    if [ "$cr_blocks_flushed" -gt 0 ]; then
        avg_cr_blocks_flush_time=$(awk "BEGIN {printf \"%.2f\", ($cr_block_flush_time / $cr_blocks_flushed) * 10}")
    fi
    if [ "$current_blocks_flushed" -gt 0 ]; then
        avg_current_blocks_flush_time=$(awk "BEGIN {printf \"%.2f\", ($current_block_flush_time / $current_blocks_flushed) * 10}")
    fi
    if [ "$cr_blocks_built" -gt 0 ]; then
        avg_cr_blocks_build_time=$(awk "BEGIN {printf \"%.2f\", ($cr_block_build_time / $cr_blocks_built) * 10}")
    fi
    if [ "$current_blocks_pinned" -gt 0 ]; then
        avg_current_blocks_pin_time=$(awk "BEGIN {printf \"%.2f\", ($current_block_pin_time / $current_blocks_pinned) * 10}")
    fi
    
    # 计算比率
    local disk_access_ratio=0
    local remote_cache_access_ratio=0
    local local_cache_access_ratio=0
    if [ "$session_logical_reads" -gt 0 ]; then
        disk_access_ratio=$(awk "BEGIN {printf \"%.2f\", ($physical_reads / $session_logical_reads) * 100}")
        remote_cache_access_ratio=$(awk "BEGIN {printf \"%.2f\", ($gc_blocks_received / $session_logical_reads) * 100}")
        local_cache_access_ratio=$(awk "BEGIN {printf \"%.2f\", 100 - $disk_access_ratio - $remote_cache_access_ratio}")
    fi
    
    # SQL2: 查询v$dlm_misc统计信息
    local sql2=""
    if [ "$is_reboot" != "true" ] && [ -n "$snap_id" ] && [ -n "$instance_number" ]; then
        sql2="SELECT t1.name, t1.value-t2.value
FROM v\$dlm_misc t1, dba_hist_dlm_misc t2
WHERE t2.snap_id=$snap_id AND t2.instance_number=$instance_number
AND t1.name=t2.name
AND t1.name IN ('msgs received queued','msgs received queue time (ms)','msgs sent queued',
                'msgs sent queue time (ms)','msgs sent queue time on ksxp (ms)',
                'msgs sent queued on ksxp','gcs msgs received','gcs msgs process time(ms)',
                'ges msgs received','ges msgs process time(ms)','messages sent indirectly',
                'messages sent directly','flow control messages sent')"
    else
        sql2="SELECT t1.name, t1.value
FROM v\$dlm_misc t1
WHERE t1.name IN ('msgs received queued','msgs received queue time (ms)','msgs sent queued',
                  'msgs sent queue time (ms)','msgs sent queue time on ksxp (ms)',
                  'msgs sent queued on ksxp','gcs msgs received','gcs msgs process time(ms)',
                  'ges msgs received','ges msgs process time(ms)','messages sent indirectly',
                  'messages sent directly','flow control messages sent')"
    fi
    
    local result2=$(execute_sql "$sql2" "$conn_str")
    
    # 初始化DLM相关变量
    local messages_sent_directly=0
    local messages_sent_indirectly=0
    local flow_control_messages_sent=0
    local gcs_msgs_received=0
    local gcs_msgs_process_time=0
    local ges_msgs_received=0
    local ges_msgs_process_time=0
    local msgs_received_queued=0
    local msgs_received_queue_time=0
    local msgs_sent_queued=0
    local msgs_sent_queue_time=0
    local msgs_sent_queue_time_on_ksxp=0
    local msgs_sent_queued_on_ksxp=0
    
    if [ -n "$result2" ] && ! echo "$result2" | grep -qiE "ORA-|SP2-|ERROR"; then
        while IFS='|' read -r name value; do
            name=$(echo "$name" | xargs)
            value=$(echo "$value" | xargs)
            [ -z "$value" ] && value=0
            
            case "$name" in
                "msgs received queued")
                    msgs_received_queued=$((msgs_received_queued + value))
                    ;;
                "msgs received queue time (ms)")
                    msgs_received_queue_time=$((msgs_received_queue_time + value))
                    ;;
                "msgs sent queued")
                    msgs_sent_queued=$((msgs_sent_queued + value))
                    ;;
                "msgs sent queue time (ms)")
                    msgs_sent_queue_time=$((msgs_sent_queue_time + value))
                    ;;
                "msgs sent queue time on ksxp (ms)")
                    msgs_sent_queue_time_on_ksxp=$((msgs_sent_queue_time_on_ksxp + value))
                    ;;
                "msgs sent queued on ksxp")
                    msgs_sent_queued_on_ksxp=$((msgs_sent_queued_on_ksxp + value))
                    ;;
                "gcs msgs received")
                    gcs_msgs_received=$((gcs_msgs_received + value))
                    ;;
                "gcs msgs process time(ms)")
                    gcs_msgs_process_time=$((gcs_msgs_process_time + value))
                    ;;
                "ges msgs received")
                    ges_msgs_received=$((ges_msgs_received + value))
                    ;;
                "ges msgs process time(ms)")
                    ges_msgs_process_time=$((ges_msgs_process_time + value))
                    ;;
                "messages sent directly")
                    messages_sent_directly=$((messages_sent_directly + value))
                    ;;
                "messages sent indirectly")
                    messages_sent_indirectly=$((messages_sent_indirectly + value))
                    ;;
                "flow control messages sent")
                    flow_control_messages_sent=$((flow_control_messages_sent + value))
                    ;;
            esac
        done <<< "$result2"
    fi
    
    # 计算DLM相关平均值和比率
    local avg_msgs_received_queue_time=0
    local avg_msgs_sent_queue_time=0
    local avg_msgs_sent_queue_time_on_ksxp=0
    local avg_gcs_msgs_process_time=0
    local avg_ges_msgs_process_time=0
    local messages_sent_directly_ratio=0
    local messages_sent_indirectly_ratio=0
    local flow_control_messages_sent_ratio=0
    
    if [ "$msgs_received_queued" -gt 0 ]; then
        avg_msgs_received_queue_time=$(awk "BEGIN {printf \"%.2f\", $msgs_received_queue_time / $msgs_received_queued}")
    fi
    if [ "$msgs_sent_queued" -gt 0 ]; then
        avg_msgs_sent_queue_time=$(awk "BEGIN {printf \"%.2f\", $msgs_sent_queue_time / $msgs_sent_queued}")
    fi
    if [ "$msgs_sent_queued_on_ksxp" -gt 0 ]; then
        avg_msgs_sent_queue_time_on_ksxp=$(awk "BEGIN {printf \"%.2f\", $msgs_sent_queue_time_on_ksxp / $msgs_sent_queued_on_ksxp}")
    fi
    if [ "$gcs_msgs_received" -gt 0 ]; then
        avg_gcs_msgs_process_time=$(awk "BEGIN {printf \"%.2f\", $gcs_msgs_process_time / $gcs_msgs_received}")
    fi
    if [ "$ges_msgs_received" -gt 0 ]; then
        avg_ges_msgs_process_time=$(awk "BEGIN {printf \"%.2f\", $ges_msgs_process_time / $ges_msgs_received}")
    fi
    
    local total_messages_sent=$((messages_sent_directly + messages_sent_indirectly + flow_control_messages_sent))
    if [ "$total_messages_sent" -gt 0 ]; then
        messages_sent_directly_ratio=$(awk "BEGIN {printf \"%.2f\", ($messages_sent_directly / $total_messages_sent) * 100}")
        messages_sent_indirectly_ratio=$(awk "BEGIN {printf \"%.2f\", ($messages_sent_indirectly / $total_messages_sent) * 100}")
        flow_control_messages_sent_ratio=$(awk "BEGIN {printf \"%.2f\", ($flow_control_messages_sent / $total_messages_sent) * 100}")
    fi
    
    # 计算预估互连流量
    local estd_interconnect_traffic=$(awk "BEGIN {printf \"%.2f\", (($gc_blocks_served + $gc_blocks_received) * $blocksize + ($gcs_messages_sent + $ges_messages_sent) * 200 + ($gcs_msgs_received + $ges_msgs_received) * 200) / 1024}")
    
    # 写入section开始标记（如果不存在）
    if [ ! -f "$data_file" ] || ! grep -q "===SECTION:rac_statistics_data===" "$data_file"; then
        echo "===SECTION:rac_statistics_data===" >> "$data_file"
        # 写入表头（键值对格式）
        append_raw_data_line "$data_file" "rac_statistics_data" "KEY|VALUE"
        log DEBUG "写入原始数据: section=rac_statistics_data, 文件=$data_file"
    fi
    
    # 写入TSV格式数据：每行一个键值对（KEY\tVALUE）
    append_raw_data_line "$data_file" "rac_statistics_data" "Estd Interconnect traffic (KB)|$(awk "BEGIN {printf \"%.2f\", $estd_interconnect_traffic}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Buffer access - disk %|$(awk "BEGIN {printf \"%.2f\", $disk_access_ratio}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Buffer access - remote cache %|$(awk "BEGIN {printf \"%.2f\", $remote_cache_access_ratio}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Buffer access - local cache %|$(awk "BEGIN {printf \"%.2f\", $local_cache_access_ratio}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Global Cache blocks served per second|$(awk "BEGIN {printf \"%.2f\", $gc_blocks_served / $delta_time}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Global Cache blocks received per second|$(awk "BEGIN {printf \"%.2f\", $gc_blocks_received / $delta_time}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "DBWR fusion writes per second|$(awk "BEGIN {printf \"%.2f\", $dbwr_fusion_writes / $delta_time}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Global Cache blocks lost|$gc_blocks_lost"
    append_raw_data_line "$data_file" "rac_statistics_data" "Avg global cache cr block receive time (ms)|$(awk "BEGIN {printf \"%.2f\", $avg_cr_blocks_receive_time}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Avg global cache current block receive time (ms)|$(awk "BEGIN {printf \"%.2f\", $avg_current_blocks_receive_time}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Avg global cache cr block flush time (ms)|$(awk "BEGIN {printf \"%.2f\", $avg_cr_blocks_flush_time}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Avg global cache current block flush time (ms)|$(awk "BEGIN {printf \"%.2f\", $avg_current_blocks_flush_time}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Avg global cache cr block build time (ms)|$(awk "BEGIN {printf \"%.2f\", $avg_cr_blocks_build_time}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Avg global cache current block pin time (ms)|$(awk "BEGIN {printf \"%.2f\", $avg_current_blocks_pin_time}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "% of direct sent messages|$(awk "BEGIN {printf \"%.2f\", $messages_sent_directly_ratio}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "% of indirect sent messages|$(awk "BEGIN {printf \"%.2f\", $messages_sent_indirectly_ratio}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "% of flow controlled messages|$(awk "BEGIN {printf \"%.2f\", $flow_control_messages_sent_ratio}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Avg message received queue time (ms)|$(awk "BEGIN {printf \"%.2f\", $avg_msgs_received_queue_time}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Avg message sent queue time (ms)|$(awk "BEGIN {printf \"%.2f\", $avg_msgs_sent_queue_time}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Avg message sent queue time on ksxp (ms)|$(awk "BEGIN {printf \"%.2f\", $avg_msgs_sent_queue_time_on_ksxp}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Avg GCS message process time (ms)|$(awk "BEGIN {printf \"%.2f\", $avg_gcs_msgs_process_time}")"
    append_raw_data_line "$data_file" "rac_statistics_data" "Avg GES message process time (ms)|$(awk "BEGIN {printf \"%.2f\", $avg_ges_msgs_process_time}")"
    
    # 写入section结束标记
    end_raw_data_section "$data_file" "rac_statistics_data"
    
    log INFO "RAC相关的统计数据采集完成"
}

# 采集UNDO表空间当前使用情况
collect_undo_tbs_usage() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "采集UNDO表空间当前使用情况..."
    
    local sql="SELECT tablespace_name,
    SUM(CASE WHEN status = 'ACTIVE' THEN bytes END)/1024/1024 AS active_bytes,
    SUM(CASE WHEN status = 'UNEXPIRED' THEN bytes END)/1024/1024 AS unexpired_bytes,
    SUM(CASE WHEN status = 'EXPIRED' THEN bytes END)/1024/1024 AS expired_bytes
FROM dba_undo_extents
GROUP BY tablespace_name
ORDER BY 4 DESC"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:undo_tbs_usage===" "$data_file"; then
            echo "===SECTION:undo_tbs_usage===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "undo_tbs_usage" "TABLESPACE_NAME|ACTIVE_BYTES|UNEXPIRED_BYTES|EXPIRED_BYTES"
            log DEBUG "写入原始数据: section=undo_tbs_usage, 文件=$data_file"
        fi
        
        local record_count=0
        while IFS='|' read -r tablespace_name active_bytes unexpired_bytes expired_bytes; do
            tablespace_name=$(echo "$tablespace_name" | xargs)
            active_bytes=$(echo "$active_bytes" | xargs)
            unexpired_bytes=$(echo "$unexpired_bytes" | xargs)
            expired_bytes=$(echo "$expired_bytes" | xargs)
            
            if [ -n "$tablespace_name" ]; then
                # 处理NULL值
                [ -z "$active_bytes" ] && active_bytes="0"
                [ -z "$unexpired_bytes" ] && unexpired_bytes="0"
                [ -z "$expired_bytes" ] && expired_bytes="0"
                
                # 写入TSV格式数据：TABLESPACE_NAME\tACTIVE_BYTES\tUNEXPIRED_BYTES\tEXPIRED_BYTES
                append_raw_data_line "$data_file" "undo_tbs_usage" "$tablespace_name|$active_bytes|$unexpired_bytes|$expired_bytes"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "undo_tbs_usage"
        
        log INFO "UNDO表空间使用情况采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取UNDO表空间使用情况"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:undo_tbs_usage===" "$data_file"; then
            write_raw_data "$data_file" "undo_tbs_usage" ""
        fi
    fi
}

# 采集undo相关的SQL数据
collect_undo_wait_sql_data() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集undo相关的SQL数据..."
    
    local sql="select * from 
                (select event,sql_id,count(*) cnt from v\$active_session_history where sample_time > TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS') and 
                event in ('enq: US - contention')
                and sql_id is not null group by event,sql_id order by 3 desc) where cnt > 10 and rownum<11"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_undo_wait_sql_data===" "$data_file"; then
            echo "===SECTION:top_undo_wait_sql_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "top_undo_wait_sql_data" "EVENT|SQL_ID|COMMAND_TYPE|COUNT"
            log DEBUG "写入原始数据: section=top_undo_wait_sql_data, 文件=$data_file"
        fi
        
        local record_count=0
        while IFS='|' read -r event sql_id cnt; do
            event=$(echo "$event" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$sql_id" ] && [ -n "$cnt" ]; then
                # 获取SQL文本（与Python版本一致：直接调用_getSqltext并更新）
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                local command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                
                # 写入TSV格式数据：EVENT\tSQL_ID\tCOMMAND_TYPE\tCOUNT
                append_raw_data_line "$data_file" "top_undo_wait_sql_data" "$event|$sql_id|$command_name|$cnt"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "top_undo_wait_sql_data"
        
        log INFO "undo相关的SQL数据采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取undo相关的SQL数据"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_undo_wait_sql_data===" "$data_file"; then
            write_raw_data "$data_file" "top_undo_wait_sql_data" ""
        fi
    fi
}

# 采集RAC集群相关的SQL数据
collect_rac_sql_data() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集RAC集群相关的SQL数据..."
    
    local sql="select * from 
                (select event,sql_id,count(*) cnt from v\$active_session_history where sample_time > TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS') and 
                event in ('gcs remote message','gcs yield cpu','gc buffer busy acquire','gc buffer busy release','gc cr multi block mixed','gc cr multi block grant','gc current multi block request','gc cr block 2-way','gc cr block busy','gc cr block congested','gc current block 2-way','gc current block busy','gc current block congested','gc current retry','gc current split','gc cr grant 2-way','gc cr grant busy','gc cr grant congested','gc cr disk read','gc current grant 2-way','gc current grant busy','gc current grant quiesce','gc domain validation','gcs resource directory to be unfrozen','gcs enter server mode','gcs ddet enter server mode','gcs to be enabled','gcs log flush sync','gcs log flush sync')   
                and sql_id is not null group by event,sql_id order by 3 desc) where cnt > 10 and rownum<11"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_rac_sql_data===" "$data_file"; then
            echo "===SECTION:top_rac_sql_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "top_rac_sql_data" "EVENT|SQL_ID|COMMAND_TYPE|COUNT"
            log DEBUG "写入原始数据: section=top_rac_sql_data, 文件=$data_file"
        fi
        
        local record_count=0
        while IFS='|' read -r event sql_id cnt; do
            event=$(echo "$event" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$sql_id" ] && [ -n "$cnt" ]; then
                # 获取SQL文本（与Python版本一致：直接调用_getSqltext并更新）
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                local command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                
                # 写入TSV格式数据：EVENT\tSQL_ID\tCOMMAND_TYPE\tCOUNT
                append_raw_data_line "$data_file" "top_rac_sql_data" "$event|$sql_id|$command_name|$cnt"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "top_rac_sql_data"
        
        log INFO "RAC集群相关的SQL数据采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取RAC集群相关的SQL数据"
    fi
}

# 采集IO相关的SQL数据
collect_top_io_wait_sql() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集IO相关的SQL数据..."
    
    local sql="select * from 
                (select event,sql_id,count(*) cnt from v\$active_session_history where sample_time > TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS') and event in ('Parameter File I/O','Disk file operations I/O','Disk file Mirror/Media Repair Write',
                'direct path sync','Log archive I/O','control file sequential read','control file parallel write','recovery read','log file sequential read','log file single write'
                ,'log file parallel write','db file sequential read','db file scattered read','db file single write','db file async I/O submit','db file parallel read','direct path read','direct path write','utl_file I/O')
                and sql_id is not null group by event,sql_id order by 3 desc) where cnt > 10 and rownum<11"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        while IFS='|' read -r event sql_id cnt; do
            event=$(echo "$event" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$sql_id" ] && [ -n "$cnt" ]; then
                # 获取SQL文本（与Python版本一致：直接调用_getSqltext并更新）
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                local command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                
                # 转义特殊字符
                sqltext=$(echo "$sqltext" | sed 's/"/\\"/g')
                event=$(echo "$event" | sed 's/"/\\"/g')
                command_name=$(echo "$command_name" | sed 's/"/\\"/g')
                
            fi
        done <<< "$result"
        
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_io_wait_sql_data===" "$data_file"; then
            echo "===SECTION:top_io_wait_sql_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "top_io_wait_sql_data" "EVENT|SQL_ID|COMMAND_TYPE|COUNT"
            log DEBUG "写入原始数据: section=top_io_wait_sql_data, 文件=$data_file"
        fi
        
        local record_count=0
        while IFS='|' read -r event sql_id cnt; do
            event=$(echo "$event" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$sql_id" ] && [ -n "$cnt" ]; then
                # 获取SQL文本（与Python版本一致：直接调用_getSqltext并更新）
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                local command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                
                # 写入TSV格式数据：EVENT\tSQL_ID\tCOMMAND_TYPE\tCOUNT
                append_raw_data_line "$data_file" "top_io_wait_sql_data" "$event|$sql_id|$command_name|$cnt"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "top_io_wait_sql_data"
        
        log INFO "IO相关的SQL数据采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取IO相关的SQL数据"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_io_wait_sql_data===" "$data_file"; then
            write_raw_data "$data_file" "top_io_wait_sql_data" ""
        fi
    fi
}

# 采集buffer cache相关的SQL数据
collect_buffer_cache_sql_data() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集buffer cache相关的SQL数据..."
    
    local sql="select * from (select event,sql_id,count(*) cnt from v\$active_session_history where sample_time > TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS') and event in ('latch: cache buffers lru chain','latch: checkpoint queue latch','latch: cache buffers chains','db file sequential read','db file scattered read','db file parallel write','free buffer waits','write complete waits','write complete waits: flash cache')
                and sql_id is not null group by event,sql_id order by 3 desc) where cnt > 10 and rownum<11"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        while IFS='|' read -r event sql_id cnt; do
            event=$(echo "$event" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$sql_id" ] && [ -n "$cnt" ]; then
                # 获取SQL文本（与Python版本一致：直接调用_getSqltext并更新）
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                local command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                
                # 转义特殊字符
                sqltext=$(echo "$sqltext" | sed 's/"/\\"/g')
                event=$(echo "$event" | sed 's/"/\\"/g')
                command_name=$(echo "$command_name" | sed 's/"/\\"/g')
                
            fi
        done <<< "$result"
        
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:buffer_cache_sql_data===" "$data_file"; then
            echo "===SECTION:buffer_cache_sql_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "buffer_cache_sql_data" "EVENT|SQL_ID|COMMAND_TYPE|COUNT"
            log DEBUG "写入原始数据: section=buffer_cache_sql_data, 文件=$data_file"
        fi
        
        local record_count=0
        while IFS='|' read -r event sql_id cnt; do
            event=$(echo "$event" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$sql_id" ] && [ -n "$cnt" ]; then
                # 获取SQL文本（与Python版本一致：直接调用_getSqltext并更新）
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                local command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                
                # 写入TSV格式数据：EVENT\tSQL_ID\tCOMMAND_TYPE\tCOUNT
                append_raw_data_line "$data_file" "buffer_cache_sql_data" "$event|$sql_id|$command_name|$cnt"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "buffer_cache_sql_data"
        
        log INFO "buffer cache相关的SQL数据采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取buffer cache相关的SQL数据"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:buffer_cache_sql_data===" "$data_file"; then
            write_raw_data "$data_file" "buffer_cache_sql_data" ""
        fi
    fi
}

# 采集DBWR活动数据
collect_dbwr_activity_data() {
    local conn_str=$1
    local data_file=$2
    local snap_id=$3
    local instance_number=$4
    local is_reboot=$5
    local ash_begin_time=$6
    
    log INFO "采集DBWR活动数据..."
    
    local sql=""
    if [ "$is_reboot" != "true" ] && [ -n "$snap_id" ] && [ -n "$instance_number" ]; then
        sql="select stat_name,t1.value - t2.value from v\$sysstat t1, dba_hist_sysstat t2 where t2.snap_id=$snap_id and t2.instance_number=$instance_number and stat_name in ( 'DBWR checkpoint buffers written',
        'DBWR checkpoints',
        'DBWR fusion writes',
        'DBWR object drop buffers written',
        'DBWR revisited being-written buffer',
        'DBWR tablespace checkpoint buffers written',
        'DBWR thread checkpoint buffers written',
        'DBWR transaction table writes',
        'DBWR undo block writes',
        'dirty buffers inspected',
        'deferred (CURRENT) block cleanout applications',
        'consistent gets - examination',
        'consistent gets direct',
        'consistent gets from cache',
        'consistent gets from cache (fastpath)')  and t1.name=t2.stat_name order by 2 desc"
    else
        sql="select name,value from v\$sysstat where name in ('DBWR checkpoints','DBWR checkpoint buffers written','DBWR thread checkpoint buffers written','DBWR tablespace checkpoint buffers written','DBWR object drop buffers written','DBWR parallel query checkpoint buffers written','DBWR revisited being-written buffer','DBWR transaction table writes','DBWR undo block writes')"
    fi
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 计算时间差（秒）
        local current_time=$(date +%s)
        local ash_time_epoch=$(date -d "$ash_begin_time" +%s 2>/dev/null || echo "$current_time")
        local delta_time=$((current_time - ash_time_epoch))
        if [ "$delta_time" -le 0 ]; then
            delta_time=1
        fi
        
        while IFS='|' read -r name value; do
            name=$(echo "$name" | xargs)
            value=$(echo "$value" | xargs)
            
            if [ -n "$name" ] && [ -n "$value" ]; then
                # 计算每秒值
                local value_per_sec=$(awk "BEGIN {printf \"%.2f\", $value / $delta_time}")
                
                # 转义特殊字符
                name=$(echo "$name" | sed 's/"/\\"/g')                
            fi
        done <<< "$result"
        
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:dbwr_activity_data===" "$data_file"; then
            echo "===SECTION:dbwr_activity_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "dbwr_activity_data" "NAME|VALUE|VALUE_PER_SEC"
            log DEBUG "写入原始数据: section=dbwr_activity_data, 文件=$data_file"
        fi
        
        local record_count=0
        while IFS='|' read -r name value; do
            name=$(echo "$name" | xargs)
            value=$(echo "$value" | xargs)
            
            if [ -n "$name" ] && [ -n "$value" ]; then
                # 计算每秒值
                local value_per_sec=$(awk "BEGIN {printf \"%.2f\", $value / $delta_time}")
                
                # 写入TSV格式数据：NAME\tVALUE\tVALUE_PER_SEC
                append_raw_data_line "$data_file" "dbwr_activity_data" "$name|$value|$value_per_sec"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "dbwr_activity_data"
        
        log INFO "DBWR活动数据采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取DBWR活动数据"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:dbwr_activity_data===" "$data_file"; then
            write_raw_data "$data_file" "dbwr_activity_data" ""
        fi
    fi
}

# 采集共享池等待事件高的SQL数据
collect_top_shared_pool_wait_sql() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集共享池等待事件高的SQL数据..."
    
    local sql="select * from (select event,sql_id,count(*) cnt from v\$active_session_history where sample_time > TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS') and event in ('latch: shared pool','library cache pin','library cache lock','library cache load lock','library cache: mutex X','library cache: mutex S','library cache revalidation','library cache shutdown','cursor: mutex X','cursor: mutex S','cursor: pin X','cursor: pin S','cursor: pin S wait on X')
                and sql_id is not null group by event,sql_id order by 3 desc) where cnt > 10 and rownum<11"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        while IFS='|' read -r event sql_id cnt; do
            event=$(echo "$event" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$sql_id" ] && [ -n "$cnt" ]; then
                # 获取SQL文本（与Python版本一致：直接调用_getSqltext并更新）
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                local command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                
                # 转义特殊字符
                sqltext=$(echo "$sqltext" | sed 's/"/\\"/g')
                event=$(echo "$event" | sed 's/"/\\"/g')
                command_name=$(echo "$command_name" | sed 's/"/\\"/g')
                
            fi
        done <<< "$result"
        
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_shared_pool_wait_sql_data===" "$data_file"; then
            echo "===SECTION:top_shared_pool_wait_sql_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "top_shared_pool_wait_sql_data" "EVENT|SQL_ID|COMMAND_TYPE|COUNT"
            log DEBUG "写入原始数据: section=top_shared_pool_wait_sql_data, 文件=$data_file"
        fi
        
        local record_count=0
        while IFS='|' read -r event sql_id cnt; do
            event=$(echo "$event" | xargs)
            sql_id=$(echo "$sql_id" | xargs)
            cnt=$(echo "$cnt" | xargs)
            
            if [ -n "$event" ] && [ -n "$sql_id" ] && [ -n "$cnt" ]; then
                # 获取SQL文本（与Python版本一致：直接调用_getSqltext并更新）
                local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1)
                local command_name=$(echo "$sqltext_info" | cut -d'|' -f2)
                
                if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                    sqltext="已刷出共享池"
                    command_name="已刷出共享池"
                fi
                
                # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                
                # 写入TSV格式数据：EVENT\tSQL_ID\tCOMMAND_TYPE\tCOUNT
                append_raw_data_line "$data_file" "top_shared_pool_wait_sql_data" "$event|$sql_id|$command_name|$cnt"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "top_shared_pool_wait_sql_data"
        
        log INFO "共享池等待事件高的SQL数据采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取共享池等待事件高的SQL数据"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:top_shared_pool_wait_sql_data===" "$data_file"; then
            write_raw_data "$data_file" "top_shared_pool_wait_sql_data" ""
        fi
    fi
}

# 采集共享池中高解析次数SQL数据
collect_high_parse_count_sql() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "采集共享池中高解析次数SQL数据..."
    
    local sql="select * from (select sql_id,executions,parse_calls,version_count,sharable_mem,sorts from v\$sqlarea where executions/parse_calls < 1.1 and parse_calls > 0 and executions>10000 order by 3 desc) where rownum<11"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:high_parse_count_sql===" "$data_file"; then
            echo "===SECTION:high_parse_count_sql===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "high_parse_count_sql" "SQL_ID|EXECUTIONS|PARSE_CALLS|VERSION_COUNT|SHARABLE_MEM|SORTS|COMMAND_NAME"
            log DEBUG "写入原始数据: section=high_parse_count_sql, 文件=$data_file"
        fi
        
        local record_count=0
        while IFS='|' read -r sql_id executions parse_calls version_count sharable_mem sorts; do
            sql_id=$(echo "$sql_id" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            executions=$(echo "$executions" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            parse_calls=$(echo "$parse_calls" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            version_count=$(echo "$version_count" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            sharable_mem=$(echo "$sharable_mem" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            sorts=$(echo "$sorts" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            
            if [ -n "$sql_id" ]; then
                # 检查sql_id是否已经在sqltext_dict中（与Python版本一致：if sql_id not in self.sqltext_dict）
                local existing_sqltext=$(get_raw_dict_value "$data_file" "sqltext_dict" "$sql_id")
                local command_name=""
                
                if [ -z "$existing_sqltext" ]; then
                    # 如果不在dict中，则获取SQL文本和命令名称
                    local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                    local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                    command_name=$(echo "$sqltext_info" | cut -d'|' -f2 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                    
                    if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                        sqltext="已刷出共享池"
                        command_name="已刷出共享池"
                    fi
                    
                    # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                    update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                else
                    # 如果已在dict中，从dict中获取（与Python版本一致）
                    command_name=$(get_raw_dict_value "$data_file" "sql_command_dict" "$sql_id")
                fi
                
                # 处理NULL值
                [ -z "$executions" ] && executions="0"
                [ -z "$parse_calls" ] && parse_calls="0"
                [ -z "$version_count" ] && version_count="0"
                [ -z "$sharable_mem" ] && sharable_mem="0"
                [ -z "$sorts" ] && sorts="0"
                [ -z "$command_name" ] && command_name="-"
                
                # 写入TSV格式数据：SQL_ID\tEXECUTIONS\tPARSE_CALLS\tVERSION_COUNT\tSHARABLE_MEM\tSORTS\tCOMMAND_NAME
                append_raw_data_line "$data_file" "high_parse_count_sql" "$sql_id|$executions|$parse_calls|$version_count|$sharable_mem|$sorts|$command_name"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "high_parse_count_sql"
        
        log INFO "高解析次数SQL数据采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取高解析次数SQL数据"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:high_parse_count_sql===" "$data_file"; then
            write_raw_data "$data_file" "high_parse_count_sql" ""
        fi
    fi
}

# 采集共享池中高版本数SQL数据
collect_high_version_count_sql() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "采集共享池中高版本数SQL数据..."
    
    local sql="select * from (select sql_id,version_count,executions,parse_calls,sharable_mem,sorts from v\$sqlarea where version_count > 50 order by 2 desc) where rownum<11"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:high_version_count_sql===" "$data_file"; then
            echo "===SECTION:high_version_count_sql===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "high_version_count_sql" "SQL_ID|VERSION_COUNT|EXECUTIONS|PARSE_CALLS|SHARABLE_MEM|SORTS|COMMAND_NAME"
            log DEBUG "写入原始数据: section=high_version_count_sql, 文件=$data_file"
        fi
        
        local record_count=0
        while IFS='|' read -r sql_id version_count executions parse_calls sharable_mem sorts; do
            sql_id=$(echo "$sql_id" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            version_count=$(echo "$version_count" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            executions=$(echo "$executions" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            parse_calls=$(echo "$parse_calls" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            sharable_mem=$(echo "$sharable_mem" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            sorts=$(echo "$sorts" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            
            if [ -n "$sql_id" ]; then
                # 检查sql_id是否已经在sqltext_dict中（与Python版本一致：if sql_id not in self.sqltext_dict）
                local existing_sqltext=$(get_raw_dict_value "$data_file" "sqltext_dict" "$sql_id")
                local command_name=""
                
                if [ -z "$existing_sqltext" ]; then
                    # 如果不在dict中，则获取SQL文本和命令名称
                    local sqltext_info=$(get_sqltext "$conn_str" "$sql_id")
                    local sqltext=$(echo "$sqltext_info" | cut -d'|' -f1 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                    command_name=$(echo "$sqltext_info" | cut -d'|' -f2 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                    
                    if [ -z "$sqltext" ] || [ "$sqltext" = "NULL" ]; then
                        sqltext="已刷出共享池"
                        command_name="已刷出共享池"
                    fi
                    
                    # 动态更新sqltext_dict和sql_command_dict（与Python版本一致）
                    update_sqltext_dict "$data_file" "$sql_id" "$sqltext" "$command_name"
                else
                    # 如果已在dict中，从dict中获取（与Python版本一致）
                    command_name=$(get_raw_dict_value "$data_file" "sql_command_dict" "$sql_id")
                fi
                
                # 处理NULL值
                [ -z "$version_count" ] && version_count="0"
                [ -z "$executions" ] && executions="0"
                [ -z "$parse_calls" ] && parse_calls="0"
                [ -z "$sharable_mem" ] && sharable_mem="0"
                [ -z "$sorts" ] && sorts="0"
                [ -z "$command_name" ] && command_name="-"
                
                # 写入TSV格式数据：SQL_ID\tVERSION_COUNT\tEXECUTIONS\tPARSE_CALLS\tSHARABLE_MEM\tSORTS\tCOMMAND_NAME
                append_raw_data_line "$data_file" "high_version_count_sql" "$sql_id|$version_count|$executions|$parse_calls|$sharable_mem|$sorts|$command_name"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "high_version_count_sql"
        
        log INFO "高版本数SQL数据采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取高版本数SQL数据"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:high_version_count_sql===" "$data_file"; then
            write_raw_data "$data_file" "high_version_count_sql" ""
        fi
    fi
}

# 采集REDO日志切换信息
collect_redo_log_switch_info() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集REDO日志切换信息..."
    
    local sql="select count(*) from v\$log_history where first_time > TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS')"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        local count=$(normalize_number "$(echo "$result" | grep -v "COUNT" | head -1 | xargs)")
        
        if [ -n "$count" ]; then
            # 添加到metric_data数组（存储到内存，格式：metric_id|avg_value|max_value|min_value|percentile_95）
            # 注意：只有 value 字段，转换为 avg_value，其他字段为空
            append_metric_data_to_memory "2180515|$count|||"
            
            log INFO "REDO日志切换信息采集完成，日志切换次数: $count"
        else
            log WARNING "未能获取REDO日志切换次数"
        fi
    else
        log WARNING "未能获取REDO日志切换信息"
    fi
}

# 采集共享池SQL执行次数大于1的SQL比例和占用共享池内存比例
collect_sql_with_executions_info() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "采集共享池SQL执行次数大于1的SQL比例和占用共享池内存比例..."
    
    # SQL1: 计算SQL执行次数大于1的SQL比例
    local sql1="select round(sql_exec_gt_1_count/total_sql_count*100,2) from (
     SELECT 
        COUNT(*) AS total_sql_count,
        count(CASE WHEN executions > 1 THEN 1 END) AS sql_exec_gt_1_count
    FROM v\$sql
    WHERE executions > 0
      AND parsing_schema_id NOT IN (0, 1, 2))"
    
    local result1=$(execute_sql "$sql1" "$conn_str")
    if [ -n "$result1" ] && ! echo "$result1" | grep -qiE "ORA-|SP2-|ERROR"; then
        local sql_with_executions_ratio=$(normalize_number "$(echo "$result1" | grep -v "ROUND" | head -1 | xargs)")
        
        if [ -n "$sql_with_executions_ratio" ]; then
            # 追加到metric_data数组（存储到内存，格式：metric_id|avg_value|max_value|min_value|percentile_95）
            # 注意：只有 value 字段，转换为 avg_value，其他字段为空
            append_metric_data_to_memory "2189201|$sql_with_executions_ratio|||"
            
            log INFO "共享池中SQL执行次数大于1的SQL比例: $sql_with_executions_ratio%"
        fi
    else
        log WARNING "未能获取共享池SQL执行次数大于1的SQL比例"
    fi
    
    # SQL2: 计算SQL执行次数大于1的SQL占用共享池内存比例
    local sql2="select round(mem_exec_gt_1/total_sharable_mem*100,2) from (   
    SELECT 
        SUM(sharable_mem) AS total_sharable_mem,
        SUM(CASE WHEN executions > 1 THEN sharable_mem ELSE 0 END) AS mem_exec_gt_1
    FROM v\$sql
    WHERE executions > 0
      AND parsing_schema_id NOT IN (0, 1, 2)
      AND sharable_mem > 0)"
    
    local result2=$(execute_sql "$sql2" "$conn_str")
    if [ -n "$result2" ] && ! echo "$result2" | grep -qiE "ORA-|SP2-|ERROR"; then
        local memory_for_sql_with_executions_ratio=$(normalize_number "$(echo "$result2" | grep -v "ROUND" | head -1 | xargs)")
        
        if [ -n "$memory_for_sql_with_executions_ratio" ]; then
            # 追加到metric_data数组（存储到内存，格式：metric_id|avg_value|max_value|min_value|percentile_95）
            # 注意：只有 value 字段，转换为 avg_value，其他字段为空
            append_metric_data_to_memory "2189202|$memory_for_sql_with_executions_ratio|||"
            
            log INFO "共享池SQL执行次数大于1的SQL占用共享池内存比例: $memory_for_sql_with_executions_ratio%"
        fi
    else
        log WARNING "未能获取共享池SQL执行次数大于1的SQL占用共享池内存比例"
    fi
    
    log INFO "共享池SQL执行次数信息采集完成"
}

# 采集REDO nowait信息
collect_redo_nowait() {
    local conn_str=$1
    local data_file=$2
    local snap_id=$3
    local instance_number=$4
    local is_reboot=$5
    
    log INFO "采集REDO nowait信息..."
    
    local sql=""
    if [ "$is_reboot" != "true" ] && [ -n "$snap_id" ] && [ -n "$instance_number" ]; then
        sql="select round((1-(t1.value/t2.value))*100,2) redo_nowait from (select (t1.value-t2.value) value from v\$sysstat t1, dba_hist_sysstat t2 where t1.name=t2.stat_name and t1.name = 'redo buffer allocation retries' and snap_id=$snap_id and instance_number=$instance_number) t1,
    (select (t1.value-t2.value) value from v\$sysstat t1, dba_hist_sysstat t2 where t1.name=t2.stat_name and t1.name = 'redo entries' and snap_id=$snap_id and instance_number=$instance_number) t2"
    else
        sql="select round((1-(t1.value/t2.value))*100,2) redo_nowait from (select t1.value value from v\$sysstat t1 where t1.name= 'redo buffer allocation retries') t1,
    (select t1.value value from v\$sysstat t1 where t1.name = 'redo entries') t2"
    fi
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        local redo_nowait=$(normalize_number "$(echo "$result" | grep -v "REDO_NOWAIT" | head -1 | xargs)")
        
        if [ -n "$redo_nowait" ]; then
            # 追加到metric_data数组（存储到内存，格式：metric_id|avg_value|max_value|min_value|percentile_95）
            # 注意：只有 value 字段，转换为 avg_value，其他字段为空
            append_metric_data_to_memory "2189203|$redo_nowait|||"
            
            log INFO "REDO nowait信息采集完成，REDO nowait比例: $redo_nowait%"
        else
            log WARNING "未能获取REDO nowait值"
        fi
    else
        log WARNING "未能获取REDO nowait信息"
    fi
}

# 采集enq序列数据
collect_enq_sequence_data() {
    local conn_str=$1
    local data_file=$2
    local snap_id=$3
    local instance_number=$4
    local is_reboot=$5
    
    log INFO "采集enq序列数据..."
    
    local sql=""
    if [ "$is_reboot" != "true" ] && [ -n "$snap_id" ] && [ -n "$instance_number" ]; then
        sql="SELECT (t1.total_req#-t2.total_req#),(t1.succ_req#-t2.succ_req#),(t1.failed_req#-t2.failed_req#),(t1.total_wait#-t2.total_wait#),(t1.cum_wait_time-t2.cum_wait_time) FROM V\$ENQUEUE_STAT t1, dba_hist_enqueue_stat t2 WHERE t1.eq_type=t2.eq_type and t1.eq_type='SQ' and t2.snap_id=$snap_id and t2.instance_number=$instance_number"
    else
        sql="SELECT total_req#,succ_req#,failed_req#,total_wait#,cum_wait_time FROM V\$ENQUEUE_STAT WHERE EQ_TYPE='SQ' "
    fi
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        while IFS='|' read -r total_reqs succ_reqs failed_reqs total_waits cum_wait_time; do
            total_reqs=$(echo "$total_reqs" | xargs)
            succ_reqs=$(echo "$succ_reqs" | xargs)
            failed_reqs=$(echo "$failed_reqs" | xargs)
            total_waits=$(echo "$total_waits" | xargs)
            cum_wait_time=$(echo "$cum_wait_time" | xargs)
            
            # 处理NULL值
            [ -z "$total_reqs" ] && total_reqs="0"
            [ -z "$succ_reqs" ] && succ_reqs="0"
            [ -z "$failed_reqs" ] && failed_reqs="0"
            [ -z "$total_waits" ] && total_waits="0"
            [ -z "$cum_wait_time" ] && cum_wait_time="0"
            
            if [ "$total_reqs" -gt 0 ]; then
                # 计算平均等待时间（cum_wait_time单位是毫秒，需要转换为秒）
                local cum_wait_time_sec=$(awk "BEGIN {printf \"%.2f\", $cum_wait_time / 1000}")
                local avg_wait_time="0"
                if [ "$total_waits" -gt 0 ]; then
                    avg_wait_time=$(awk "BEGIN {printf \"%.2f\", $cum_wait_time_sec / $total_waits}")
                fi
            fi
        done <<< "$result"
        
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:enq_sequence_data===" "$data_file"; then
            echo "===SECTION:enq_sequence_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "enq_sequence_data" "NAME|TOTAL_REQS|SUCC_REQS|FAILED_REQS|TOTAL_WAITS|CUM_WAIT_TIME|AVG_WAIT_TIME"
            log DEBUG "写入原始数据: section=enq_sequence_data, 文件=$data_file"
        fi
        
        local record_count=0
        while IFS='|' read -r total_reqs succ_reqs failed_reqs total_waits cum_wait_time; do
            total_reqs=$(echo "$total_reqs" | xargs)
            succ_reqs=$(echo "$succ_reqs" | xargs)
            failed_reqs=$(echo "$failed_reqs" | xargs)
            total_waits=$(echo "$total_waits" | xargs)
            cum_wait_time=$(echo "$cum_wait_time" | xargs)
            
            # 处理NULL值
            [ -z "$total_reqs" ] && total_reqs="0"
            [ -z "$succ_reqs" ] && succ_reqs="0"
            [ -z "$failed_reqs" ] && failed_reqs="0"
            [ -z "$total_waits" ] && total_waits="0"
            [ -z "$cum_wait_time" ] && cum_wait_time="0"
            
            if [ "$total_reqs" -gt 0 ]; then
                # 计算平均等待时间（cum_wait_time单位是毫秒，需要转换为秒）
                local cum_wait_time_sec=$(awk "BEGIN {printf \"%.2f\", $cum_wait_time / 1000}")
                local avg_wait_time="0"
                if [ "$total_waits" -gt 0 ]; then
                    avg_wait_time=$(awk "BEGIN {printf \"%.2f\", $cum_wait_time_sec / $total_waits}")
                fi
                
                # 写入TSV格式数据：NAME\tTOTAL_REQS\tSUCC_REQS\tFAILED_REQS\tTOTAL_WAITS\tCUM_WAIT_TIME\tAVG_WAIT_TIME
                append_raw_data_line "$data_file" "enq_sequence_data" "SQ-Sequence Cache|$total_reqs|$succ_reqs|$failed_reqs|$total_waits|$cum_wait_time_sec|$avg_wait_time"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "enq_sequence_data"
        
        log INFO "enq序列数据采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取enq序列数据"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:enq_sequence_data===" "$data_file"; then
            write_raw_data "$data_file" "enq_sequence_data" ""
        fi
    fi
}

# 采集dc序列数据
collect_dc_sequence_data() {
    local conn_str=$1
    local data_file=$2
    local snap_id=$3
    local instance_number=$4
    local is_reboot=$5
    
    log INFO "采集dc序列数据..."
    
    local sql=""
    if [ "$is_reboot" != "true" ] && [ -n "$snap_id" ] && [ -n "$instance_number" ]; then
        sql="select PARAMETER,gets,getmisses,ROUND(getmisses / case when gets = 0 then 1 else gets end * 100, 3) AS miss_pct,modifications from 
(select t1.PARAMETER,(t1.gets-t2.gets) gets,(t1.getmisses-t2.getmisses) getmisses,(t1.modifications-t2.modifications) modifications from v\$rowcache t1,dba_hist_rowcache_summary t2 where t1.parameter=t2.parameter and t1.parameter='dc_sequences' and t2.snap_id=$snap_id and t2.instance_number=$instance_number)"
    else
        sql="SELECT parameter, gets, getmisses,
       ROUND(getmisses / gets * 100, 3) AS miss_pct,
       modifications
FROM v\$rowcache
WHERE parameter='dc_sequences' "
    fi
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        while IFS='|' read -r name gets getmisses miss_pct modifications; do
            name=$(echo "$name" | xargs)
            gets=$(echo "$gets" | xargs)
            getmisses=$(echo "$getmisses" | xargs)
            miss_pct=$(echo "$miss_pct" | xargs)
            modifications=$(echo "$modifications" | xargs)
            
            if [ -n "$name" ]; then
                
                # 处理NULL值并规范化数字格式
                [ -z "$gets" ] && gets="0"
                [ -z "$getmisses" ] && getmisses="0"
                miss_pct=$(normalize_number "$miss_pct")
                [ -z "$miss_pct" ] && miss_pct="0"
                [ -z "$modifications" ] && modifications="0"
                
                # 转义特殊字符
                name=$(echo "$name" | sed 's/"/\\"/g')
            fi
        done <<< "$result"
        
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:dc_sequence_data===" "$data_file"; then
            echo "===SECTION:dc_sequence_data===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "dc_sequence_data" "NAME|GETS|GETMISSES|MISS_PCT|MODIFICATIONS"
            log DEBUG "写入原始数据: section=dc_sequence_data, 文件=$data_file"
        fi
        
        local record_count=0
        while IFS='|' read -r name gets getmisses miss_pct modifications; do
            name=$(echo "$name" | xargs)
            gets=$(echo "$gets" | xargs)
            getmisses=$(echo "$getmisses" | xargs)
            miss_pct=$(echo "$miss_pct" | xargs)
            modifications=$(echo "$modifications" | xargs)
            
            if [ -n "$name" ]; then
                # 处理NULL值并规范化数字格式
                [ -z "$gets" ] && gets="0"
                [ -z "$getmisses" ] && getmisses="0"
                miss_pct=$(normalize_number "$miss_pct")
                [ -z "$miss_pct" ] && miss_pct="0"
                [ -z "$modifications" ] && modifications="0"
                
                # 写入TSV格式数据：NAME\tGETS\tGETMISSES\tMISS_PCT\tMODIFICATIONS
                append_raw_data_line "$data_file" "dc_sequence_data" "$name|$gets|$getmisses|$miss_pct|$modifications"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "dc_sequence_data"
        
        log INFO "dc序列数据采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取dc序列数据"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:dc_sequence_data===" "$data_file"; then
            write_raw_data "$data_file" "dc_sequence_data" ""
        fi
    fi
}

# 采集表空间IO数据
collect_tablespace_io_data() {
    local conn_str=$1
    local data_file=$2
    local snap_id=$3
    local instance_number=$4
    local is_reboot=$5
    local ash_begin_time=$6
    local is_rac=$7
    
    log INFO "采集表空间IO数据..."
    
    # 计算时间差（秒）
    local current_time=$(date +%s)
    local ash_time_epoch=$(date -d "$ash_begin_time" +%s 2>/dev/null || echo "$current_time")
    local delta_time=$((current_time - ash_time_epoch))
    if [ "$delta_time" -le 0 ]; then
        delta_time=1
    fi
    
    local sql=""
    if [ "$is_rac" = "true" ]; then
        # RAC环境
        if [ "$is_reboot" != "true" ] && [ -n "$snap_id" ] && [ -n "$instance_number" ]; then
            sql="SELECT t1.con_id, t1.name, t1.phyrds-t2.phyrds, t1.phywrts-t2.phywrts,
       t1.phyblkrd-t2.phyblkrd, t1.phyblkwrt-t2.phyblkwrt,
       t1.singleblkrds-t2.singleblkrds, t1.readtim-t2.readtim,
       t1.writetim-t2.writetim, t1.singleblkrdtim-t2.singleblkrdtim
FROM (
    SELECT t1.con_id, t2.name, SUM(phyrds) phyrds, SUM(phywrts) phywrts,
           SUM(phyblkrd) phyblkrd, SUM(phyblkwrt) phyblkwrt,
           SUM(singleblkrds) singleblkrds, SUM(readtim*10) readtim,
           SUM(writetim*10) writetim, SUM(singleblkrdtim*10) singleblkrdtim
    FROM v\$datafile t1, v\$tablespace t2, V\$FILESTAT t3
    WHERE t1.ts#=t2.ts# AND t1.con_id=t2.con_id
    AND t3.file#=t1.file# AND t1.con_id=t3.con_id
    GROUP BY t1.con_id, t2.name
    ORDER BY 1, 3 DESC) t1,
    (SELECT con_id, tsname, SUM(phyrds) phyrds, SUM(phywrts) phywrts,
            SUM(phyblkrd) phyblkrd, SUM(phyblkwrt) phyblkwrt,
            SUM(singleblkrds) singleblkrds, SUM(readtim*10) readtim,
            SUM(writetim*10) writetim, SUM(singleblkrdtim*10) singleblkrdtim
     FROM dba_hist_filestatxs
     WHERE snap_id=$snap_id AND instance_number=$instance_number
     GROUP BY con_id, tsname
     ORDER BY 1, 3 DESC) t2
WHERE t1.name=t2.tsname AND t1.con_id=t2.con_id
ORDER BY 1, 3 DESC"
        else
            sql="SELECT t1.con_id, t2.name, SUM(phyrds) phyrds, SUM(phywrts) phywrts,
       SUM(phyblkrd) phyblkrd, SUM(phyblkwrt) phyblkwrt,
       SUM(singleblkrds) singleblkrds, SUM(readtim*10) readtim,
       SUM(writetim*10) writetim, SUM(singleblkrdtim*10) singleblkrdtim
FROM v\$datafile t1, v\$tablespace t2, V\$FILESTAT t3
WHERE t1.ts#=t2.ts# AND t1.con_id=t2.con_id
AND t3.file#=t1.file# AND t1.con_id=t3.con_id
GROUP BY t1.con_id, t2.name
ORDER BY 1, 3 DESC"
        fi
    else
        # 非RAC环境
        if [ "$is_reboot" != "true" ] && [ -n "$snap_id" ] && [ -n "$instance_number" ]; then
            sql="SELECT t1.tablespace_name, t1.phyrds-t2.phyrds, t1.phywrts-t2.phywrts,
       t1.phyblkrd-t2.phyblkrd, t1.phyblkwrt-t2.phyblkwrt,
       t1.singleblkrds-t2.singleblkrds, t1.readtim-t2.readtim,
       t1.writetim-t2.writetim, t1.singleblkrdtim-t2.singleblkrdtim
FROM (
    SELECT tablespace_name, SUM(phyrds) phyrds, SUM(phywrts) phywrts,
           SUM(phyblkrd) phyblkrd, SUM(phyblkwrt) phyblkwrt,
           SUM(singleblkrds) singleblkrds, SUM(readtim*10) readtim,
           SUM(writetim*10) writetim, SUM(singleblkrdtim*10) singleblkrdtim
    FROM V\$FILESTAT t1, dba_data_files t2
    WHERE t1.file#=t2.file_id
    GROUP BY tablespace_name) t1,
    (SELECT tsname, SUM(phyrds) phyrds, SUM(phywrts) phywrts,
            SUM(phyblkrd) phyblkrd, SUM(phyblkwrt) phyblkwrt,
            SUM(singleblkrds) singleblkrds, SUM(readtim*10) readtim,
            SUM(writetim*10) writetim, SUM(singleblkrdtim*10) singleblkrdtim
     FROM dba_hist_filestatxs
     WHERE snap_id=$snap_id AND instance_number=$instance_number
     GROUP BY tsname) t2
WHERE t1.tablespace_name=t2.tsname
ORDER BY 2 DESC"
        else
            sql="SELECT tablespace_name, SUM(phyrds), SUM(phywrts), SUM(phyblkrd),
       SUM(phyblkwrt), SUM(singleblkrds), SUM(readtim*10), SUM(writetim*10),
       SUM(singleblkrdtim*10)
FROM V\$FILESTAT t1, dba_data_files t2
WHERE t1.file#=t2.file_id
GROUP BY tablespace_name"
        fi
    fi
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:tablespace_io_data===" "$data_file"; then
            echo "===SECTION:tablespace_io_data===" >> "$data_file"
            # 写入表头（根据RAC环境动态调整）
            if [ "$is_rac" = "true" ]; then
                append_raw_data_line "$data_file" "tablespace_io_data" "CON_ID|TABLESPACE_NAME|PHYRDS|PHYRDS_PER_SEC|PHYBLKRD_PER_READ|AVG_READ_TIME|SINGLEBLKRDS_PER_SEC|AVG_SINGLE_BLOCK_READ_TIME|PHYWRTS|PHYWRTS_PER_SEC|PHYBLKWRT_PER_WRITE|AVG_WRITE_TIME"
            else
                append_raw_data_line "$data_file" "tablespace_io_data" "TABLESPACE_NAME|PHYRDS|PHYRDS_PER_SEC|PHYBLKRD_PER_READ|AVG_READ_TIME|SINGLEBLKRDS_PER_SEC|AVG_SINGLE_BLOCK_READ_TIME|PHYWRTS|PHYWRTS_PER_SEC|PHYBLKWRT_PER_WRITE|AVG_WRITE_TIME"
            fi
            log DEBUG "写入原始数据: section=tablespace_io_data, 文件=$data_file"
        fi
        
        local record_count=0
        if [ "$is_rac" = "true" ]; then
            # RAC环境：con_id, name, phyrds, phywrts, ...
            while IFS='|' read -r con_id tablespace_name phyrds phywrts phyblkrd phyblkwrt singleblkrds readtim writetim singleblkrdtim; do
                con_id=$(echo "$con_id" | xargs)
                tablespace_name=$(echo "$tablespace_name" | xargs)
                phyrds=$(echo "$phyrds" | xargs)
                phywrts=$(echo "$phywrts" | xargs)
                phyblkrd=$(echo "$phyblkrd" | xargs)
                phyblkwrt=$(echo "$phyblkwrt" | xargs)
                singleblkrds=$(echo "$singleblkrds" | xargs)
                readtim=$(echo "$readtim" | xargs)
                writetim=$(echo "$writetim" | xargs)
                singleblkrdtim=$(echo "$singleblkrdtim" | xargs)
                
                # 处理NULL值
                [ -z "$phyrds" ] && phyrds="0"
                [ -z "$phywrts" ] && phywrts="0"
                [ -z "$phyblkrd" ] && phyblkrd="0"
                [ -z "$phyblkwrt" ] && phyblkwrt="0"
                [ -z "$singleblkrds" ] && singleblkrds="0"
                [ -z "$readtim" ] && readtim="0"
                [ -z "$writetim" ] && writetim="0"
                [ -z "$singleblkrdtim" ] && singleblkrdtim="0"
                
                if [ -n "$tablespace_name" ]; then
                    # 计算衍生指标
                    local phyrds_per_sec=$(awk "BEGIN {printf \"%.2f\", $phyrds / $delta_time}")
                    local phywrts_per_sec=$(awk "BEGIN {printf \"%.2f\", $phywrts / $delta_time}")
                    local phyblkrd_per_read="0"
                    if [ "$phyrds" -gt 0 ]; then
                        phyblkrd_per_read=$(awk "BEGIN {printf \"%.2f\", $phyblkrd / $phyrds}")
                    fi
                    local phyblkwrt_per_write="0"
                    if [ "$phywrts" -gt 0 ]; then
                        phyblkwrt_per_write=$(awk "BEGIN {printf \"%.2f\", $phyblkwrt / $phywrts}")
                    fi
                    local singleblkrds_per_sec=$(awk "BEGIN {printf \"%.2f\", $singleblkrds / $delta_time}")
                    local avg_read_time="0"
                    if [ "$phyrds" -gt 0 ]; then
                        avg_read_time=$(awk "BEGIN {printf \"%.2f\", $readtim / $phyrds}")
                    fi
                    local avg_write_time="0"
                    if [ "$phywrts" -gt 0 ]; then
                        avg_write_time=$(awk "BEGIN {printf \"%.2f\", $writetim / $phywrts}")
                    fi
                    local avg_single_block_read_time="0"
                    if [ "$phyrds" -gt 0 ]; then
                        avg_single_block_read_time=$(awk "BEGIN {printf \"%.2f\", $singleblkrdtim / $phyrds}")
                    fi
                    
                    # 写入TSV格式数据：CON_ID\tTABLESPACE_NAME\tPHYRDS\tPHYRDS_PER_SEC\tPHYBLKRD_PER_READ\tAVG_READ_TIME\tSINGLEBLKRDS_PER_SEC\tAVG_SINGLE_BLOCK_READ_TIME\tPHYWRTS\tPHYWRTS_PER_SEC\tPHYBLKWRT_PER_WRITE\tAVG_WRITE_TIME
                    append_raw_data_line "$data_file" "tablespace_io_data" "$con_id|$tablespace_name|$phyrds|$phyrds_per_sec|$phyblkrd_per_read|$avg_read_time|$singleblkrds_per_sec|$avg_single_block_read_time|$phywrts|$phywrts_per_sec|$phyblkwrt_per_write|$avg_write_time"
                    record_count=$((record_count + 1))
                fi
            done <<< "$result"
        else
            # 非RAC环境：tablespace_name, phyrds, phywrts, ...
            while IFS='|' read -r tablespace_name phyrds phywrts phyblkrd phyblkwrt singleblkrds readtim writetim singleblkrdtim; do
                tablespace_name=$(echo "$tablespace_name" | xargs)
                phyrds=$(echo "$phyrds" | xargs)
                phywrts=$(echo "$phywrts" | xargs)
                phyblkrd=$(echo "$phyblkrd" | xargs)
                phyblkwrt=$(echo "$phyblkwrt" | xargs)
                singleblkrds=$(echo "$singleblkrds" | xargs)
                readtim=$(echo "$readtim" | xargs)
                writetim=$(echo "$writetim" | xargs)
                singleblkrdtim=$(echo "$singleblkrdtim" | xargs)
                
                # 处理NULL值
                [ -z "$phyrds" ] && phyrds="0"
                [ -z "$phywrts" ] && phywrts="0"
                [ -z "$phyblkrd" ] && phyblkrd="0"
                [ -z "$phyblkwrt" ] && phyblkwrt="0"
                [ -z "$singleblkrds" ] && singleblkrds="0"
                [ -z "$readtim" ] && readtim="0"
                [ -z "$writetim" ] && writetim="0"
                [ -z "$singleblkrdtim" ] && singleblkrdtim="0"
                
                if [ -n "$tablespace_name" ]; then
                    # 计算衍生指标
                    local phyrds_per_sec=$(awk "BEGIN {printf \"%.2f\", $phyrds / $delta_time}")
                    local phywrts_per_sec=$(awk "BEGIN {printf \"%.2f\", $phywrts / $delta_time}")
                    local phyblkrd_per_read="0"
                    if [ "$phyrds" -gt 0 ]; then
                        phyblkrd_per_read=$(awk "BEGIN {printf \"%.2f\", $phyblkrd / $phyrds}")
                    fi
                    local phyblkwrt_per_write="0"
                    if [ "$phywrts" -gt 0 ]; then
                        phyblkwrt_per_write=$(awk "BEGIN {printf \"%.2f\", $phyblkwrt / $phywrts}")
                    fi
                    local singleblkrds_per_sec=$(awk "BEGIN {printf \"%.2f\", $singleblkrds / $delta_time}")
                    local avg_read_time="0"
                    if [ "$phyrds" -gt 0 ]; then
                        avg_read_time=$(awk "BEGIN {printf \"%.2f\", $readtim / $phyrds}")
                    fi
                    local avg_write_time="0"
                    if [ "$phywrts" -gt 0 ]; then
                        avg_write_time=$(awk "BEGIN {printf \"%.2f\", $writetim / $phywrts}")
                    fi
                    local avg_single_block_read_time="0"
                    if [ "$phyrds" -gt 0 ]; then
                        avg_single_block_read_time=$(awk "BEGIN {printf \"%.2f\", $singleblkrdtim / $phyrds}")
                    fi
                    
                    # 写入TSV格式数据：TABLESPACE_NAME\tPHYRDS\tPHYRDS_PER_SEC\tPHYBLKRD_PER_READ\tAVG_READ_TIME\tSINGLEBLKRDS_PER_SEC\tAVG_SINGLE_BLOCK_READ_TIME\tPHYWRTS\tPHYWRTS_PER_SEC\tPHYBLKWRT_PER_WRITE\tAVG_WRITE_TIME
                    append_raw_data_line "$data_file" "tablespace_io_data" "$tablespace_name|$phyrds|$phyrds_per_sec|$phyblkrd_per_read|$avg_read_time|$singleblkrds_per_sec|$avg_single_block_read_time|$phywrts|$phywrts_per_sec|$phyblkwrt_per_write|$avg_write_time"
                    record_count=$((record_count + 1))
                fi
            done <<< "$result"
        fi
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "tablespace_io_data"
        
        log INFO "表空间IO数据采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取表空间IO数据"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:tablespace_io_data===" "$data_file"; then
            write_raw_data "$data_file" "tablespace_io_data" ""
        fi
    fi
}

# 采集表统计信息过旧检查数据
collect_tab_statitics_stale_data() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "采集表统计信息过旧检查数据..."
    
    # SQL1: 统计过旧统计信息的表数量
    local sql1="SELECT COUNT(*) FROM dba_tab_statistics
WHERE stale_stats='YES' AND stattype_locked IS NULL"
    
    local result1=$(execute_sql "$sql1" "$conn_str")
    local count="0"
    if [ -n "$result1" ] && ! echo "$result1" | grep -qiE "ORA-|SP2-|ERROR"; then
        count=$(echo "$result1" | grep -v "COUNT" | head -1 | xargs)
    fi
    
    # SQL2: 获取过旧统计信息的表详情（前20条）
    local sql2="SELECT * FROM (
SELECT owner, table_name, num_rows, to_char(last_analyzed, 'YYYY-MM-DD HH24:MI:SS') as last_analyzed, stale_stats
FROM dba_tab_statistics
WHERE stale_stats='YES' AND stattype_locked IS NULL
ORDER BY last_analyzed)
WHERE rownum < 21"
    
    local result2=$(execute_sql "$sql2" "$conn_str")
    
    # 写入section开始标记（如果不存在）
    if [ ! -f "$data_file" ] || ! grep -q "===SECTION:tab_statitics_stale_data===" "$data_file"; then
        echo "===SECTION:tab_statitics_stale_data===" >> "$data_file"
        # 写入COUNT值（键值对格式）
        append_raw_data_line "$data_file" "tab_statitics_stale_data" "COUNT|$count"
        # 写入表头（注意：第一行是COUNT键值对，后面是表数据）
        append_raw_data_line "$data_file" "tab_statitics_stale_data" "OWNER|TABLE_NAME|NUM_ROWS|LAST_ANALYZED|STALE_STATS"
        log DEBUG "写入原始数据: section=tab_statitics_stale_data, 文件=$data_file"
    fi
    
    # 写入表列表数据
    if [ -n "$result2" ] && ! echo "$result2" | grep -qiE "ORA-|SP2-|ERROR"; then
        while IFS='|' read -r owner table_name num_rows last_analyzed stale_stats; do
            owner=$(echo "$owner" | xargs)
            table_name=$(echo "$table_name" | xargs)
            num_rows=$(echo "$num_rows" | xargs)
            last_analyzed=$(echo "$last_analyzed" | xargs)
            stale_stats=$(echo "$stale_stats" | xargs)
            
            if [ -n "$owner" ] && [ -n "$table_name" ]; then
                # 处理NULL值
                [ -z "$num_rows" ] && num_rows="0"
                [ -z "$last_analyzed" ] && last_analyzed=""
                [ -z "$stale_stats" ] && stale_stats="YES"
                
                # 写入TSV格式数据：OWNER\tTABLE_NAME\tNUM_ROWS\tLAST_ANALYZED\tSTALE_STATS
                append_raw_data_line "$data_file" "tab_statitics_stale_data" "$owner|$table_name|$num_rows|$last_analyzed|$stale_stats"
            fi
        done <<< "$result2"
    fi
    
    # 写入section结束标记
    end_raw_data_section "$data_file" "tab_statitics_stale_data"
    
    log INFO "表统计信息过旧检查数据采集完成，过旧表数量: $count"
}

# 采集SGA的RESIZE信息
collect_sga_resize_info() {
    local conn_str=$1
    local data_file=$2
    local ash_begin_time=$3
    
    log INFO "采集SGA的RESIZE信息..."
    
    local sql="SELECT component, oper_type, oper_mode, parameter,
       initial_size, target_size, final_size, status,
       start_time, end_time
FROM v\$sga_resize_ops
WHERE start_time > TO_DATE('$ash_begin_time', 'YYYY-MM-DD HH24:MI:SS')"
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:sga_resize_info===" "$data_file"; then
            echo "===SECTION:sga_resize_info===" >> "$data_file"
            # 写入表头
            append_raw_data_line "$data_file" "sga_resize_info" "COMPONENT|OPER_TYPE|OPER_MODE|PARAMETER|INITIAL_SIZE|TARGET_SIZE|FINAL_SIZE|STATUS|START_TIME|END_TIME"
            log DEBUG "写入原始数据: section=sga_resize_info, 文件=$data_file"
        fi
        
        local record_count=0
        while IFS='|' read -r component oper_type oper_mode parameter initial_size target_size final_size status start_time end_time; do
            component=$(echo "$component" | xargs)
            oper_type=$(echo "$oper_type" | xargs)
            oper_mode=$(echo "$oper_mode" | xargs)
            parameter=$(echo "$parameter" | xargs)
            initial_size=$(echo "$initial_size" | xargs)
            target_size=$(echo "$target_size" | xargs)
            final_size=$(echo "$final_size" | xargs)
            status=$(echo "$status" | xargs)
            start_time=$(echo "$start_time" | xargs)
            end_time=$(echo "$end_time" | xargs)
            
            if [ -n "$component" ]; then
                # 处理NULL值
                [ -z "$initial_size" ] && initial_size="0"
                [ -z "$target_size" ] && target_size="0"
                [ -z "$final_size" ] && final_size="0"
                
                # 写入TSV格式数据：COMPONENT\tOPERATION\tOPER_MODE\tPARAMETER\tINITIAL_SIZE\tTARGET_SIZE\tFINAL_SIZE\tSTATUS\tSTART_TIME\tEND_TIME
                append_raw_data_line "$data_file" "sga_resize_info" "$component|$oper_type|$oper_mode|$parameter|$initial_size|$target_size|$final_size|$status|$start_time|$end_time"
                record_count=$((record_count + 1))
            fi
        done <<< "$result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "sga_resize_info"
        
        log INFO "SGA的RESIZE信息采集完成，共采集 $record_count 条记录"
    else
        log WARNING "未能获取SGA的RESIZE信息"
        # 即使没有结果也写入空段
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:sga_resize_info===" "$data_file"; then
            write_raw_data "$data_file" "sga_resize_info" ""
        fi
    fi
}

# 主Oracle采集函数
collect_oracle_data() {
    local conn_str=$1
    local data_file=$2
    
    log INFO "========== 开始Oracle数据库采集 =========="
    # 1. 获取ASH开始时间
    get_ash_begin_time "$conn_str" "$data_file"
    
    # 2. 获取用户名字典（保存在内存中）
    get_username_dict "$conn_str" || {
        log WARN "获取用户名字典失败，将继续执行（用户名将显示为'-'）"
    }
    # 注意：数据库日志文件路径已在main函数中获取并验证，这里不再重复获取
    
    # 3. 采集各种数据（按照原有采集顺序）
    # 使用全局变量（已在get_ash_begin_time中设置）
    # 使用 || true 允许单个采集函数失败而不影响其他采集（因为脚本设置了 set -e）
    collect_latch_data "$conn_str" "$data_file" "$SNAP_ID" "$INSTANCE_NUMBER" "$IS_REBOOT" || true
    collect_time_model_info "$conn_str" "$data_file" "$SNAP_ID" "$INSTANCE_NUMBER" "$IS_REBOOT" || true
    collect_metric_info "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_db_settings "$conn_str" "$data_file" || true
    collect_event_info "$conn_str" "$data_file" "$SNAP_ID" "$INSTANCE_NUMBER" "$IS_REBOOT" || true
    collect_capacity_info "$conn_str" "$data_file" || true
    collect_db_recovery_area_info "$conn_str" "$data_file" || true
    collect_long_transaction "$conn_str" "$data_file" || true
    collect_blocked_chain "$conn_str" "$data_file" || true
    collect_history_session_info "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_active_session_info "$conn_str" "$data_file" || true
    collect_higher_elapsed_time_sql "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_higher_executions_sql "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_io_type_data "$conn_str" "$data_file" "$SNAP_ID" "$INSTANCE_NUMBER" "$IS_REBOOT" "$ASH_BEGIN_TIME" || true
    collect_table_scan_data "$conn_str" "$data_file" "$SNAP_ID" "$INSTANCE_NUMBER" "$IS_REBOOT" || true
    collect_redo_log_info "$conn_str" "$data_file" || true
    collect_top_redo_wait_sql "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_hot_block_sql "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_top_latch_wait_sql "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_top_enq_wait_sql "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_top_seq_wait_sql "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_undo_stats_history "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_undo_tbs_usage "$conn_str" "$data_file" || true
    collect_undo_wait_sql_data "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_rac_sql_data "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_top_io_wait_sql "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_buffer_cache_sql_data "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_dbwr_activity_data "$conn_str" "$data_file" "$SNAP_ID" "$INSTANCE_NUMBER" "$IS_REBOOT" "$ASH_BEGIN_TIME" || true
    collect_top_shared_pool_wait_sql "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_high_parse_count_sql "$conn_str" "$data_file" || true
    collect_high_version_count_sql "$conn_str" "$data_file" || true
    collect_redo_log_switch_info "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_sql_with_executions_info "$conn_str" "$data_file" || true
    collect_redo_nowait "$conn_str" "$data_file" "$SNAP_ID" "$INSTANCE_NUMBER" "$IS_REBOOT" || true
    collect_enq_sequence_data "$conn_str" "$data_file" "$SNAP_ID" "$INSTANCE_NUMBER" "$IS_REBOOT" || true
    collect_dc_sequence_data "$conn_str" "$data_file" "$SNAP_ID" "$INSTANCE_NUMBER" "$IS_REBOOT" || true
    collect_tablespace_io_data "$conn_str" "$data_file" "$SNAP_ID" "$INSTANCE_NUMBER" "$IS_REBOOT" "$ASH_BEGIN_TIME" "$IS_RAC" || true
    collect_rac_statistics_data "$conn_str" "$data_file" "$SNAP_ID" "$INSTANCE_NUMBER" "$IS_REBOOT" "$ASH_BEGIN_TIME" || true
    collect_sga_resize_info "$conn_str" "$data_file" "$ASH_BEGIN_TIME" || true
    collect_tab_statitics_stale_data "$conn_str" "$data_file" || true
    collect_expired_user_info "$conn_str" "$data_file" || true
    collect_user_with_dba_privilege "$conn_str" "$data_file" || true
    collect_invalid_object_info "$conn_str" "$data_file" || true
    
    # 注意：原脚本还有更多采集函数（如collect_top_latch_wait_sql, collect_top_enq_wait_sql等）
    # 这些函数可以按照相同模式继续添加
    
    # 在Oracle采集结束时，将内存中的sqltext_dict和sql_command_dict写入文件
    flush_sqltext_dicts_to_file "$data_file"
    
    log INFO "========== Oracle数据库采集完成 =========="
}

# 保存Oracle数据
save_oracle_data() {
    local data_file=$1
    local output_dir=$2
    
    mkdir -p "$output_dir"
    local output_file="$output_dir/oracle_data_once.txt"
    
    if [ -f "$data_file" ]; then
        cp "$data_file" "$output_file"
        log INFO "Oracle数据已保存到: $output_file"
    else
        log ERROR "数据文件不存在: $data_file"
    fi
}

# ==================== 指标ID映射 ====================

# OS指标名称到ID的映射
declare -A OS_METRIC_NAME_MAP=(
    ["CPU使用率"]=3000003
    ["running process"]=3000009
    ["cpu num"]=3000010
    ["blocked process"]=3000013
    ["内存使用率"]=3000014
    ["IO Latency"]=3000006
    ["meminfo详细信息"]=3000065
    ["IOPS"]=3000100
    ["IOKBPS"]=3000101
    ["Swap使用率"]=3001031
    ["user time(%)"]=3001010
    ["system time(%)"]=3001011
    ["idle time(%)"]=3001013
    ["wait time(%)"]=3001014
    ["nice time(%)"]=3001012
    ["所有网络接口每秒丢包错包数"]=3000200
    ["网络接口接收速率（字节/秒）"]=3000204
    ["网络接口发送速率（字节/秒）"]=3000206
    ["所有网络接口总速率（字节/秒）"]=3000208
)

# Oracle指标名称到ID的映射
declare -A ORACLE_METRIC_NAME_MAP=(
    ["Buffer Cache Hit Ratio"]=2189000
    ["Redo Allocation Hit Ratio"]=2189002
    ["Physical Reads Per Sec"]=2189004
    ["Physical Writes Per Sec"]=2189006
    ["Physical Reads Direct Per Sec"]=2189008
    ["Physical Writes Direct Per Sec"]=2189010
    ["Redo Generated Per Sec"]=2189016
    ["Logons Per Sec"]=2189018
    ["User Transaction Per Sec"]=2189003
    ["Logical Reads Per Sec"]=2189030
    ["Leaf Node Splits Per Sec"]=2189083
    ["Branch Node Splits Per Sec"]=2189085
    ["Shared Pool Free %"]=2189114
    ["Average Active Sessions"]=2189147
    ["Executions Per Sec"]=2189121
    ["Hard Parse Count Per Sec"]=2189046
    ["Soft Parse Ratio"]=2189055
    ["Full Index Scans Per Sec"]=2189040
    ["Total Table Scans Per Sec"]=2189038
    ["Long Table Scans Per Sec"]=2189036
    ["Total Parse Count Per Sec"]=2189044
    ["Parse Failure Count Per Sec"]=2189048
    ["Session Limit %"]=2189119
    ["Memory Sorts Ratio"]=2189001
    ["Library Cache Hit Ratio"]=2189112
    ["Cursor Cache Hit Ratio"]=2189050
    ["Execute Without Parse Ratio"]=2189054
    ["Enq Blocked Sessions Count"]=2180518
    ["last hour log switch count"]=2180515
    ["% SQL with executions>1"]=2189201
    ["% Memory for SQL w/exec>1"]=2189202
    ["redo nowait"]=2189203
)

# 获取指标ID（根据指标名称）
get_metric_id() {
    local metric_name=$1
    local metric_type=${2:-"os"}  # os 或 oracle
    
    if [ "$metric_type" = "os" ]; then
        if [ -n "${OS_METRIC_NAME_MAP[$metric_name]}" ]; then
            echo "${OS_METRIC_NAME_MAP[$metric_name]}"
            return 0
        fi
    elif [ "$metric_type" = "oracle" ]; then
        if [ -n "${ORACLE_METRIC_NAME_MAP[$metric_name]}" ]; then
            echo "${ORACLE_METRIC_NAME_MAP[$metric_name]}"
            return 0
        fi
    fi
    
    # 如果找不到映射，返回空（使用原名称）
    echo ""
    return 1
}

# ==================== OS采集函数 ====================

# 采集CPU信息
collect_cpu_info() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    local data_file=$6
    
    log INFO "采集CPU信息..."
    
    # 使用sar命令获取CPU使用率
    local cmd_cpu="sar -u 1 3 2>/dev/null | grep Average | awk '{print 100-\$NF}'"
    local cpu_result=$(execute_ssh "$host" "$port" "$username" "$password" "$ssh_key_path" "$cmd_cpu")
    
    if [ -n "$cpu_result" ]; then
        # 清理并验证cpu_usage
        local cleaned_cpu_usage=$(echo "$cpu_result" | grep -oE '[0-9]+\.?[0-9]*' | head -1 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        local cpu_usage=0
        if [[ "$cleaned_cpu_usage" =~ ^[0-9]+\.?[0-9]*$ ]]; then
            cpu_usage="$cleaned_cpu_usage"
        else
            log WARNING "无法解析CPU使用率，使用默认值0"
        fi
        
        # 获取CPU详细信息
        local cmd_cpu_detail="sar -u 1 3 2>/dev/null | grep Average | awk '{print \$3, \$4, \$5, \$6, \$8}'"
        local cpu_detail=$(execute_ssh "$host" "$port" "$username" "$password" "$ssh_key_path" "$cmd_cpu_detail")
        
        # 使用指标ID获取指标ID
        local cpu_usage_id=$(get_metric_id "CPU使用率" "os")
        local cpu_user_id=$(get_metric_id "user time(%)" "os")
        local cpu_system_id=$(get_metric_id "system time(%)" "os")
        local cpu_idle_id=$(get_metric_id "idle time(%)" "os")
        local cpu_iowait_id=$(get_metric_id "wait time(%)" "os")
        local cpu_nice_id=$(get_metric_id "nice time(%)" "os")
        
        # 使用关联数组存储数据（替代JSON）
        declare -A cpu_metrics
        
        # CPU使用率（3000003）
        if [ -n "$cpu_usage_id" ]; then
            cpu_metrics["$cpu_usage_id"]="$cpu_usage"
        fi
        
        if [ -n "$cpu_detail" ]; then
            local cpu_user=$(echo "$cpu_detail" | awk '{print $1}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            local cpu_system=$(echo "$cpu_detail" | awk '{print $2}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            local cpu_idle=$(echo "$cpu_detail" | awk '{print $3}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            local cpu_iowait=$(echo "$cpu_detail" | awk '{print $4}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            local cpu_nice=$(echo "$cpu_detail" | awk '{print $5}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            
            # 验证并添加CPU详细信息（使用指标ID）
            if [[ "$cpu_user" =~ ^[0-9]+\.?[0-9]*$ ]] && [ -n "$cpu_user_id" ]; then
                cpu_metrics["$cpu_user_id"]="$cpu_user"
            fi
            
            if [[ "$cpu_system" =~ ^[0-9]+\.?[0-9]*$ ]] && [ -n "$cpu_system_id" ]; then
                cpu_metrics["$cpu_system_id"]="$cpu_system"
            fi
            
            if [[ "$cpu_idle" =~ ^[0-9]+\.?[0-9]*$ ]] && [ -n "$cpu_idle_id" ]; then
                cpu_metrics["$cpu_idle_id"]="$cpu_idle"
            fi
            
            if [[ "$cpu_iowait" =~ ^[0-9]+\.?[0-9]*$ ]] && [ -n "$cpu_iowait_id" ]; then
                cpu_metrics["$cpu_iowait_id"]="$cpu_iowait"
            fi
            
            if [[ "$cpu_nice" =~ ^[0-9]+\.?[0-9]*$ ]] && [ -n "$cpu_nice_id" ]; then
                cpu_metrics["$cpu_nice_id"]="$cpu_nice"
            fi
        fi
        
        # 保存到临时文件（键值对格式：指标ID:值，每行一个）
        local cpu_metrics_file="${TMP_DIR}/cpu_info.dict"
        > "$cpu_metrics_file"  # 清空文件
        for metric_id in "${!cpu_metrics[@]}"; do
            echo "${metric_id}:${cpu_metrics[$metric_id]}" >> "$cpu_metrics_file"
        done
    fi
}

# 采集内存信息
collect_memory_info() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    local data_file=$6
    
    log INFO "采集内存信息..."
    
    local cmd_mem="free -m"
    local mem_result=$(execute_ssh "$host" "$port" "$username" "$password" "$ssh_key_path" "$cmd_mem")
    
    if [ -n "$mem_result" ]; then
        # 解析内存信息
        local mem_total=$(echo "$mem_result" | grep "^Mem:" | awk '{print $2}')
        local mem_used=$(echo "$mem_result" | grep "^Mem:" | awk '{print $3}')
        local mem_available=$(echo "$mem_result" | grep "^Mem:" | awk '{print $7}')
        
        if [ -n "$mem_total" ] && [ -n "$mem_available" ]; then
            local mem_usage=$(awk "BEGIN {printf \"%.2f\", ($mem_total - $mem_available) / $mem_total * 100}")
            # 使用指标ID存储内存使用率（3000014）
            local mem_usage_id=$(get_metric_id "内存使用率" "os")
            # 使用关联数组存储数据（替代JSON）
            declare -A mem_metrics
            if [ -n "$mem_usage_id" ]; then
                mem_metrics["$mem_usage_id"]="$mem_usage"
            else
                # 如果找不到映射，使用原名称
                mem_metrics["mem_usage"]="$mem_usage"
            fi
            
            # 保存到临时文件（键值对格式：指标ID:值，每行一个）
            local mem_metrics_file="${TMP_DIR}/mem_info.dict"
            > "$mem_metrics_file"  # 清空文件
            for metric_id in "${!mem_metrics[@]}"; do
                echo "${metric_id}:${mem_metrics[$metric_id]}" >> "$mem_metrics_file"
            done
        fi
    fi
}

# 采集运行队列和阻塞队列信息
collect_runqueue_info() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    local data_file=$6
    
    log INFO "采集运行队列和阻塞队列信息..."
    
    # 使用vmstat命令获取运行队列(r)和阻塞队列(b)
    local cmd_runqueue="vmstat 1 3 | tail -1 | awk '{print \$1, \$2}'"
    local runqueue_result=$(execute_ssh "$host" "$port" "$username" "$password" "$ssh_key_path" "$cmd_runqueue")
    
    if [ -n "$runqueue_result" ]; then
        # 解析运行队列和阻塞队列数据
        # vmstat输出格式：r b（运行队列 阻塞队列）
        local runqueue_usage_str=$(echo "$runqueue_result" | awk '{print $1}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        local blocked_queue_usage_str=$(echo "$runqueue_result" | awk '{print $2}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        
        local runqueue_usage=""
        local blocked_queue_usage=""
        
        # 使用关联数组存储数据（替代JSON）
        declare -A runqueue_metrics
        
        # 解析运行队列值
        if [ -n "$runqueue_usage_str" ] && [[ "$runqueue_usage_str" =~ ^[0-9]+\.?[0-9]*$ ]]; then
            runqueue_usage="$runqueue_usage_str"
            # 使用指标ID存储运行队列（3000009）
            local runqueue_id=$(get_metric_id "running process" "os")
            if [ -n "$runqueue_id" ]; then
                runqueue_metrics["$runqueue_id"]="$runqueue_usage"
            else
                runqueue_metrics["running_process"]="$runqueue_usage"
            fi
        else
            log WARNING "无法解析运行队列值: $runqueue_usage_str"
        fi
        
        # 解析阻塞队列值
        if [ -n "$blocked_queue_usage_str" ] && [[ "$blocked_queue_usage_str" =~ ^[0-9]+\.?[0-9]*$ ]]; then
            blocked_queue_usage="$blocked_queue_usage_str"
            # 使用指标ID存储阻塞队列（3000013）
            local blocked_id=$(get_metric_id "blocked process" "os")
            if [ -n "$blocked_id" ]; then
                runqueue_metrics["$blocked_id"]="$blocked_queue_usage"
            else
                runqueue_metrics["blocked_process"]="$blocked_queue_usage"
            fi
        else
            log WARNING "无法解析阻塞队列值: $blocked_queue_usage_str"
        fi
        
        # 保存到临时文件，后续合并到OS数据文件
        if [ ${#runqueue_metrics[@]} -gt 0 ]; then
            local runqueue_metrics_file="${TMP_DIR}/runqueue_info.dict"
            > "$runqueue_metrics_file"  # 清空文件
            for metric_id in "${!runqueue_metrics[@]}"; do
                echo "${metric_id}:${runqueue_metrics[$metric_id]}" >> "$runqueue_metrics_file"
            done
            log INFO "运行队列: ${runqueue_usage:-无法获取}, 阻塞队列: ${blocked_queue_usage:-无法获取}"
        else
            log WARNING "未能获取有效的运行队列和阻塞队列数据"
        fi
    else
        log WARNING "无法获取运行队列和阻塞队列信息"
    fi
}

# 采集Swap信息
collect_swap_info() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    local data_file=$6
    
    log INFO "采集Swap信息..."
    
    local cmd_swap="free -m | grep Swap"
    local swap_result=$(execute_ssh "$host" "$port" "$username" "$password" "$ssh_key_path" "$cmd_swap")
    # echo $swap_result
    if [ -n "$swap_result" ]; then
        local swap_total=$(echo "$swap_result" | awk '{print $2}')
        local swap_used=$(echo "$swap_result" | awk '{print $3}')
        
        if [ -n "$swap_total" ] && [ "$swap_total" -gt 0 ]; then
            local swap_usage=$(awk "BEGIN {printf \"%.2f\", $swap_used / $swap_total * 100}")
            # 使用指标ID存储Swap使用率（3001031）
            local swap_usage_id=$(get_metric_id "Swap使用率" "os")
            # 使用关联数组存储数据（替代JSON）
            declare -A swap_metrics
            if [ -n "$swap_usage_id" ]; then
                swap_metrics["$swap_usage_id"]="$swap_usage"
            else
                # 如果找不到映射，使用原名称
                swap_metrics["swap_usage"]="$swap_usage"
            fi
            
            # 保存到临时文件（键值对格式：指标ID:值，每行一个）
            local swap_metrics_file="${TMP_DIR}/swap_info.dict"
            > "$swap_metrics_file"  # 清空文件
            for metric_id in "${!swap_metrics[@]}"; do
                echo "${metric_id}:${swap_metrics[$metric_id]}" >> "$swap_metrics_file"
            done
        fi
    fi
}

# 采集IO统计信息
collect_io_stats() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    local data_file=$6
    
    log INFO "采集IO统计信息..."
    
    local cmd_iostat="iostat -x 1 3 2>/dev/null | grep -A 100 'Device' | tail -n +2"
    local iostat_result=$(execute_ssh "$host" "$port" "$username" "$password" "$ssh_key_path" "$cmd_iostat")
    
    if [ -n "$iostat_result" ]; then
        # 计算平均IOPS和IOKBPS
        local total_iops=0
        local total_iokbps=0
        local count=0
        
        while IFS= read -r line; do
            if [ -n "$line" ] && ! echo "$line" | grep -qE "^(Device|avg-cpu)"; then
                local r_iops=$(echo "$line" | awk '{print $4}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                local w_iops=$(echo "$line" | awk '{print $5}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                local r_kbps=$(echo "$line" | awk '{print $6}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                local w_kbps=$(echo "$line" | awk '{print $7}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                
                # 验证所有值都是数字，并设置默认值
                if [[ "$r_iops" =~ ^[0-9]+\.?[0-9]*$ ]] && [[ "$w_iops" =~ ^[0-9]+\.?[0-9]*$ ]] && \
                   [[ "$r_kbps" =~ ^[0-9]+\.?[0-9]*$ ]] && [[ "$w_kbps" =~ ^[0-9]+\.?[0-9]*$ ]]; then
                    # 使用awk的-v选项安全传递变量
                    total_iops=$(awk -v total="$total_iops" -v r="$r_iops" -v w="$w_iops" 'BEGIN {print total + r + w}')
                    total_iokbps=$(awk -v total="$total_iokbps" -v r="$r_kbps" -v w="$w_kbps" 'BEGIN {print total + r + w}')
                    count=$((count + 1))
                fi
            fi
        done <<< "$iostat_result"
        
        if [ "$count" -gt 0 ]; then
            local avg_iops=$(awk -v total="$total_iops" -v cnt="$count" 'BEGIN {printf "%.2f", total / cnt}')
            local avg_iokbps=$(awk -v total="$total_iokbps" -v cnt="$count" 'BEGIN {printf "%.2f", total / cnt}')
            # 使用指标ID存储IOPS和IOKBPS（3000100, 3000101）
            local iops_id=$(get_metric_id "IOPS" "os")
            local iokbps_id=$(get_metric_id "IOKBPS" "os")
            # 使用关联数组存储数据（替代JSON）
            declare -A io_metrics
            if [ -n "$iops_id" ] && [ -n "$iokbps_id" ]; then
                io_metrics["$iops_id"]="$avg_iops"
                io_metrics["$iokbps_id"]="$avg_iokbps"
            else
                io_metrics["avg_iops"]="$avg_iops"
                io_metrics["avg_iokbps"]="$avg_iokbps"
            fi
            
            # 保存到临时文件（键值对格式：指标ID:值，每行一个）
            local io_metrics_file="${TMP_DIR}/io_stats.dict"
            > "$io_metrics_file"  # 清空文件
            for metric_id in "${!io_metrics[@]}"; do
                echo "${metric_id}:${io_metrics[$metric_id]}" >> "$io_metrics_file"
            done
        fi
    fi
}

# 采集IO响应时间（await）
collect_io_await() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    local data_file=$6
    
    log INFO "采集IO响应时间（await）..."
    
    # 使用iostat获取平均IO响应时间
    local cmd_io_await="iostat -x 1 3 2>/dev/null | grep -A 100 'Device' | tail -n +2 | awk '{sum_await+=\$10; count++} END {if(count>0) print sum_await/count; else print 0}'"
    local io_await_result=$(execute_ssh "$host" "$port" "$username" "$password" "$ssh_key_path" "$cmd_io_await")
    
    if [ -n "$io_await_result" ]; then
        # 清理并验证io_await值
        local cleaned_io_await=$(echo "$io_await_result" | grep -oE '[0-9]+\.?[0-9]*' | head -1 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        local io_await_val=0
        
        if [[ "$cleaned_io_await" =~ ^[0-9]+\.?[0-9]*$ ]]; then
            # 保留两位小数
            io_await_val=$(awk "BEGIN {printf \"%.2f\", $cleaned_io_await}")
        else
            log WARNING "无法解析IO响应时间，使用默认值0"
        fi
        
        # 使用指标ID存储IO Latency（3000006）
        local io_await_id=$(get_metric_id "IO Latency" "os")
        # 使用关联数组存储数据（替代JSON）
        declare -A io_await_metrics
        if [ -n "$io_await_id" ]; then
            io_await_metrics["$io_await_id"]="$io_await_val"
        else
            # 如果找不到映射，使用原名称
            io_await_metrics["io_latency"]="$io_await_val"
        fi
        
        # 保存到临时文件（键值对格式：指标ID:值，每行一个）
        local io_await_metrics_file="${TMP_DIR}/io_await_info.dict"
        > "$io_await_metrics_file"  # 清空文件
        for metric_id in "${!io_await_metrics[@]}"; do
            echo "${metric_id}:${io_await_metrics[$metric_id]}" >> "$io_await_metrics_file"
        done
        log INFO "平均IO响应时间(await): ${io_await_val} ms"
    else
        log WARNING "无法获取IO响应时间信息"
    fi
}

# 采集meminfo信息
collect_meminfo() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    local data_file=$6
    
    log INFO "采集meminfo信息..."
    
    # 获取meminfo信息
    local cmd_meminfo="cat /proc/meminfo"
    local meminfo_result=$(execute_ssh "$host" "$port" "$username" "$password" "$ssh_key_path" "$cmd_meminfo")
    local meminfo_id=$(get_metric_id "meminfo详细信息" "os")

    if [ -n "$meminfo_result" ]; then
        # 转换为JSON字典格式（与Python版本一致：使用指标ID作为键，字典格式保存）
        # Python版本：self.os_metrics[timestamp][metric_id][item[0]] = item[1]
        # 格式：{"MemTotal": "12345678 kB", "MemFree": "1234567 kB", ...}
        local meminfo_dict="{"
        local first=true
        
        while IFS= read -r line; do
            if [ -n "$line" ]; then
                # 按冒号分割（与Python版本一致：line_list = line.strip().split(':')）
                local key=$(echo "$line" | cut -d':' -f1 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                local value=$(echo "$line" | cut -d':' -f2- | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                
                if [ -n "$key" ] && [ -n "$value" ]; then
                    if [ "$first" = true ]; then
                        first=false
                    else
                        meminfo_dict="$meminfo_dict,"
                    fi
                    
                    # 转义特殊字符（与Python版本一致）
                    key=$(echo "$key" | sed 's/"/\\"/g')
                    value=$(echo "$value" | sed 's/"/\\"/g')
                    
                    # 构建JSON字典（与Python版本一致：字典格式）
                    # Python版本：self.os_metrics[timestamp][metric_id][item[0]] = item[1]
                    # 格式：{"key": "value"}
                    meminfo_dict="$meminfo_dict\"$key\":\"$value\""
                fi
            fi
        done <<< "$meminfo_result"
        
        meminfo_dict="$meminfo_dict}"
        
        # 写入到os_data_once.txt文件
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:$meminfo_id===" "$data_file"; then
            echo "===SECTION:$meminfo_id===" >> "$data_file"
        fi
        
        # 将meminfo字典转换为键值对格式写入文件
        while IFS= read -r line; do
            if [ -n "$line" ]; then
                # 按冒号分割
                local key=$(echo "$line" | cut -d':' -f1 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                local value=$(echo "$line" | cut -d':' -f2- | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                if [ -n "$key" ] && [ -n "$value" ]; then
                    append_raw_data_line "$data_file" "$meminfo_id" "$key|$value"
                fi
            fi
        done <<< "$meminfo_result"
        
        end_raw_data_section "$data_file" "$meminfo_id"
        
        log INFO "meminfo信息采集完成"
    else
        log WARNING "无法获取meminfo信息"
        # 即使没有结果也写入空段（包含表头）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:$meminfo_id===" "$data_file"; then
            echo "===SECTION:$meminfo_id===" >> "$data_file"
            append_raw_data_line "$data_file" "$meminfo_id" ""
            end_raw_data_section "$data_file" "$meminfo_id"
        fi
    fi
}

# 采集操作系统日志
collect_os_log() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    local data_file=$6
    
    log INFO "采集操作系统日志..."
    
    # 获取操作系统日志（与Python版本一致：tail -n 10000 /var/log/messages，过滤错误关键词）
    local cmd_os_log='tail -n 10000 /var/log/messages 2>/dev/null | grep -iE "(error|warn|critical|failed|fail|fatal|panic)"'
    local os_log_result=$(execute_ssh "$host" "$port" "$username" "$password" "$ssh_key_path" "$cmd_os_log")
    
    if [ -n "$os_log_result" ]; then
        # 保存操作系统日志（原始文本格式）
        write_raw_data "$data_file" "操作系统日志" "$os_log_result"
        log INFO "操作系统日志采集完成"
    else
        # 即使没有结果也保存空字符串
        write_raw_data "$data_file" "操作系统日志" ""
        log INFO "操作系统日志采集完成（未发现错误信息）"
    fi
}

# 分析数据库日志错误（与Python版本一致）
analyze_db_log_errors() {
    local log_content=$1
    
    if [ -z "$log_content" ]; then
        echo ""
        return
    fi
    
    # 用于存储每个错误码的统计和样本
    declare -A error_counts
    declare -A error_samples
    
    # 按行处理日志内容
    while IFS= read -r line || [ -n "$line" ]; do
        if [ -z "$line" ]; then
            continue
        fi
        
        # 查找ORA-错误码（格式：ORA-数字，如ORA-00600, ORA-00942）
        # 使用grep提取所有ORA-错误码（不区分大小写）
        local ora_errors=$(echo "$line" | grep -ioE 'ORA-[0-9]{5}')
        if [ -n "$ora_errors" ]; then
            while IFS= read -r error_code; do
                if [ -n "$error_code" ]; then
                    # 统一转换为大写
                    error_code=$(echo "$error_code" | tr '[:lower:]' '[:upper:]')
                    # 使用默认值避免unbound variable错误
                    if [ -z "${error_samples[$error_code]:-}" ]; then
                        # 保存第一个样本，截取前500个字符避免过长
                        local sample="${line:0:500}"
                        error_samples["$error_code"]="$sample"
                    fi
                    error_counts["$error_code"]=$((${error_counts[$error_code]:-0} + 1))
                fi
            done <<< "$ora_errors"
        fi
        
        # 查找TNS-错误码（格式：TNS-数字，如TNS-12535, TNS-12541）
        # 使用grep提取所有TNS-错误码（不区分大小写）
        local tns_errors=$(echo "$line" | grep -ioE 'TNS-[0-9]{5}')
        if [ -n "$tns_errors" ]; then
            while IFS= read -r error_code; do
                if [ -n "$error_code" ]; then
                    # 统一转换为大写
                    error_code=$(echo "$error_code" | tr '[:lower:]' '[:upper:]')
                    # 使用默认值避免unbound variable错误
                    if [ -z "${error_samples[$error_code]:-}" ]; then
                        # 保存第一个样本，截取前500个字符避免过长
                        local sample="${line:0:500}"
                        error_samples["$error_code"]="$sample"
                    fi
                    error_counts["$error_code"]=$((${error_counts[$error_code]:-0} + 1))
                fi
            done <<< "$tns_errors"
        fi
    done <<< "$log_content"
    
    # 如果没有错误，返回空字符串
    if [ ${#error_counts[@]} -eq 0 ]; then
        echo ""
        return
    fi
    
    # 构建文本格式：错误码|出现次数|样本
    # 按错误码排序
    local sorted_errors=$(printf '%s\n' "${!error_counts[@]}" | sort)
    
    while IFS= read -r error_code; do
        if [ -n "$error_code" ]; then
            local count=${error_counts[$error_code]}
            local sample="${error_samples[$error_code]:-未找到样本}"
            # 输出格式：错误码|出现次数|样本（使用|分隔）
            echo "$error_code|$count|$sample"
        fi
    done <<< "$sorted_errors"
}

# 获取数据库日志文件路径（与Python版本一致）
get_db_log_path() {
    local conn_str=$1
    
    # 日志输出到标准错误，避免被命令替换捕获
    log INFO "获取数据库日志文件路径..." >&2
    
    local db_log_file_path=""
    
    # 获取数据库版本
    local sql_version="SELECT version FROM v\$instance"
    local version_result=$(execute_sql "$sql_version" "$conn_str")
    local version="11"
    
    if [ -n "$version_result" ] && ! echo "$version_result" | grep -qiE "ORA-|SP2-|ERROR"; then
        version=$(echo "$version_result" | grep -v "VERSION" | head -1 | xargs)
        # 提取版本号（如"11.2.0.4.0" -> "11"）
        version=$(echo "$version" | cut -d'.' -f1)
    fi
    
    # 根据版本选择不同的SQL查询
    local sql=""
    if [ "$version" -ge 11 ] 2>/dev/null; then
        sql="SELECT value||'/alert_'||instance_name||'.log'
FROM v\$diag_info t1, v\$instance t2
WHERE name = 'Diag Trace'"
    else
        sql="SELECT value||'/alert_'||instance_name||'.log'
FROM v\$parameter t1, v\$instance t2
WHERE name = 'background_dump_dest'"
    fi
    
    local result=$(execute_sql "$sql" "$conn_str")
    if [ -n "$result" ] && ! echo "$result" | grep -qiE "ORA-|SP2-|ERROR"; then
        db_log_file_path=$(echo "$result" | grep -v "SELECT\|FROM\|WHERE" | head -1 | xargs)
        if [ -n "$db_log_file_path" ]; then
            # 日志输出到标准错误，避免被命令替换捕获
            log INFO "数据库日志文件路径: $db_log_file_path" >&2
            # 只输出路径到标准输出
            echo "$db_log_file_path"
            return 0
        fi
    fi
    
    # 日志输出到标准错误，避免被命令替换捕获
    log WARNING "数据库日志文件路径查询未返回数据或查询失败" >&2
    echo ""
    return 1
}

# 采集数据库日志
collect_db_log() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    local data_file=$6
    local db_log_file_path=$7
    
    log INFO "采集数据库日志..."
    
    if [ -z "$db_log_file_path" ]; then
        log WARNING "数据库日志文件路径为空，跳过数据库日志采集"
        # 写入到os_data_once.txt文件（空内容）
        write_raw_data "$data_file" "数据库日志" ""
        return
    fi
    
    # 清理路径，移除可能的换行符、空格和日志信息
    db_log_file_path=$(echo "$db_log_file_path" | tr -d '\n\r' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | grep -v "^\[INFO\]" | grep -v "^\[WARNING\]" | grep -v "^\[ERROR\]" | head -1)
    
    if [ -z "$db_log_file_path" ]; then
        log WARNING "数据库日志文件路径为空或无效，跳过数据库日志采集"
        # 写入到os_data_once.txt文件（空内容）
        write_raw_data "$data_file" "数据库日志" ""
        return
    fi
    
    # 获取数据库日志（与Python版本一致：tail -n 10000，过滤ORA-和TNS-错误）
    local cmd_db_log="tail -n 500 '$db_log_file_path' 2>/dev/null | grep -iE 'ORA-|TNS-'"
    echo $cmd_db_log
    local db_log_result=$(execute_ssh "$host" "$port" "$username" "$password" "$ssh_key_path" "$cmd_db_log")
    if [ -n "$db_log_result" ]; then
        # 分析日志中的错误码（与Python版本一致）
        local error_stats=$(analyze_db_log_errors "$db_log_result")
        # 检查日志大小，如果超过500K则不保存到文件（与Python版本一致）
        local log_size_bytes=$(echo -n "$db_log_result" | wc -c)
        local max_size_bytes=$((500 * 1024))  # 500K = 500 * 1024 字节
        if [ "$log_size_bytes" -le "$max_size_bytes" ]; then
            # 日志大小不超过500K，保存原始日志内容（与Python版本一致）
            # 写入到os_data_once.txt文件（原始日志内容）
            write_raw_data "$data_file" "数据库日志" "$db_log_result"
        else
            # 日志大小超过500K，不保存原始日志，但记录提示信息（与Python版本一致）
            log INFO "数据库日志大小超过500K (${log_size_bytes}字节)，跳过保存到文件"
            # 写入到os_data_once.txt文件（提示信息）
            write_raw_data "$data_file" "数据库日志" "[提示：日志内容过大(${log_size_bytes}字节)，已跳过保存到文件]"
        fi
        
        # 保存错误统计结果到os_data_once.txt文件（无论日志大小如何，都保存统计结果）
        if [ -n "$error_stats" ]; then
            # 写入到os_data_once.txt文件
            if [ ! -f "$data_file" ] || ! grep -q "===SECTION:数据库日志错误统计===" "$data_file"; then
                echo "===SECTION:数据库日志错误统计===" >> "$data_file"
                # 写入表头（使用|分隔）
                append_raw_data_line "$data_file" "数据库日志错误统计" "错误码|出现次数|样本"
            fi
            # 写入错误统计数据（每行一个错误码）
            while IFS= read -r error_line; do
                if [ -n "$error_line" ]; then
                    append_raw_data_line "$data_file" "数据库日志错误统计" "$error_line"
                fi
            done <<< "$error_stats"
            end_raw_data_section "$data_file" "数据库日志错误统计"
        else
            # 即使没有错误统计也写入空段（包含表头）
            if [ ! -f "$data_file" ] || ! grep -q "===SECTION:数据库日志错误统计===" "$data_file"; then
                echo "===SECTION:数据库日志错误统计===" >> "$data_file"
                append_raw_data_line "$data_file" "数据库日志错误统计" "错误码|出现次数|样本"
                end_raw_data_section "$data_file" "数据库日志错误统计"
            fi
        fi
        
        log INFO "数据库日志采集完成"
    else
        # 即使没有结果也保存空字符串（与Python版本一致）
        # 写入到os_data_once.txt文件（空内容）
        write_raw_data "$data_file" "数据库日志" ""
        log INFO "数据库日志采集完成（未发现错误信息）"
    fi
}

# 采集文件系统信息（排除只读文件系统）
collect_fs_info() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    local data_file=$6
    
    log INFO "采集文件系统信息..."
    
    # 首先获取所有只读文件系统的挂载点
    local readonly_mounts=()
    local cmd_mount="mount | grep -E '\\(ro| ro |,ro| ro,| ro\\)' | awk '{print \$3}'"
    local mount_result=$(execute_ssh "$host" "$port" "$username" "$password" "$ssh_key_path" "$cmd_mount")
    
    if [ -n "$mount_result" ]; then
        while IFS= read -r mount_point; do
            mount_point=$(echo "$mount_point" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            if [ -n "$mount_point" ]; then
                readonly_mounts+=("$mount_point")
            fi
        done <<< "$mount_result"
        log DEBUG "找到 ${#readonly_mounts[@]} 个只读文件系统挂载点"
    fi
    
    # 获取文件系统信息
    local cmd_fs="df -h"
    local fs_result=$(execute_ssh "$host" "$port" "$username" "$password" "$ssh_key_path" "$cmd_fs")
    
    if [ -n "$fs_result" ]; then
        # 写入section开始标记（如果不存在）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:文件系统信息===" "$data_file"; then
            echo "===SECTION:文件系统信息===" >> "$data_file"
            log DEBUG "写入原始数据: section=文件系统信息, 文件=$data_file"
        fi
        
        local fs_count=0
        while IFS= read -r line; do
            if [ -n "$line" ] && ! echo "$line" | grep -qE "^(Filesystem|文件系统)"; then
                # 解析df -h的输出行
                # 使用awk解析，处理可能包含空格的挂载点
                local fs_name=$(echo "$line" | awk '{print $1}')
                local fs_size=$(echo "$line" | awk '{print $2}')
                local fs_used=$(echo "$line" | awk '{print $3}')
                local fs_avail=$(echo "$line" | awk '{print $4}')
                local fs_use_pct=$(echo "$line" | awk '{print $5}')
                # 挂载点从第6个字段开始到末尾（处理包含空格的情况）
                local fs_mount=$(echo "$line" | awk '{for(i=6;i<=NF;i++) printf "%s ", $i; print ""}' | sed 's/[[:space:]]*$//')
                
                # 检查是否有足够的字段（至少6个：文件系统、总容量、已用、可用、使用率、挂载点）
                if [ -n "$fs_name" ] && [ -n "$fs_size" ] && [ -n "$fs_used" ] && [ -n "$fs_avail" ] && [ -n "$fs_use_pct" ] && [ -n "$fs_mount" ]; then
                    # 检查是否为只读文件系统
                    local is_readonly=false
                    for readonly_mount in "${readonly_mounts[@]}"; do
                        if [ "$fs_mount" = "$readonly_mount" ]; then
                            is_readonly=true
                            break
                        fi
                    done
                    
                    # 跳过只读文件系统
                    if [ "$is_readonly" = false ]; then
                        # 写入原始数据行（使用|分隔）
                        append_raw_data_line "$data_file" "文件系统信息" "$fs_name|$fs_size|$fs_used|$fs_avail|$fs_use_pct|$fs_mount"
                        fs_count=$((fs_count + 1))
                    else
                        log DEBUG "跳过只读文件系统: $fs_mount"
                    fi
                fi
            fi
        done <<< "$fs_result"
        
        # 写入section结束标记
        end_raw_data_section "$data_file" "文件系统信息"
        log INFO "文件系统信息采集完成（已排除只读文件系统，共采集 $fs_count 条记录）"
    else
        log WARNING "无法获取文件系统信息"
        # 即使没有结果也写入空段（包含表头）
        if [ ! -f "$data_file" ] || ! grep -q "===SECTION:文件系统信息===" "$data_file"; then
            echo "===SECTION:文件系统信息===" >> "$data_file"
            # 写入表头（使用|分隔）
            append_raw_data_line "$data_file" "文件系统信息" "文件系统名|大小|已用|可用|使用率|挂载点"
            end_raw_data_section "$data_file" "文件系统信息"
        fi
    fi
}

# 采集网络信息（计算速率和汇总指标）
collect_network_info() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    local data_file=$6
    
    log INFO "采集网络信息..."
    
    # 获取当前时间戳
    local collect_timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # 上一次网络数据文件路径（使用文本格式）
    local prev_network_file="${TMP_DIR}/prev_network_data.txt"
    
    # 获取网络接口数据
    local cmd_net="cat /proc/net/dev | grep -v 'lo:' | grep -v docker0 | grep -v virbr | awk 'NR>2 {ifname=\$1; gsub(/:/, \"\", ifname); printf \"%s|%s|%s|%s|%s|%s|%s|%s|%s\\n\", ifname, \$2, \$3, \$4, \$5, \$10, \$11, \$12, \$13}'"
    local net_result=$(execute_ssh "$host" "$port" "$username" "$password" "$ssh_key_path" "$cmd_net")
    # echo $net_result
    if [ -n "$net_result" ]; then
        # 使用关联数组存储每个接口的指标值（替代JSON）
        declare -A interface_rx_rate_dict
        declare -A interface_tx_rate_dict
        
        # 用于计算所有接口的汇总值
        local total_error_drop_rate=0
        local total_rx_rate=0
        local total_tx_rate=0
        
        # 用于保存当前数据供下次计算速率使用（使用文本格式）
        local new_network_file="${TMP_DIR}/prev_network_data_new.txt"
        > "$new_network_file"  # 清空新文件
        
        # 获取指标ID
        local rx_rate_id=$(get_metric_id "网络接口接收速率（字节/秒）" "os")
        local tx_rate_id=$(get_metric_id "网络接口发送速率（字节/秒）" "os")
        
        while IFS='|' read -r ifname rx_bytes rx_packets rx_errs rx_drop tx_bytes tx_packets tx_errs tx_drop; do
            if [ -n "$ifname" ]; then
                # 清理数据
                ifname=$(echo "$ifname" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                rx_bytes=$(echo "$rx_bytes" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                rx_packets=$(echo "$rx_packets" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                rx_errs=$(echo "$rx_errs" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                rx_drop=$(echo "$rx_drop" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                tx_bytes=$(echo "$tx_bytes" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                tx_packets=$(echo "$tx_packets" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                tx_errs=$(echo "$tx_errs" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                tx_drop=$(echo "$tx_drop" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                
                # echo $collect_timestamp
                # echo $ifname $rx_bytes $tx_bytes $rx_errs $rx_drop $tx_errs $tx_drop
                # 验证数据为数字
                if [[ "$rx_bytes" =~ ^[0-9]+$ ]] && [[ "$tx_bytes" =~ ^[0-9]+$ ]] && \
                   [[ "$rx_errs" =~ ^[0-9]+$ ]] && [[ "$rx_drop" =~ ^[0-9]+$ ]] && \
                   [[ "$tx_errs" =~ ^[0-9]+$ ]] && [[ "$tx_drop" =~ ^[0-9]+$ ]]; then
                    
                    # 计算每个接口的指标
                    # 3000201: 网络接口丢包错包总数 = 接收错误 + 接收丢包 + 发送错误 + 发送丢包
                    local error_drop_total=$((rx_errs + rx_drop + tx_errs + tx_drop))
                    
                    # 计算速率（需要上一次的数据）
                    local rx_rate=0
                    local tx_rate=0
                    local error_drop_rate=0
                    
                    # 读取上一次的数据（从文本格式读取）
                    if [ -f "$prev_network_file" ]; then
                        # 从文本文件中提取数据（格式：接口名|rx_bytes|tx_bytes|error_drop_packages|timestamp）
                        local prev_data_line=$(grep "^${ifname}|" "$prev_network_file" 2>/dev/null | head -1)
                        if [ -n "$prev_data_line" ]; then
                            # 提取网络接口数据（使用|分隔）
                            local prev_rx_bytes=$(echo "$prev_data_line" | cut -d'|' -f2 || echo "0")
                            local prev_tx_bytes=$(echo "$prev_data_line" | cut -d'|' -f3 || echo "0")
                            local prev_error_drop=$(echo "$prev_data_line" | cut -d'|' -f4 || echo "0")
                            # 提取timestamp（格式：YYYY-MM-DD HH:MM:SS）
                            local prev_timestamp=$(echo "$prev_data_line" | cut -d'|' -f5 || echo "")
                            
                            if [ -n "$prev_timestamp" ] && [ "$prev_timestamp" != "null" ] && [ "$prev_timestamp" != "" ]; then
                                # 计算时间差（秒）
                                local time_diff=$(awk -v start="$prev_timestamp" -v end="$collect_timestamp" '
                                BEGIN {
                                    # 解析时间字符串 "YYYY-MM-DD HH:MM:SS"
                                    split(start, start_parts, /[- :]/)
                                    split(end, end_parts, /[- :]/)
                                    start_epoch = mktime(start_parts[1] " " start_parts[2] " " start_parts[3] " " start_parts[4] " " start_parts[5] " " start_parts[6])
                                    end_epoch = mktime(end_parts[1] " " end_parts[2] " " end_parts[3] " " end_parts[4] " " end_parts[5] " " end_parts[6])
                                    diff = end_epoch - start_epoch
                                    if (diff > 0) print diff
                                    else print 1
                                }')
                                
                                if [ "$time_diff" -gt 0 ]; then
                                    # 计算速率（字节/秒）- 使用-v选项安全传递变量
                                    rx_rate=$(awk -v rx_bytes="$rx_bytes" -v prev_rx_bytes="$prev_rx_bytes" -v time_diff="$time_diff" 'BEGIN {printf "%.2f", (rx_bytes - prev_rx_bytes) / time_diff}')
                                    tx_rate=$(awk -v tx_bytes="$tx_bytes" -v prev_tx_bytes="$prev_tx_bytes" -v time_diff="$time_diff" 'BEGIN {printf "%.2f", (tx_bytes - prev_tx_bytes) / time_diff}')
                                    error_drop_rate=$(awk -v error_drop_total="$error_drop_total" -v prev_error_drop="$prev_error_drop" -v time_diff="$time_diff" 'BEGIN {printf "%.2f", (error_drop_total - prev_error_drop) / time_diff}')
                                    
                                    # 确保速率不为负（处理计数器重置的情况）
                                    if (( $(echo "$rx_rate < 0" | bc -l 2>/dev/null || echo "1") )); then
                                        rx_rate=0
                                    fi
                                    if (( $(echo "$tx_rate < 0" | bc -l 2>/dev/null || echo "1") )); then
                                        tx_rate=0
                                    fi
                                    if (( $(echo "$error_drop_rate < 0" | bc -l 2>/dev/null || echo "1") )); then
                                        error_drop_rate=0
                                    fi
                                fi
                            fi
                        fi
                    fi
                    
                    # 3000204: 网络接口接收速率（字节/秒）- 使用关联数组存储
                    if [ -n "$rx_rate_id" ]; then
                        interface_rx_rate_dict["${rx_rate_id}_${ifname}"]="$rx_rate"
                    else
                        interface_rx_rate_dict["network_rx_rate_${ifname}"]="$rx_rate"
                    fi
                    
                    # 3000206: 网络接口发送速率（字节/秒）- 使用关联数组存储
                    if [ -n "$tx_rate_id" ]; then
                        interface_tx_rate_dict["${tx_rate_id}_${ifname}"]="$tx_rate"
                    else
                        interface_tx_rate_dict["network_tx_rate_${ifname}"]="$tx_rate"
                    fi
                    
                    # 保存当前数据供下次计算速率使用（使用文本格式：接口名|rx_bytes|tx_bytes|error_drop_packages|timestamp）
                    echo "${ifname}|${rx_bytes}|${tx_bytes}|${error_drop_total}|${collect_timestamp}" >> "$new_network_file"
                    
                    # 累计所有接口的汇总值
                    total_error_drop_rate=$(awk "BEGIN {printf \"%.2f\", $total_error_drop_rate + $error_drop_rate}")
                    total_rx_rate=$(awk "BEGIN {printf \"%.2f\", $total_rx_rate + $rx_rate}")
                    total_tx_rate=$(awk "BEGIN {printf \"%.2f\", $total_tx_rate + $tx_rate}")
                else
                    log DEBUG "跳过无效的网络接口数据: $ifname"
                fi
            fi
        done <<< "$net_result"
        
        # 将新数据文件替换旧文件（供下次使用）
        if [ -f "$new_network_file" ]; then
            mv "$new_network_file" "$prev_network_file"
        fi
        
        # 注意：network_info不保存到采集文件中（仅用于内部处理）
        
        # 保存网络指标（使用关联数组替代JSON）
        # 3000204: 网络接口接收速率（字节/秒）- 保存到字典文件
        local rx_rate_metrics_file="${TMP_DIR}/network_rx_rate.dict"
        > "$rx_rate_metrics_file"  # 清空文件
        for metric_key in "${!interface_rx_rate_dict[@]}"; do
            echo "${metric_key}:${interface_rx_rate_dict[$metric_key]}" >> "$rx_rate_metrics_file"
        done
        
        # 3000206: 网络接口发送速率（字节/秒）- 保存到字典文件
        local tx_rate_metrics_file="${TMP_DIR}/network_tx_rate.dict"
        > "$tx_rate_metrics_file"  # 清空文件
        for metric_key in "${!interface_tx_rate_dict[@]}"; do
            echo "${metric_key}:${interface_tx_rate_dict[$metric_key]}" >> "$tx_rate_metrics_file"
        done
        
        # 3000201: 所有网络接口每秒丢包错包数（汇总值）
        local error_drop_id=$(get_metric_id "所有网络接口每秒丢包错包数" "os")
        declare -A network_error_drop_metrics
        if [ -n "$error_drop_id" ]; then
            network_error_drop_metrics["$error_drop_id"]="$total_error_drop_rate"
        else
            network_error_drop_metrics["network_error_drop_rate"]="$total_error_drop_rate"
        fi
        local error_drop_metrics_file="${TMP_DIR}/network_error_drop_rate.dict"
        > "$error_drop_metrics_file"
        for metric_id in "${!network_error_drop_metrics[@]}"; do
            echo "${metric_id}:${network_error_drop_metrics[$metric_id]}" >> "$error_drop_metrics_file"
        done
        
        # 3000208: 所有网络接口总速率（字节/秒）= 接收速率 + 发送速率（汇总值）
        local total_rate=$(awk "BEGIN {printf \"%.2f\", $total_rx_rate + $total_tx_rate}")
        local total_rate_id=$(get_metric_id "所有网络接口总速率（字节/秒）" "os")
        declare -A network_total_rate_metrics
        if [ -n "$total_rate_id" ]; then
            network_total_rate_metrics["$total_rate_id"]="$total_rate"
        else
            network_total_rate_metrics["network_total_rate"]="$total_rate"
        fi
        local total_rate_metrics_file="${TMP_DIR}/network_total_rate.dict"
        > "$total_rate_metrics_file"
        for metric_id in "${!network_total_rate_metrics[@]}"; do
            echo "${metric_id}:${network_total_rate_metrics[$metric_id]}" >> "$total_rate_metrics_file"
        done
        
        log INFO "网络信息采集完成（接口数: $(echo "$net_result" | wc -l), 总接收速率: ${total_rx_rate} 字节/秒, 总发送速率: ${total_tx_rate} 字节/秒）"
    else
        log WARNING "无法获取网络信息"
    fi
}

# 采集CPU数量信息
collect_cpu_num_info() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    local data_file=$6
    
    log INFO "采集CPU数量信息..."
    
    # 使用cat命令获取CPU数量
    local cmd_cpu_num="cat /proc/cpuinfo | grep 'processor' | wc -l"
    local cpu_num_result=$(execute_ssh "$host" "$port" "$username" "$password" "$ssh_key_path" "$cmd_cpu_num")
    local cpu_num_id=$(get_metric_id "cpu num" "os")
    if [ -n "$cpu_num_result" ]; then
        local cpu_num=$(echo "$cpu_num_result" | head -1 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        # 验证是否为数字
        if [[ "$cpu_num" =~ ^[0-9]+$ ]]; then
            # 写入到os_data_once.txt文件
            if [ ! -f "$data_file" ] || ! grep -q "===SECTION:$cpu_num_id===" "$data_file"; then
                echo "===SECTION:$cpu_num_id===" >> "$data_file"
            fi
            append_raw_data_line "$data_file" "$cpu_num_id" "$cpu_num"
            end_raw_data_section "$data_file" "$cpu_num_id"
            log INFO "CPU数量: $cpu_num"
        else
            log WARNING "CPU数量采集结果无效: $cpu_num_result"
            # 即使结果无效也写入空段（包含表头）
            if [ ! -f "$data_file" ] || ! grep -q "===SECTION:$cpu_num_id===" "$data_file"; then
                echo "===SECTION:$cpu_num_id===" >> "$data_file"
                end_raw_data_section "$data_file" "$cpu_num_id"
            fi
        fi
    else
        log WARNING "无法获取CPU数量信息"
        # 即使没有结果也写入空段（包含表头）
        append_raw_data_line "$data_file" "$cpu_num_id" "0"
        end_raw_data_section "$data_file" "$cpu_num_id"
    fi
}

# OS单次采集函数（只需采集一次的指标：文件系统信息、CPU数量）
collect_os_data_once() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    
    log INFO "========== 开始操作系统单次采集 =========="
    
    local os_data_file="${TMP_DIR}/os_data_once.txt"
    
    # 初始化OS数据文件（原始数据格式，直接创建空文件）
    mkdir -p "$(dirname "$os_data_file")"
    > "$os_data_file"  # 创建空文件
    if [ ! -f "$os_data_file" ]; then
        log ERROR "OS数据文件初始化失败: $os_data_file"
        return 1
    fi
    log DEBUG "OS数据文件初始化成功"
    
    # 采集CPU数量信息（只需一次采集）
    collect_cpu_num_info "$host" "$port" "$username" "$password" "$ssh_key_path" "$os_data_file"
    
    # 采集meminfo信息（只需一次采集）
    collect_meminfo "$host" "$port" "$username" "$password" "$ssh_key_path" "$os_data_file"
    
    # 采集文件系统信息（只需一次采集）
    collect_fs_info "$host" "$port" "$username" "$password" "$ssh_key_path" "$os_data_file"
    
    # 采集操作系统日志（只需一次采集）
    collect_os_log "$host" "$port" "$username" "$password" "$ssh_key_path" "$os_data_file"
    
    # 采集数据库日志（只需一次采集，需要数据库日志文件路径）
    local db_log_file_path=""
    if [ -f "${TMP_DIR}/db_log_path.txt" ]; then
        # 读取路径并清理，移除可能的换行符、空格和日志信息
        db_log_file_path=$(cat "${TMP_DIR}/db_log_path.txt" | tr -d '\n\r' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | grep -v "^\[INFO\]" | grep -v "^\[WARNING\]" | grep -v "^\[ERROR\]" | head -1)
    fi
    collect_db_log "$host" "$port" "$username" "$password" "$ssh_key_path" "$os_data_file" "$db_log_file_path"
    
    # 所有数据已通过 write_raw_data 和 append_raw_data_line 直接写入到 data_file，无需合并
    # 保存OS数据
    mkdir -p "$DATA_DIR"
    local output_file="$DATA_DIR/os_data_once.txt"
    if [ -f "$os_data_file" ] && [ -s "$os_data_file" ]; then
        cp "$os_data_file" "$output_file"
        local file_size=$(stat -c%s "$output_file" 2>/dev/null || wc -c < "$output_file" 2>/dev/null || echo "未知")
        log INFO "OS单次采集数据已保存到: $output_file (文件大小: $file_size 字节)"
    else
        log ERROR "OS数据文件为空或不存在，无法保存: $os_data_file"
        if [ -f "$os_data_file" ]; then
            local file_size=$(stat -c%s "$os_data_file" 2>/dev/null || wc -c < "$os_data_file" 2>/dev/null || echo "0")
            log ERROR "文件存在但为空，文件大小: $file_size 字节"
        fi
    fi
    
    log INFO "========== 操作系统单次采集完成 =========="
}

# OS多次采集函数（需要多次采样的指标：CPU、内存、Swap、IO、网络）
collect_os_data_multiple() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    local collect_index=${6:-1}
    
    log INFO "========== 开始操作系统多次采集（第 ${collect_index} 次） =========="
    
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local os_data_file="${DATA_DIR}/os_data_${PROGRAM_START_TIME}.txt"
    
    # 采集需要多次采样的指标（采集所有指标）
    collect_cpu_info "$host" "$port" "$username" "$password" "$ssh_key_path" "$os_data_file"
    collect_memory_info "$host" "$port" "$username" "$password" "$ssh_key_path" "$os_data_file"
    collect_swap_info "$host" "$port" "$username" "$password" "$ssh_key_path" "$os_data_file"
    collect_io_stats "$host" "$port" "$username" "$password" "$ssh_key_path" "$os_data_file"
    collect_io_await "$host" "$port" "$username" "$password" "$ssh_key_path" "$os_data_file"
    collect_runqueue_info "$host" "$port" "$username" "$password" "$ssh_key_path" "$os_data_file"
    collect_network_info "$host" "$port" "$username" "$password" "$ssh_key_path" "$os_data_file"
    
    # 构建本次采集的数据行（原始数据格式：时间戳|指标ID:值|指标ID:值|...）
    # 使用临时文件收集所有键值对
    local temp_metrics_file="${TMP_DIR}/os_multiple_metrics_${collect_index}.tmp"
    > "$temp_metrics_file"  # 清空临时文件
    
    # 从字典文件读取并添加CPU信息（使用指标ID）
    if [ -f "${TMP_DIR}/cpu_info.dict" ]; then
        cat "${TMP_DIR}/cpu_info.dict" >> "$temp_metrics_file"
    fi
    
    # 从字典文件读取并添加内存信息（使用指标ID）
    if [ -f "${TMP_DIR}/mem_info.dict" ]; then
        cat "${TMP_DIR}/mem_info.dict" >> "$temp_metrics_file"
    fi
    
    # 从字典文件读取并添加Swap信息（使用指标ID）
    if [ -f "${TMP_DIR}/swap_info.dict" ]; then
        cat "${TMP_DIR}/swap_info.dict" >> "$temp_metrics_file"
    fi
    
    # 从字典文件读取并添加IO统计（使用指标ID）
    if [ -f "${TMP_DIR}/io_stats.dict" ]; then
        cat "${TMP_DIR}/io_stats.dict" >> "$temp_metrics_file"
    fi
    
    # 从字典文件读取并添加IO响应时间（await）信息（使用指标ID）
    if [ -f "${TMP_DIR}/io_await_info.dict" ]; then
        cat "${TMP_DIR}/io_await_info.dict" >> "$temp_metrics_file"
    fi
    
    # 从字典文件读取并添加运行队列信息（使用指标ID）
    if [ -f "${TMP_DIR}/runqueue_info.dict" ]; then
        cat "${TMP_DIR}/runqueue_info.dict" >> "$temp_metrics_file"
    fi
    
    # 从字典文件读取并添加网络速率指标（使用指标ID）
    # 3000204: 网络接口接收速率（字节/秒）
    if [ -f "${TMP_DIR}/network_rx_rate.dict" ]; then
        cat "${TMP_DIR}/network_rx_rate.dict" >> "$temp_metrics_file"
    fi
    
    # 3000206: 网络接口发送速率（字节/秒）
    if [ -f "${TMP_DIR}/network_tx_rate.dict" ]; then
        cat "${TMP_DIR}/network_tx_rate.dict" >> "$temp_metrics_file"
    fi
    
    # 3000200: 所有网络接口每秒丢包错包数（汇总值）
    if [ -f "${TMP_DIR}/network_error_drop_rate.dict" ]; then
        cat "${TMP_DIR}/network_error_drop_rate.dict" >> "$temp_metrics_file"
    fi
    
    # 3000208: 所有网络接口总速率（字节/秒）（汇总值）
    if [ -f "${TMP_DIR}/network_total_rate.dict" ]; then
        cat "${TMP_DIR}/network_total_rate.dict" >> "$temp_metrics_file"
    fi
    
    # 构建数据行：时间戳|指标ID:值|指标ID:值|...
    local data_line="$timestamp"
    if [ -f "$temp_metrics_file" ] && [ -s "$temp_metrics_file" ]; then
        while IFS= read -r metric_line; do
            if [ -n "$metric_line" ]; then
                data_line="$data_line|$metric_line"
            fi
        done < "$temp_metrics_file"
    fi
    
    # 追加到文件（原始数据格式：每行一个时间点的所有指标数据）
    mkdir -p "$DATA_DIR"
    append_raw_data_line "$os_data_file" "os_multiple_collect" "$data_line"
    
    # 清理临时文件
    rm -f "$temp_metrics_file"
    
    log INFO "OS多次采集数据已追加保存到: $os_data_file (采集时间: $timestamp, 第 ${collect_index} 次)"
    log INFO "========== 操作系统多次采集完成（第 ${collect_index} 次） =========="
}

# 主OS采集函数（兼容旧接口，默认单次采集）
collect_os_data() {
    local host=$1
    local port=$2
    local username=$3
    local password=$4
    local ssh_key_path=$5
    
    # 默认执行单次采集
    collect_os_data_once "$host" "$port" "$username" "$password" "$ssh_key_path"
}

# ==================== 数据合并函数 ====================


# 合并OS和Oracle采集数据（最终合并）
merge_all_data() {
    local data_dir=$1
    local db_name=$2
    local program_start_time=$3
    
    log INFO "========== 开始合并OS和Oracle采集数据 =========="
    
    # 检查数据目录是否存在
    if [ ! -d "$data_dir" ]; then
        log WARNING "数据目录不存在: $data_dir，跳过数据合并"
        return 1
    fi
    
    local has_data=false
    
    # ========== 读取OS单次采集数据 ==========
    local os_once_file="$data_dir/os_data_once.txt"
    local os_once_file_path=""
    
    if [ -f "$os_once_file" ]; then
        log INFO "读取OS单次采集数据（原始格式）: $os_once_file"
        os_once_file_path="$os_once_file"
        has_data=true
    else
        log DEBUG "未找到OS单次采集数据文件: $os_once_file"
    fi
    
    # ========== 读取OS多次采集数据 ==========
    local os_multiple_file=""
    local os_multiple_line_count=0
    
    if [ -n "$program_start_time" ]; then
        os_multiple_file="$data_dir/os_data_${program_start_time}.txt"
    else
        # 查找最新的 .txt 文件
        local os_pattern_txt="$data_dir/os_data_*.txt"
        local os_files=()
        for file in $os_pattern_txt; do
            if [ -f "$file" ] && [ "$(basename "$file")" != "os_data_once.txt" ]; then
                os_files+=("$file")
            fi
        done
        if [ ${#os_files[@]} -gt 0 ]; then
            os_multiple_file="${os_files[0]}"
            for file in "${os_files[@]}"; do
                if [ "$file" -nt "$os_multiple_file" ]; then
                    os_multiple_file="$file"
                fi
            done
        fi
    fi
    
    if [ -n "$os_multiple_file" ] && [ -f "$os_multiple_file" ]; then
        log INFO "读取OS多次采集数据: $os_multiple_file"
        # 检查是否为原始数据格式（包含 ===SECTION:os_multiple_collect===）
        if grep -q "===SECTION:os_multiple_collect===" "$os_multiple_file"; then
            # 统计采样点数量
            local in_section=false
            while IFS= read -r line || [ -n "$line" ]; do
                if echo "$line" | grep -q "^===SECTION:os_multiple_collect==="; then
                    in_section=true
                    continue
                fi
                if echo "$line" | grep -q "^===END_SECTION:os_multiple_collect==="; then
                    in_section=false
                    continue
                fi
                if [ "$in_section" = true ] && [ -n "$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')" ]; then
                    os_multiple_line_count=$((os_multiple_line_count + 1))
                fi
            done < "$os_multiple_file"
            has_data=true
            log INFO "OS多次采集数据包含 $os_multiple_line_count 个采样点"
        else
            log DEBUG "OS多次采集数据文件格式错误"
        fi
    else
        log DEBUG "未找到OS多次采集数据文件"
    fi
    
    # ========== 读取Oracle单次采集数据 ==========
    local oracle_once_file="$data_dir/oracle_data_once.txt"
    
    if [ -f "$oracle_once_file" ]; then
        log INFO "读取Oracle单次采集数据（原始格式）: $oracle_once_file"
        has_data=true
    else
        log DEBUG "未找到Oracle单次采集数据文件"
    fi
    
    # ========== 检查是否有数据 ==========
    if [ "$has_data" = false ]; then
        log WARNING "未找到任何采集数据文件，跳过合并"
        return 1
    fi
    
    # 判断是否有单次采集数据
    local once_has_data=false
    if [ -n "$oracle_once_file" ] && [ -f "$oracle_once_file" ]; then
        once_has_data=true
    fi
    if [ -n "$os_once_file_path" ] && [ -f "$os_once_file_path" ]; then
        once_has_data=true
    fi
    
    # 判断是否有多次采集数据
    local multiple_has_data=false
    if [ -n "$os_multiple_file" ] && [ -f "$os_multiple_file" ] && [ "$os_multiple_line_count" -gt 0 ]; then
        multiple_has_data=true
    fi
    
    # 格式化时间戳
    local program_start_time_formatted=""
    if [ -n "$program_start_time" ]; then
        # 将 'YYYYMMDD_HHMMSS' 格式转换为 'YYYY-MM-DD HH:MM:SS' 格式
        local year=$(echo "$program_start_time" | cut -d'_' -f1 | cut -c1-4)
        local month=$(echo "$program_start_time" | cut -d'_' -f1 | cut -c5-6)
        local day=$(echo "$program_start_time" | cut -d'_' -f1 | cut -c7-8)
        local hour=$(echo "$program_start_time" | cut -d'_' -f2 | cut -c1-2)
        local minute=$(echo "$program_start_time" | cut -d'_' -f2 | cut -c3-4)
        local second=$(echo "$program_start_time" | cut -d'_' -f2 | cut -c5-6)
        program_start_time_formatted="${year}-${month}-${day} ${hour}:${minute}:${second}"
    else
        program_start_time_formatted=$(date '+%Y-%m-%d %H:%M:%S')
    fi
    
    local program_end_time=$(date '+%Y-%m-%d %H:%M:%S')
    
    # 生成合并后的文件名（使用.txt扩展名）
    local timestamp_str="${program_start_time:-$(date +%Y%m%d_%H%M%S)}"
    local db_name_safe=$(echo "$db_name" | sed 's/[^a-zA-Z0-9_]/_/g')
    local merged_file="$data_dir/${db_name_safe}_dbcheck_data_${timestamp_str}.txt"
    
    # 保存合并后的数据（使用文本格式）
    {
        # 写入once_collect部分
        if [ "$once_has_data" = true ]; then
            echo "===SECTION:once_collect==="
            
            # 写入Oracle单次采集数据
            if [ -n "$oracle_once_file" ] && [ -f "$oracle_once_file" ]; then
                echo "===SECTION:oracle==="
                # 直接输出整个文件内容，保留所有内部的section标记
                cat "$oracle_once_file"
                echo "===END_SECTION:oracle==="
            fi
            
            # 写入OS单次采集数据
            if [ -n "$os_once_file_path" ] && [ -f "$os_once_file_path" ]; then
                echo "===SECTION:os==="
                # 直接输出整个文件内容，保留所有内部的section标记
                cat "$os_once_file_path"
                echo "===END_SECTION:os==="
            fi
            
            echo "===END_SECTION:once_collect==="
        fi
        
        # 写入multiple_times_collect部分
        if [ "$multiple_has_data" = true ]; then
            echo "===SECTION:multiple_times_collect==="
            
            # 写入OS多次采集数据
            if [ -n "$os_multiple_file" ] && [ -f "$os_multiple_file" ]; then
                echo "===SECTION:os_multiple_collect==="
                # 直接从原始文件读取数据段内容
                local in_section=false
                while IFS= read -r line || [ -n "$line" ]; do
                    if echo "$line" | grep -q "^===SECTION:os_multiple_collect==="; then
                        in_section=true
                        continue
                    fi
                    if echo "$line" | grep -q "^===END_SECTION:os_multiple_collect==="; then
                        in_section=false
                        continue
                    fi
                    if [ "$in_section" = true ]; then
                        echo "$line"
                    fi
                done < "$os_multiple_file"
                echo "===END_SECTION:os_multiple_collect==="
            fi
            
            echo "===END_SECTION:multiple_times_collect==="
        fi
        
        # 写入metadata部分
        echo "===SECTION:metadata==="
        echo "db_type=oracle"
        echo "program_start_time=$program_start_time_formatted"
        echo "program_end_time=$program_end_time"
        echo "data_type=db_check"
        echo "===END_SECTION:metadata==="
    } > "$merged_file"
    
    log INFO "数据文件合并完成: $merged_file"
    
    # 输出合并统计信息
    if [ "$once_has_data" = true ]; then
        local once_info=()
        if [ -n "$oracle_once_file" ] && [ -f "$oracle_once_file" ]; then
            once_info+=("Oracle单次采集")
        fi
        if [ -n "$os_once_file_path" ] && [ -f "$os_once_file_path" ]; then
            once_info+=("OS单次采集")
        fi
        if [ ${#once_info[@]} -gt 0 ]; then
            log INFO "  - 单次采集: $(IFS=', '; echo "${once_info[*]}")"
        fi
    fi
    
    if [ "$multiple_has_data" = true ]; then
        log INFO "  - 多次采集: OS多次采集(${os_multiple_line_count}次采样)"
    fi
    
    log INFO "========== OS和Oracle数据合并完成 =========="
    
    echo "$merged_file"
    return 0
}

# 清理中间文件，只保留最终结果文件和日志文件
cleanup_intermediate_files() {
    local data_dir=$1
    local final_result_file=$2
    local log_file=$3
    
    log INFO "========== 开始清理中间文件 =========="
    
    # 删除DATA_DIR下的中间文件，但保留最终结果文件
    if [ -n "$data_dir" ] && [ -d "$data_dir" ]; then
        # 删除oracle_data_once.txt
        local oracle_once_file="$data_dir/oracle_data_once.txt"
        if [ -f "$oracle_once_file" ]; then
            log INFO "删除中间文件: $oracle_once_file"
            rm -f "$oracle_once_file" 2>/dev/null || true
        fi
        
        # 删除os_data_once.txt
        local os_once_file="$data_dir/os_data_once.txt"
        if [ -f "$os_once_file" ]; then
            log INFO "删除中间文件: $os_once_file"
            rm -f "$os_once_file" 2>/dev/null || true
        fi
        
        # 删除os_data_*.txt
        local os_multiple_pattern="$data_dir/os_data_*.txt"
        for file in $os_multiple_pattern; do
            if [ -f "$file" ]; then
                # 排除最终结果文件（格式：*_dbcheck_data_*.txt
                if [[ "$(basename "$file")" != *_dbcheck_data_*.txt ]]; then
                    log INFO "删除中间文件: $file"
                    rm -f "$file" 2>/dev/null || true
                fi
            fi
        done
        
        # 删除其他可能的临时文件（.tmp文件等）
        local temp_pattern="$data_dir/*.tmp"
        for file in $temp_pattern; do
            if [ -f "$file" ]; then
                log INFO "删除临时文件: $file"
                rm -f "$file" 2>/dev/null || true
            fi
        done
        
        # 确认最终结果文件存在
        if [ -n "$final_result_file" ] && [ -f "$final_result_file" ]; then
            log INFO "保留最终结果文件: $final_result_file"
        fi
    fi
    
    # 确认日志文件存在
    if [ -n "$log_file" ] && [ -f "$log_file" ]; then
        log INFO "保留日志文件: $log_file"
    fi
    
    log INFO "========== 中间文件清理完成 =========="
}


# ==================== 参数解析 ====================

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -H|--db-host)
                DB_HOST="$2"
                shift 2
                ;;
            -P|--db-port)
                DB_PORT="$2"
                shift 2
                ;;
            -u|--db-username)
                DB_USERNAME="$2"
                shift 2
                ;;
            -p|--db-password)
                DB_PASSWORD="$2"
                shift 2
                ;;
            -d|--dbname)
                DBNAME="$2"
                shift 2
                ;;
            --os-host)
                OS_HOST="$2"
                shift 2
                ;;
            --os-port)
                OS_PORT="$2"
                shift 2
                ;;
            --os-username)
                OS_USERNAME="$2"
                shift 2
                ;;
            --os-password)
                OS_PASSWORD="$2"
                shift 2
                ;;
            --os-ssh-key-path)
                OS_SSH_KEY_PATH="$2"
                shift 2
                ;;
            --os-ssh-key-user)
                OS_SSH_KEY_USER="$2"
                shift 2
                ;;
            --log-path)
                LOG_DIR="$2"
                shift 2
                ;;
            --data-dir)
                DATA_DIR="$2"
                shift 2
                ;;
            --log-level)
                LOG_LEVEL="$2"
                shift 2
                ;;
            --os-collect-interval)
                OS_COLLECT_INTERVAL="$2"
                shift 2
                ;;
            --os-collect-duration)
                OS_COLLECT_DURATION="$2"
                shift 2
                ;;
            --os-collect-count)
                OS_COLLECT_COUNT="$2"
                shift 2
                ;;
            --sql-timeout)
                SQL_TIMEOUT="$2"
                shift 2
                ;;
            --local)
                LOCAL_MODE=true
                LOCAL_MODE_FROM_ARG=true
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log ERROR "未知参数: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

show_help() {
    cat <<EOF
Oracle数据库监控数据采集脚本（Shell版本）

用法: $0 [选项]

必需参数（Oracle模式，远程采集）:
  -H, --db-host HOST           数据库主机地址
  -P, --db-port PORT           数据库端口
  -u, --db-username USER       数据库用户名
  -p, --db-password PASS       数据库密码
  -d, --dbname NAME            数据库服务名(SERVICE_NAME)

可选参数:
  --os-host HOST               操作系统主机地址（默认与数据库主机相同，本地模式不需要）
  --os-port PORT               SSH端口（默认: 22，本地模式不需要）
  --os-username USER           SSH用户名（默认与数据库用户名相同，本地模式不需要）
  --os-password PASS           SSH密码（默认与数据库密码相同，本地模式不需要）
  --os-ssh-key-path PATH      SSH密钥路径（本地模式不需要）
  --os-ssh-key-user USER      SSH密钥用户（默认: root，本地模式不需要）
  --local                      本地采集模式，不需要SSH连接（本地模式不需要输入IP和SSH参数）
  --log-path PATH              日志文件路径（默认: ./logs/db_collector.log）
  --data-dir DIR               数据保存目录（默认: ./logs）
  --log-level LEVEL            日志级别: DEBUG|INFO|WARNING|ERROR（默认: INFO）
  --os-collect-interval SEC    操作系统采集间隔（秒，0=只采集一次）
  --os-collect-duration SEC    操作系统采集总时长（秒，必须>0，不支持无限采集）
  --os-collect-count COUNT     操作系统采集采样点个数（必须>0，不支持无限采集）
  --sql-timeout SEC             SQL执行超时时间（秒，默认: 300，0=不设置超时）

示例:
  # 远程采集（只支持通过服务名 SERVICE_NAME 连接）
  $0 -H 192.168.1.100 -P 1521 -u scott -p tiger -d ORCL --os-host 192.168.1.100 --os-port 22 --os-username root --os-password password --os-collect-interval 1 --os-collect-count 10
  $0 -H 192.168.1.100 -P 1521 -u sys -p password -d ORCL --os-host 192.168.1.100 --os-port 22 --os-username root --os-password password --os-collect-interval 1 --os-collect-duration 600
  # 本地采集（脚本运行在数据库主机上，只支持通过实例名 SID 连接, 不需要输入IP和SSH参数）
  $0 -u scott -p tiger -d ORCL --local --os-collect-interval 1 --os-collect-count 10

EOF
}

# ==================== 交互式输入 ====================

interactive_input() {
    log INFO "进入交互式输入模式"
    
    # 采集模式选择：在交互开始时优先确认是本地采集还是远程采集
    # 如果命令行已经通过 --local 明确指定，则不再询问
    if [ "$LOCAL_MODE_FROM_ARG" != "true" ]; then
        # 如果尚未提供远程主机信息，则让用户选择采集模式
        if [ -z "$DB_HOST" ] && [ -z "$OS_HOST" ]; then
            echo "请选择采集模式："
            echo "  1) 本地采集（脚本运行在数据库所在主机，直接访问数据库和操作系统，无需SSH）"
            echo "  2) 远程采集（通过SSH远程连接数据库主机进行采集）"
            read -p "选择 [1/2，默认1=本地采集]: " collect_mode_choice
            case "$collect_mode_choice" in
                2)
                    LOCAL_MODE=false
                    ;;
                *)
                    LOCAL_MODE=true
                    ;;
            esac
            log INFO "用户选择采集模式：$([ "$LOCAL_MODE" = "true" ] && echo 本地采集 || echo 远程采集)"
        fi
    fi
    
    # Oracle数据库连接参数（固定采集）
    # 本地模式下不需要输入主机和端口
    if [ "$LOCAL_MODE" = "true" ]; then
        if [ -z "$DB_USERNAME" ]; then
            read -p "数据库用户名: " DB_USERNAME
        fi
        if [ -z "$DB_PASSWORD" ]; then
            read -sp "数据库密码: " DB_PASSWORD
            echo
        fi
        if [ -z "$DBNAME" ]; then
            read -p "数据库实例名(SID): " DBNAME
        fi
    else
        # 远程模式需要输入主机和端口
        if [ -z "$DB_HOST" ]; then
            read -p "数据库主机地址: " DB_HOST
        fi
        if [ -z "$DB_PORT" ]; then
            read -p "数据库端口 [1521]: " input
            DB_PORT=${input:-1521}
        fi
        if [ -z "$DB_USERNAME" ]; then
            read -p "数据库用户名: " DB_USERNAME
        fi
        if [ -z "$DB_PASSWORD" ]; then
            read -sp "数据库密码: " DB_PASSWORD
            echo
        fi
        # 远程模式只支持通过服务名连接
        # 提示输入服务名
        if [ -z "$DBNAME" ]; then
            read -p "数据库服务名(SERVICE_NAME): " DBNAME
        fi
    fi
    # OS连接参数与采集参数（固定采集）
    if [ "$LOCAL_MODE" != "true" ]; then
        # 远程模式需要输入SSH参数
        if [ -z "$OS_HOST" ]; then
            if [ -n "$DB_HOST" ]; then
                read -p "操作系统主机地址 [${DB_HOST}]: " input
                OS_HOST=${input:-$DB_HOST}
            else
                read -p "操作系统主机地址: " OS_HOST
            fi
        fi
        if [ -z "$OS_PORT" ]; then
            read -p "SSH端口 [22]: " input
            OS_PORT=${input:-22}
        fi
        if [ -z "$OS_USERNAME" ]; then
            if [ -n "$DB_USERNAME" ]; then
                read -p "SSH用户名 [${DB_USERNAME}]: " input
                OS_USERNAME=${input:-$DB_USERNAME}
            else
                read -p "SSH用户名: " OS_USERNAME
            fi
        fi
        if [ -z "$OS_PASSWORD" ]; then
            if [ -n "$DB_PASSWORD" ]; then
                read -sp "SSH密码 [默认与数据库密码相同，直接回车使用]: " input
                echo
                OS_PASSWORD=${input:-$DB_PASSWORD}
            else
                read -sp "SSH密码: " OS_PASSWORD
                echo
            fi
        fi
        if [ -z "$OS_SSH_KEY_PATH" ]; then
            read -p "SSH密钥路径（可选，直接回车跳过）: " OS_SSH_KEY_PATH
        fi
        if [ -n "$OS_SSH_KEY_PATH" ] && [ -z "$OS_SSH_KEY_USER" ]; then
            read -p "SSH密钥用户 [root]: " input
            OS_SSH_KEY_USER=${input:-root}
        fi
        # 操作系统采集参数（默认0，表示只采集一次；>0表示多次采集）
        if [ "$OS_COLLECT_INTERVAL" -eq 0 ]; then
            read -p "操作系统采集间隔（秒，0=只采集一次，>0表示多次采集）[0]: " input
            OS_COLLECT_INTERVAL=${input:-0}
        fi
        # 只有在多次采集模式下才需要设置采集次数或总时长
        if [ "$OS_COLLECT_INTERVAL" -gt 0 ]; then
            # 采集次数和采集时间只能选一个，且必须二选一，且都必须>0
            local os_collect_mode=""
            while [ "$os_collect_mode" != "1" ] && [ "$os_collect_mode" != "2" ]; do
                echo "请选择操作系统采集限制方式："
                echo "  1) 按采集次数限制（必须>0）"
                echo "  2) 按总采集时间限制（秒，必须>0）"
                read -p "选择 [1/2]: " os_collect_mode
            done
            
            case "$os_collect_mode" in
                1)
                    while true; do
                        read -p "请输入采集次数（正整数，必须>0）: " input
                        if [[ "$input" =~ ^[1-9][0-9]*$ ]]; then
                            OS_COLLECT_COUNT=$input
                            OS_COLLECT_DURATION=0
                            break
                        else
                            echo "输入无效，请输入大于0的整数。"
                        fi
                    done
                    ;;
                2)
                    while true; do
                        read -p "请输入总采集时间（秒，正整数，必须>0）: " input
                        if [[ "$input" =~ ^[1-9][0-9]*$ ]]; then
                            OS_COLLECT_DURATION=$input
                            OS_COLLECT_COUNT=0
                            break
                        else
                            echo "输入无效，请输入大于0的整数。"
                        fi
                    done
                    ;;
            esac
        fi
    else
        # 本地模式：不需要SSH参数，但仍然需要配置OS采集参数
        log INFO "本地模式：跳过SSH连接参数输入，仅配置操作系统采集参数"
        # 操作系统采集参数（默认0，表示只采集一次；>0表示多次采集）
        if [ "$OS_COLLECT_INTERVAL" -eq 0 ]; then
            read -p "操作系统采集间隔（秒，0=只采集一次，>0表示多次采集）[0]: " input
            OS_COLLECT_INTERVAL=${input:-0}
        fi
        # 只有在多次采集模式下才需要设置采集次数或总时长
        if [ "$OS_COLLECT_INTERVAL" -gt 0 ]; then
            # 采集次数和采集时间只能选一个，且必须二选一，且都必须>0
            local os_collect_mode=""
            while [ "$os_collect_mode" != "1" ] && [ "$os_collect_mode" != "2" ]; do
                echo "请选择操作系统采集限制方式："
                echo "  1) 按采集次数限制（必须>0）"
                echo "  2) 按总采集时间限制（秒，必须>0）"
                read -p "选择 [1/2]: " os_collect_mode
            done
            
            case "$os_collect_mode" in
                1)
                    while true; do
                        read -p "请输入采集次数（正整数，必须>0）: " input
                        if [[ "$input" =~ ^[1-9][0-9]*$ ]]; then
                            OS_COLLECT_COUNT=$input
                            OS_COLLECT_DURATION=0
                            break
                        else
                            echo "输入无效，请输入大于0的整数。"
                        fi
                    done
                    ;;
                2)
                    while true; do
                        read -p "请输入总采集时间（秒，正整数，必须>0）: " input
                        if [[ "$input" =~ ^[1-9][0-9]*$ ]]; then
                            OS_COLLECT_DURATION=$input
                            OS_COLLECT_COUNT=0
                            break
                        else
                            echo "输入无效，请输入大于0的整数。"
                        fi
                    done
                    ;;
            esac
        fi
    fi
}

# ==================== 主函数 ====================

main() {
    # 解析命令行参数
    parse_args "$@"
    
    # 检查必需的命令
    if ! check_required_commands; then
        exit 1
    fi
    
    # 检查是否需要交互式输入（固定同时采集数据库和操作系统）
    local need_interactive=false
    
    # Oracle 必要参数检查
    if [ "$LOCAL_MODE" = "true" ]; then
        if [ -z "$DB_USERNAME" ] || [ -z "$DB_PASSWORD" ] || [ -z "$DBNAME" ]; then
            need_interactive=true
        fi
    else
        if [ -z "$DB_HOST" ] || [ -z "$DB_PORT" ] || [ -z "$DB_USERNAME" ] || [ -z "$DB_PASSWORD" ] || [ -z "$DBNAME" ]; then
            need_interactive=true
        fi
    fi
    
    # OS 必要参数检查
    if [ "$LOCAL_MODE" != "true" ]; then
        # 远程模式下，OS 连接参数必须具备；如果信息不足，则进入交互模式补全
        if [ -z "$OS_HOST" ]; then
            if [ -n "$DB_HOST" ]; then
                # 没有显式传入 OS_HOST，但 DB_HOST 已提供，可以直接复用
                OS_HOST=$DB_HOST
            else
                need_interactive=true
            fi
        fi
        
        # OS_PORT 在远程模式下也视为必选，如果未设置则通过交互补充
        if [ -z "$OS_PORT" ]; then
            need_interactive=true
        fi
        
        # OS 用户名/密码不能默认等于数据库用户/密码，如果缺失则必须通过交互输入
        if [ -z "$OS_USERNAME" ] || [ -z "$OS_PASSWORD" ]; then
            need_interactive=true
        fi
        
        # 远程模式下，OS 采集参数也是必选：
        # - 必须明确指定采集间隔 OS_COLLECT_INTERVAL（>0 表示多次采集，0 表示只采集一次）
        # - 如果是多次采集（interval>0），则必须通过 duration 或 count 二选一，并且 >0
        if [ "$OS_COLLECT_INTERVAL" -le 0 ]; then
            # 未指定有效的采集间隔，进入交互补全
            need_interactive=true
        else
            # 多次采集场景下，检查 duration / count 是否完整有效，否则进入交互补全
            if [ "$OS_COLLECT_COUNT" -le 0 ] && [ "$OS_COLLECT_DURATION" -le 0 ]; then
                need_interactive=true
            fi
            if [ "$OS_COLLECT_COUNT" -gt 0 ] && [ "$OS_COLLECT_DURATION" -gt 0 ]; then
                need_interactive=true
            fi
        fi
    else
        # 本地模式下，必须显式指定 OS 采集参数：
        # - 必须指定 os-collect-interval (>0，表示多次采集)
        # - 必须指定 os-collect-duration 或 os-collect-count（两者二选一，且>0）
        if [ "$OS_COLLECT_INTERVAL" -le 0 ]; then
            need_interactive=true
        else
            if [ "$OS_COLLECT_COUNT" -le 0 ] && [ "$OS_COLLECT_DURATION" -le 0 ]; then
                need_interactive=true
            fi
            if [ "$OS_COLLECT_COUNT" -gt 0 ] && [ "$OS_COLLECT_DURATION" -gt 0 ]; then
                need_interactive=true
            fi
        fi
    fi
    
    # 如果需要交互式输入
    if [ "$need_interactive" = true ]; then
        interactive_input
    fi
    
    # 再次验证必需参数（固定同时采集）
    # Oracle 参数校验
    if [ "$LOCAL_MODE" = "true" ]; then
        if [ -z "$DB_USERNAME" ] || [ -z "$DB_PASSWORD" ] || [ -z "$DBNAME" ]; then
            log ERROR "需要提供数据库用户名、密码和实例名(SID)"
            exit 1
        fi
    else
        if [ -z "$DB_HOST" ] || [ -z "$DB_PORT" ] || [ -z "$DB_USERNAME" ] || [ -z "$DB_PASSWORD" ] || [ -z "$DBNAME" ]; then
            log ERROR "需要提供数据库连接参数（主机、端口、用户名、密码和服务名/实例名）"
            exit 1
        fi
    fi
    
    # OS 参数校验（本地模式不需要远程 OS 地址等）
    if [ "$LOCAL_MODE" != "true" ]; then
        if [ -z "$OS_HOST" ] && [ -z "$DB_HOST" ]; then
            log ERROR "需要提供操作系统主机地址（或使用 --local 进行本地采集）"
            exit 1
        fi
        OS_HOST=${OS_HOST:-$DB_HOST}
        OS_USERNAME=${OS_USERNAME:-$DB_USERNAME}
        OS_PASSWORD=${OS_PASSWORD:-$DB_PASSWORD}
    fi
    
    # OS 多次采集参数校验：不支持无限采集，duration / count 必须>0，且二选一
    if [ "$LOCAL_MODE" = "true" ]; then
        # 本地模式：要求显式配置多次采集参数
        if [ "$OS_COLLECT_INTERVAL" -le 0 ]; then
            log ERROR "本地模式下必须指定 --os-collect-interval（>0）以及 --os-collect-duration 或 --os-collect-count（两者二选一且>0）"
            exit 1
        fi
        if [ "$OS_COLLECT_COUNT" -le 0 ] && [ "$OS_COLLECT_DURATION" -le 0 ]; then
            log ERROR "本地模式下，在多次采集模式中必须通过 --os-collect-count 或 --os-collect-duration 指定一个大于0的限制"
            exit 1
        fi
        if [ "$OS_COLLECT_COUNT" -gt 0 ] && [ "$OS_COLLECT_DURATION" -gt 0 ]; then
            log ERROR "本地模式下不能同时设置 --os-collect-count 和 --os-collect-duration，请二选一"
            exit 1
        fi
    else
        # 远程模式：仅在 interval>0（多次采集）时强制 duration/count 规则
        if [ "$OS_COLLECT_INTERVAL" -gt 0 ]; then
            if [ "$OS_COLLECT_COUNT" -le 0 ] && [ "$OS_COLLECT_DURATION" -le 0 ]; then
                log ERROR "在多次采集模式下，必须通过 --os-collect-count 或 --os-collect-duration 指定一个大于0的限制，不支持无限采集"
                exit 1
            fi
            if [ "$OS_COLLECT_COUNT" -gt 0 ] && [ "$OS_COLLECT_DURATION" -gt 0 ]; then
                log ERROR "不能同时设置 --os-collect-count 和 --os-collect-duration，请二选一"
                exit 1
            fi
        fi
    fi
    
    # 初始化日志
    init_logger "$LOG_DIR" "$LOG_LEVEL"
    
    # 确保数据目录存在
    mkdir -p "$DATA_DIR"
    
    # 构建数据库连接字符串（固定采集数据库）
    local db_conn_str=""
    db_conn_str=$(build_sqlplus_conn "$DB_HOST" "$DB_PORT" "$DB_USERNAME" "$DB_PASSWORD" "$DBNAME")
    
    # 检查build_sqlplus_conn是否成功（本地模式下如果使用SERVICE_NAME会返回错误）
    if [ $? -ne 0 ]; then
        log ERROR "构建数据库连接字符串失败，脚本退出"
        exit 1
    fi
    
    log DEBUG "数据库连接字符串: $db_conn_str"
    
    # 测试数据库连接
    if ! test_db_connection "$db_conn_str"; then
        log ERROR "数据库连接失败，脚本退出"
        exit 1
    fi
    
    # 测试SSH连接（本地模式下跳过）
    if [ "$LOCAL_MODE" != "true" ]; then
        if ! test_ssh_connection "$OS_HOST" "$OS_PORT" "$OS_USERNAME" "$OS_PASSWORD" "$OS_SSH_KEY_PATH"; then
            log ERROR "SSH连接失败，脚本退出"
            exit 1
        fi
    else
        log INFO "本地模式：跳过SSH连接测试"
    fi
    
    # 输出采集配置信息
    log INFO "============================================================"
    log INFO "采集配置信息"
    log INFO "============================================================"
    
    # 固定同时采集 Oracle 和 OS 的配置信息
    log INFO "【Oracle数据库采集配置】"
    if [ "$LOCAL_MODE" = "true" ]; then
        log INFO "  连接模式: 本地模式"
        log INFO "  用户名: $DB_USERNAME"
        log INFO "  实例名(SID): $DBNAME"
    else
        log INFO "  连接模式: 远程模式"
        log INFO "  主机地址: $DB_HOST"
        log INFO "  端口: $DB_PORT"
        log INFO "  用户名: $DB_USERNAME"
        log INFO "  服务名(SERVICE_NAME): $DBNAME"
    fi
    
    log INFO "【操作系统采集配置】"
    if [ "$LOCAL_MODE" = "true" ]; then
        log INFO "  采集模式: 本地模式（无需SSH）"
    else
        log INFO "  采集模式: 远程模式（SSH）"
        log INFO "  主机地址: $OS_HOST"
        log INFO "  端口: $OS_PORT"
        log INFO "  用户名: $OS_USERNAME"
    fi
    
    log INFO "============================================================"
    
    # 初始化数据存储
    local data_file="${TMP_DIR}/oracle_data.txt"
    init_data_structure "$data_file" "$db_conn_str"
    
    # 获取并验证数据库日志文件路径（在采集开始阶段，与Python版本一致）
    local db_log_file_path=""
    db_log_file_path=$(get_db_log_path "$db_conn_str")
    
    # 验证告警日志文件是否存在（使用SSH在远程服务器上验证，本地模式直接验证）
    if [ -n "$db_log_file_path" ]; then
        # 检查文件是否存在（本地模式直接检查，远程模式使用SSH）
        local check_cmd="test -f \"$db_log_file_path\" && echo \"exists\" || echo \"not_exists\""
        local check_result=""
        if [ "$LOCAL_MODE" = "true" ]; then
            # 本地模式直接执行命令
            check_result=$(eval "$check_cmd")
        else
            # 远程模式使用SSH
            check_result=$(execute_ssh "$OS_HOST" "$OS_PORT" "$OS_USERNAME" "$OS_PASSWORD" "$OS_SSH_KEY_PATH" "$check_cmd")
        fi
        
        local file_exists=false
        if [ -n "$check_result" ]; then
            # 清理返回结果：去除首尾空白，取最后一行（防止有多行输出）
            local last_line=$(echo "$check_result" | tail -1 | xargs)
            # 检查是否包含 "exists"（更健壮的检查方式）
            if echo "$last_line" | grep -qi "exists" && ! echo "$last_line" | grep -qi "not_exists"; then
                file_exists=true
            fi
        fi
        
        if [ "$file_exists" = false ]; then
            # 文件不存在，提示用户输入
            log WARNING "告警日志文件不存在: $db_log_file_path"
            echo ""
            echo "警告：告警日志文件不存在: $db_log_file_path"
            
            # 循环提示用户输入，直到输入的文件存在或用户不输入
            while true; do
                echo "请手动输入告警日志文件的完整路径（直接回车则跳过，日志文件位置将置空）："
                read -r user_input
                user_input=$(echo "$user_input" | xargs)
                
                if [ -z "$user_input" ]; then
                    # 用户未输入，置空并退出循环
                    db_log_file_path=""
                    log WARNING "用户未输入告警日志文件路径，日志文件位置已置空"
                    echo "未输入告警日志文件路径，日志文件位置已置空"
                    break
                fi
                
                # 验证用户输入的文件是否存在（本地模式直接检查，远程模式使用SSH）
                local check_cmd_user="test -f \"$user_input\" && echo \"exists\" || echo \"not_exists\""
                local check_result_user=""
                if [ "$LOCAL_MODE" = "true" ]; then
                    # 本地模式直接执行命令
                    check_result_user=$(eval "$check_cmd_user")
                else
                    # 远程模式使用SSH
                    check_result_user=$(execute_ssh "$OS_HOST" "$OS_PORT" "$OS_USERNAME" "$OS_PASSWORD" "$OS_SSH_KEY_PATH" "$check_cmd_user")
                fi
                
                local file_exists_user=false
                if [ -n "$check_result_user" ]; then
                    local last_line_user=$(echo "$check_result_user" | tail -1 | xargs)
                    if echo "$last_line_user" | grep -qi "exists" && ! echo "$last_line_user" | grep -qi "not_exists"; then
                        file_exists_user=true
                    fi
                fi
                
                if [ "$file_exists_user" = true ]; then
                    # 文件存在，使用用户输入的路径
                    db_log_file_path="$user_input"
                    log INFO "用户输入的告警日志文件路径已验证存在: $db_log_file_path"
                    echo "告警日志文件路径已设置为: $db_log_file_path"
                    break
                else
                    # 文件不存在，继续循环提示
                    log WARNING "用户输入的文件路径不存在: $user_input，请重新输入"
                    echo "警告：输入的文件路径不存在: $user_input，请重新输入"
                fi
            done
        else
            # 文件存在
            log INFO "告警日志文件存在: $db_log_file_path"
        fi
    fi
    
    # 保存数据库日志文件路径到临时文件（供后续OS采集使用）
    if [ -n "$db_log_file_path" ]; then
        echo "$db_log_file_path" > "${TMP_DIR}/db_log_path.txt"
        log INFO "数据库日志文件路径已保存: $db_log_file_path"
    else
        echo "" > "${TMP_DIR}/db_log_path.txt"
        log WARNING "数据库日志文件路径为空"
    fi
    
    # 执行采集（与Python版本一致：先OS多次采集，再OS单次采集，最后数据库采集）
    # OS采集：根据interval决定单次还是多次采集
    if [ "$OS_COLLECT_INTERVAL" -eq 0 ]; then
        # 单次采集模式：先执行OS多次采集（只执行一次），再执行OS单次采集
        log INFO "开始操作系统多次采集（单次模式）"
        collect_os_data_multiple "$OS_HOST" "$OS_PORT" "$OS_USERNAME" "$OS_PASSWORD" "$OS_SSH_KEY_PATH" "1"
        
        log INFO "开始操作系统单次采集"
        collect_os_data_once "$OS_HOST" "$OS_PORT" "$OS_USERNAME" "$OS_PASSWORD" "$OS_SSH_KEY_PATH"
        
    else
        # 多次采集模式：先执行OS多次采集循环，再执行OS单次采集
        log INFO "开始操作系统多次采集（间隔: ${OS_COLLECT_INTERVAL}秒）"
        
        # 执行多次采集循环
        local start_time=$(date +%s)
        local collect_count=0
        
        while true; do
            collect_count=$((collect_count + 1))
            collect_os_data_multiple "$OS_HOST" "$OS_PORT" "$OS_USERNAME" "$OS_PASSWORD" "$OS_SSH_KEY_PATH" "$collect_count"
            
            # 检查是否达到采样点个数限制
            if [ "$OS_COLLECT_COUNT" -gt 0 ] && [ "$collect_count" -ge "$OS_COLLECT_COUNT" ]; then
                log INFO "操作系统采集达到采样点个数限制: ${OS_COLLECT_COUNT}个"
                break
            fi
            
            # 检查是否达到总时长限制
            if [ "$OS_COLLECT_DURATION" -gt 0 ]; then
                local current_time=$(date +%s)
                local elapsed_time=$((current_time - start_time))
                if [ "$elapsed_time" -ge "$OS_COLLECT_DURATION" ]; then
                    log INFO "操作系统采集达到总时长限制: ${OS_COLLECT_DURATION}秒"
                    break
                fi
            fi
            
            # 等待指定间隔
            sleep "$OS_COLLECT_INTERVAL"
        done
        
        log INFO "操作系统多次采集完成，共采集 ${collect_count} 次"
        
        # 多次采集完成后，执行OS单次采集
        log INFO "开始操作系统单次采集"
        collect_os_data_once "$OS_HOST" "$OS_PORT" "$OS_USERNAME" "$OS_PASSWORD" "$OS_SSH_KEY_PATH"
    fi
    
    # 最后执行数据库采集（与Python版本一致）
    log INFO "开始Oracle数据库采集"
    collect_oracle_data "$db_conn_str" "$data_file"
    
    # 保存数据
    save_oracle_data "$data_file" "$DATA_DIR"
    
    # 合并OS和Oracle采集数据（最终合并，固定执行）
    local db_name_for_merge="${DBNAME:-oracle}"
    local final_result_file=""
    final_result_file=$(merge_all_data "$DATA_DIR" "$db_name_for_merge" "$PROGRAM_START_TIME" 2>/dev/null | tail -1)
    if [ -n "$final_result_file" ] && [ -f "$final_result_file" ]; then
        log INFO "OS和Oracle数据合并成功"
    else
        log WARNING "OS和Oracle数据合并失败，但脚本继续执行"
    fi
    
    # 清理中间文件，只保留最终结果文件和日志文件
    cleanup_intermediate_files "$DATA_DIR" "$final_result_file" "$LOG_FILE"
    
    log INFO "脚本执行完成"
}

# 执行主函数
main "$@"
