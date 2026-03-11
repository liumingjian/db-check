# Docker E2E

该目录用于端到端 Docker 集成测试。

- `docker/docker-compose.yml`：公共测试环境配置（MySQL 公共环境 + OS target）
- `docker/docker-compose.mysql56.yml`：MySQL 5.6 覆盖配置（`linux/amd64`）
- `docker/docker-compose.mysql57.yml`：MySQL 5.7 覆盖配置（`linux/amd64`）
- `docker/docker-compose.mysql80.yml`：MySQL 8.0 覆盖配置
- `docker/mysql/init.sql`：MySQL 初始化脚本（含结构与权限）
- `docker/mysql/scenarios.sql`：SQL 场景用例（慢查询/全扫等）
- `docker/mysql/apply_scenarios.sh`：运行时场景注入（含锁等待/认证失败）
- `run_docker_e2e.sh`：一键执行 collector -> analyzer -> reporter -> contracts 校验
- `test_docker_e2e.py`：`unittest` 包装（通过 `DBCHECK_RUN_DOCKER_E2E=1` 启用）

执行前必须激活虚拟环境（`VIRTUAL_ENV` 必须存在）。

默认会顺序执行 `5.6`、`5.7`、`8.0` 三个版本；也可以按版本指定：

```bash
source .venv/bin/activate
tests/e2e/run_docker_e2e.sh --mysql-version 5.6 --mysql-version 5.7 --mysql-version 8.0
```
