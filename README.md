# PostgreSQL Data Upgrade Verification Tool

这是一个用于在 PostgreSQL 数据库大版本升级前后进行数据一致性校验的工具。它能够对数据进行备份，并对比升级前后的数据条数和主键 ID，生成详细的差异报告（Excel 格式）。

## 功能特性

*   **自动备份**: 调用 Docker 容器内的 `pg_dump` 对数据库进行物理备份。
*   **数据快照**: 统计指定数据库中所有业务表（排除系统表）的数据行数及主键集合。
*   **差异对比**: 对比升级前后的数据快照，快速发现数据丢失、新增或异常变化。
*   **多线程加速**: 支持多线程并行扫描，大幅提升大数据量、多表场景下的处理速度。
*   **详细报告**: 生成 Excel (`.xlsx`) 格式报告，包含"概览"和"差异详情"两个 Sheet 页。

## 环境要求

*   Python 3.6+
*   Docker (如果数据库运行在容器中)
*   PostgreSQL 数据库

## 安装依赖

在使用本工具前，请确保安装了必要的 Python 依赖包：

```bash
pip install -r requirements.txt
```

## 使用指南

脚本 `pg_diff_tool.py` 支持两个主要命令：`backup` 和 `compare`。

### 1. 升级前：备份与快照 (Backup)

在执行数据库升级之前，运行此命令进行数据备份和状态记录。

```bash
python3 pg_diff_tool.py backup --db-name <database_name> --db-url <connection_string> [--threads N]
```

**参数说明**:
*   `--db-name`: (必填) 需要备份的数据库名称 (例如: `logto`, `test`)。
*   `--db-url`: (可选) 数据库连接字符串，默认为 `postgres:caiLL2747@127.0.0.1:5432`。
*   `--threads`: (可选) 并行扫描的线程数，默认为 4。对于包含上千张表的数据库，建议适当调大此值（如 10 或 20）以加快扫描速度。
*   `--dump-file`: (可选) 指定 SQL 备份文件的名称。
*   `--skip-dump`: (可选) 如果只需要统计行数而不需要物理备份，可添加此参数。

**示例**:
```bash
python3 pg_diff_tool.py backup --db-name logto --threads 8
```
执行后，将生成：
1.  `.sql` 备份文件 (例如 `logto_backup_20240101_120000.sql`)
2.  `migration_snapshot.json` (数据行数及主键快照)

### 2. 升级后：对比校验 (Compare)

在数据库升级完成并导入数据后，运行此命令进行校验。

```bash
python3 pg_diff_tool.py compare --db-name <database_name> --db-url <connection_string> [--threads N]
```

**参数说明**:
*   `--snapshot-file`: (可选) 指定要对比的快照文件，默认为 `migration_snapshot.json`。
*   `--output`: (可选) 指定输出的 Excel 报告文件名，默认为 `upgrade_diff_report.xlsx`。
*   `--threads`: (可选) 并行扫描线程数，建议与 backup 时保持一致或根据负载调整。

**示例**:
```bash
python3 pg_diff_tool.py compare --db-name logto --threads 8
```

### 3. 查看报告

生成的 Excel 报告 (`upgrade_diff_report.xlsx`) 包含两个 Sheet 页：

#### Sheet 1: Summary (概览)
包含所有表的对比结果：
*   `Schema`, `Table`: 表信息
*   `Before Count`, `After Count`: 升级前后行数
*   `Is Change`: 是否有变化 (Y/N)

#### Sheet 2: Diff Details (差异详情)
仅列出有差异的表及其具体变化：
*   `Change Type`: 变化类型（Missing IDs, Added IDs, Count Mismatch）
*   `IDs`: 具体缺失或新增的主键 ID 列表（列表过长时会自动截断）

## 常见问题

**Q: 提示 `pg_dump` 命令未找到?**
A: 本工具默认尝试通过 `docker exec` 调用容器内的 `pg_dump`。请确保您的数据库容器正在运行，并且脚本中的 `CONTAINER_NAME` 变量设置正确（脚本会自动连接默认配置，如需修改请编辑脚本中的配置项）。

**Q: 如何连接其他主机或端口?**
A: 通过 `--db-url` 参数指定，例如: `--db-url postgres:password@192.168.1.100:5433`。

**Q: 数据量非常大怎么办?**
A: 
1. 使用 `--threads` 参数开启多线程扫描。
2. 脚本目前会拉取所有主键 ID 进行对比，对于亿级数据大表可能会消耗较多内存。建议在测试环境验证性能，或针对特定大表进行单独处理。
