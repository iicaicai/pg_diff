# PostgreSQL Data Upgrade Verification Tool

这是一个用于在 PostgreSQL 数据库大版本升级前后进行数据一致性校验的工具。它能够对数据进行备份，并对比升级前后的数据条数、主键 ID 以及内容校验和，生成详细的差异报告（Excel 格式）。

## 功能特性

*   **自动备份**: 调用 Docker 容器内的 `pg_dump` 对数据库进行物理备份。
*   **数据快照**: 统计指定数据库中所有业务表（排除系统表）的数据行数及主键集合。
*   **差异对比**: 对比升级前后的数据快照，快速发现数据丢失、新增或异常变化。
*   **内容哈希校验**: 通过计算全表 checksum 检测行数不变但内容改变的情况。
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
python3 pg_diff_tool.py backup --db-name <database_name> [OPTIONS]
```

**参数说明**:

*   **基础参数**:
    *   `--db-name`: (必填) 数据库名称 (例如: `logto`).
    *   `--db-url`: (可选) 连接字符串或 DSN。支持 URI (如 `postgresql://user:pass@host:port`) 或键值对 (如 `host=127.0.0.1 user=root`)。默认优先读取环境变量 `PG_DB_URL`，否则默认为 `postgresql://127.0.0.1:5432`。

*   **运行模式 (Docker vs Local)**:
    *   `--container-name`: (可选) Docker 容器名称，默认为 `docker-tmp-postgres-1`。
    *   `--local`: (可选) 开启此开关后，将直接调用宿主机的 `pg_dump`，而不使用 Docker exec。
    *   `--db-user`: (可选) 指定用于备份的用户名 (默认为 `postgres`)。

*   **性能与输出**:
    *   `--threads`: (可选) 并行扫描线程数，默认为 4。
    *   `--dump-file`: (可选) 指定 SQL 备份文件名。
    *   `--skip-dump`: (可选) 跳过物理备份，仅统计数据。

**示例**:

1.  **Docker 模式 (默认)**:
    ```bash
    python3 pg_diff_tool.py backup --db-name logto --container-name my-postgres-db
    ```

2.  **本地模式 (无密码/证书认证)**:
    ```bash
    # 假设已配置好 ~/.pgpass 或证书，直接指定 host 和 user
    python3 pg_diff_tool.py backup --db-name logto --local --db-url "host=127.0.0.1 user=root" --db-user root
    ```

### 2. 升级后：对比校验 (Compare)

在数据库升级完成并导入数据后，运行此命令进行校验。

```bash
python3 pg_diff_tool.py compare --db-name <database_name> [OPTIONS]
```

**参数说明**:
*   `--db-url`, `--threads`: 同上。
*   `--snapshot-file`: (可选) 指定快照文件，默认为 `migration_snapshot.json`。
*   `--output`: (可选) 指定输出的 Excel 报告文件名。

**示例**:
```bash
python3 pg_diff_tool.py compare --db-name logto --db-url "host=127.0.0.1 user=root"
```

### 3. 查看报告

生成的 Excel 报告 (`upgrade_diff_report.xlsx`) 包含两个 Sheet 页：

#### Sheet 1: Summary (概览)
包含所有表的对比结果：
*   `Schema`, `Table`: 表信息
*   `Before Count`, `After Count`: 升级前后行数
*   `Is Change`: 是否有变化 (Y/N)
*   `Change Type`: 变化类型简述

#### Sheet 2: Diff Details (差异详情)
仅列出有差异的表及其具体变化：
*   `Change Type`: 变化类型（Missing IDs, Added IDs, Count Mismatch, Content Mismatch）
*   `IDs/Details`: 具体缺失/新增的主键 ID 列表，或内容校验和不一致的说明

### 4. 快照文件说明 (migration_snapshot.json)

`migration_snapshot.json` 是 `backup` 阶段生成的中间文件，用于持久化存储数据库的"指纹"信息，以便在升级后进行比对。请勿手动修改此文件。

**文件结构示例**:
```json
{
  "public.users": {
    "count": 105,                       // 数据行数
    "checksum": "-6649525406097391557", // 全局内容校验和 (防止行数一样但内容变了)
    "pks": ["user_01", "user_02", ...], // 主键 ID 列表 (用于精确识别哪条数据丢失/新增)
    "pk_col": "id",                     // 主键列名
    "error": null                       // 扫描时的错误信息 (如有)
  },
  ...
}
```

## 实现原理

### 工作流程图

```mermaid
graph TD
    A[用户指令] --> B{选择模式}
    
    subgraph "Backup (升级前)"
    B -- Backup --> C[物理备份 (pg_dump)]
    C --> D[初始化连接池]
    D --> E[获取表清单]
    E --> F[多线程并行扫描]
    F --> G[统计行数 & 获取主键集合]
    G --> H[生成快照 JSON]
    end
    
    subgraph "Compare (升级后)"
    B -- Compare --> I[加载旧快照 JSON]
    I --> J[初始化连接池]
    J --> K[多线程并行扫描当前库]
    K --> L[统计行数 & 获取主键集合]
    L --> M[对比新旧数据]
    M --> N[计算差异 (新增/缺失 ID)]
    N --> O[生成 Excel 报告]
    end
```

### 核心机制

1.  **多线程并行扫描**: 
    *   工具使用 `ThreadPoolExecutor` 创建线程池，配合 `psycopg2.pool.ThreadedConnectionPool` 管理数据库连接。
    *   主线程获取所有业务表清单后，将任务分发给子线程。每个子线程负责一张表的 `COUNT(*)` 和 `SELECT PrimaryKey` 查询，极大提高了对海量表的扫描效率。

2.  **主键指纹对比**:
    *   为了不仅知道"数据量变了"，还能知道"哪条数据变了"，工具会提取每张表的主键值（IDs）。
    *   通过 Python 的集合（Set）运算（`Old_IDs - New_IDs` 和 `New_IDs - Old_IDs`），快速计算出缺失（Missing）和新增（Added）的具体记录 ID。

3.  **内容哈希校验 (Content Checksum)**:
    *   为了防止"行数一样、主键一样，但内容变了"的场景，工具对每张表计算一个全局内容校验和。
    *   **算法**: `SUM( ('x' || SUBSTR(MD5(row::text), 1, 16))::bit(64)::bigint )`
    *   即：对每行数据转换为文本后计算 MD5，取前 64 位作为整数，然后对全表求和。该算法与行顺序无关，能高效、准确地检测数据内容变化。

4.  **容器化兼容**:
    *   针对运行在 Docker 容器中的数据库，工具通过 `docker exec` 通道直接调用容器内部的原生 `pg_dump` 工具，无需在宿主机安装特定版本的 PostgreSQL 客户端，保证了备份工具版本与数据库版本的严格一致。

## 常见问题

**Q: 提示 `pg_dump` 命令未找到?**
A: 本工具默认尝试通过 `docker exec` 调用容器内的 `pg_dump`。请确保您的数据库容器正在运行，并且脚本中的 `CONTAINER_NAME` 变量设置正确（脚本会自动连接默认配置，如需修改请编辑脚本中的配置项）。

**Q: 如何连接其他主机或端口?**
A: 通过 `--db-url` 参数指定，例如: `--db-url postgres:password@192.168.1.100:5433`。

**Q: 数据量非常大怎么办?**
A: 
1. 使用 `--threads` 参数开启多线程扫描。
2. 脚本目前会拉取所有主键 ID 进行对比，对于亿级数据大表可能会消耗较多内存。建议在测试环境验证性能，或针对特定大表进行单独处理。
