# 数仓关联样本生成服务（Synthetic Referential Sample Service）

面向数仓测试工程师，生成**不含任何真实患者值**的关联样本，用于装载回归测试。
服务只读源库的**表结构（系统目录）**，从不读取任何行数据；用 z3 求解出同时满足
指定行数、各整数列闭区间与全部约束的合成数据，渲染为建表 + 数据 + 约束的 SQL 包，
并在**同结构的隔离空库实际装载通过、且约束负向探测全部生效后**才开放下载。

## 能力边界（明确支持 / 明确拒绝）

**支持的结构**

- 整数列：`smallint` / `integer` / `bigint`
- 主键（含复合主键）、普通 `UNIQUE` 唯一键（含复合）
- `NOT NULL` 与可空列，完整保留 NULL 语义（SQL 唯一键中多个 NULL 互不冲突）
- 复合外键 `MATCH SIMPLE` 与 `MATCH FULL`
- 自引用外键、跨表循环外键（含传递闭包）
- `NOT DEFERRABLE` 与 `DEFERRABLE [INITIALLY ...]` 外键，原样保留可延迟性

**明确拒绝（不会静默忽略）**：非整数列、`CHECK`、`EXCLUDE`、列 `DEFAULT`、
`IDENTITY`/生成列、触发器、独立唯一索引（部分/表达式索引）、视图、物化视图、
外部表、分区/继承表、写规则、`MATCH PARTIAL`、跨 schema 外键、`ON ... SET DEFAULT`。
拒绝时返回结构化原因（对象 + 理由）。

## 安全保证

- 源库连接强制 `default_transaction_read_only=on`、语句/锁超时；
- 内省只查询 `pg_catalog` / `information_schema`，绝不 `SELECT` 用户表行；
- 部署的 `app_ro` 角色对用户表无任何表级权限（实测读行返回 `42501`、写返回 `25006`）；
- 验证在独立的验证集群上为每个作业新建隔离空库，结束后删除；
- 不关闭任何约束、不把即时约束改写为延迟约束；循环外键通过「先建表→插数据→
  再 `ALTER TABLE ADD CONSTRAINT`」的结构方式装载；
- 失败不产出、不提供任何部分包，下载仅在 `SUCCEEDED` 后可用。

## 一键启动（Docker Compose）

```bash
docker compose up --build
# API:        http://localhost:8000
# Swagger 文档: http://localhost:8000/docs
# 源库(仅结构): localhost:55432   验证库: localhost:55433
```

源库容器首次启动时自动装载 `sql/00_demo_source_schema.sql`（循环引用检验表，零行数据）
并创建只读账号 `app_ro`。

## API 概览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 存活探针 |
| GET | `/schemas/{schema}/tables` | 列出普通基表（非基表关系被标注排除） |
| GET | `/schemas/{schema}/tables/{table}/structure` | 支持的结构视图；不支持时返回 422 与拒绝原因 |
| POST | `/jobs` | 提交生成+验证作业（后台执行），返回 `job_id` |
| GET | `/jobs/{job_id}` | 状态、无解冲突报告、逐步日志 |
| GET | `/jobs/{job_id}/download` | 仅在验证成功后下载 zip（含 5 个文件） |
| POST | `/migrations` | 输入两个成功作业号，返回验证过的单事务增量迁移脚本 |

### 请求体

每张表给 0～20 行，并为**每一列**声明整数闭区间 `[low, high]`：

```json
{
  "schema_name": "public",
  "tables": [
    {
      "table": "employee",
      "rows": 5,
      "ranges": {
        "emp_id":   {"low": 1000, "high": 1999},
        "dept_id":  {"low": 100,  "high": 199},
        "mentor_id": {"low": 1000, "high": 1999},
        "level":    {"low": 1,    "high": 5}
      }
    }
  ]
}
```

- 至多选择 4 张表；所选表的全部外键目标（含传递依赖）自动纳入闭包，
  闭包后仍不得超过 4 张，否则以 `closure` 冲突明确拒绝。
- 闭包自动带入的表按其整数类型边界取默认区间、默认 5 行。
- 无解时返回 `kind=no_solution`，并给出参与冲突的具体行数/区间/约束清单。

### 下载包内容

| 文件 | 内容 |
| --- | --- |
| `01_schema.sql` | `CREATE TABLE`，内联主键/唯一键（不含外键） |
| `02_data.sql` | 恰好等于请求行数的 `INSERT`，NULL 显式输出 |
| `03_constraints.sql` | 全部外键 `ALTER TABLE ADD CONSTRAINT`（处理循环），可延迟性子句原样保留 |
| `manifest.json` | 行数、区间、约束清单、`contains_real_patient_data=false` |
| `README.md` | 装载说明 |

装载：

```bash
psql "$EMPTY_DATABASE_DSN" -v ON_ERROR_STOP=1 \
  -f 01_schema.sql -f 02_data.sql -f 03_constraints.sql
```

## 增量装载回归：就地旧版 → 新版迁移（`POST /migrations`）

数仓做增量装载回归时，已经装好旧样本的库需要就地更新到新版。本服务在不触碰源库、
不读取任何用户行的前提下，用两个**验证成功**的作业号生成一份 PostgreSQL 迁移脚本。

### 明确支持的结构（其余一律 422 拒绝）

- 恰好两张表：一张**登记表**、一张**报告表**；
- 每张表只有**单列整数主键**（smallint/integer/bigint），没有其他 UNIQUE/主键约束；
- 报告表对登记表恰好有**一条**外键：单列、**可空**、**NOT DEFERRABLE**、
  **ON DELETE NO ACTION**，引用登记表主键；两表没有其他外键；
- 两个作业的结构必须逐列一致（列名、类型、顺序、可空性、主键与外键定义）。

### 请求与响应

```json
POST /migrations
{ "old_job_id": "…旧版成功作业…", "new_job_id": "…新版成功作业…" }
```

响应含 `registry_table` / `report_table`、每表 `insert/update/delete` 计数、
完整 `script` 以及隔离库 `verify_log`。作业号相同 422、作业不存在 404、
作业未成功 409、结构不合规 422、隔离库验证失败 409（不交付任何脚本）。

### 脚本的事务与并发语义

脚本是**单事务**（`BEGIN … COMMIT`），核心保证：

1. 事务开头对两张表加 `SHARE ROW EXCLUSIVE` 锁并设置 `lock_timeout`/
   `statement_timeout`：该锁与所有行写锁冲突，**并发 INSERT/UPDATE/DELETE 无法插入
   到核对与更新之间**，只能在事务提交后排队执行或因锁超时失败；
2. 在同一事务内把旧版、新版的完整快照装入 TEMP 表，对两张表用双向
   `EXCEPT ALL` 做**全量数据核对**（每一列、每一行，含 NULL 语义）；
3. 已是新版 → 成功且**不写入**（仅 NOTICE）；与旧版完全一致 → 执行渲染好的
   增删改；两版都不匹配 → `RAISE EXCEPTION` **整笔回滚拒绝**；
4. **外键检查全程开启**，不关闭触发器、不改可延迟性。DML 按
   报告 UPDATE → 登记 INSERT → 登记 UPDATE → 报告 INSERT → 报告 DELETE →
   登记 DELETE 的顺序排列，保证 NOT DEFERRABLE NO ACTION 外键在每条语句都满足
   （先释放子表引用，最后才能删除被引用的父行）；
5. 变更后再次双向 `EXCEPT ALL` 核对，结果不等于新版则同样整笔回滚。

### 交付前的隔离库验证（不通过不返回脚本）

`app/migverify.py` 在验证集群上新建三个互不复用的隔离空库：

| 场景 | 准备 | 期望 |
| --- | --- | --- |
| upgrade | 装入**旧包**后执行脚本 | 两表逐表逐行等于新版；外键负向探测 23503 生效 |
| already-new | 装入**新包**后执行脚本 | 成功且字节级无写入 |
| neither | 旧包基础上把可空外键列改成 NULL（第三态） | 脚本整笔拒绝（23000），回滚后第三态原样保留 |

### 演示数据与示例

`sql/02_demo_registry_report.sql` 在源库新增零行的 `dw_registry`（登记）/
`dw_report`（报告）表，结构严格落在上述支持范围内。两版请求示例：

- `examples/request_registry_report_old.json`
- `examples/request_registry_report_new.json`

启动后在 http://localhost:8000/docs 的 `POST /migrations` 直接试发，
响应中的 `script` 即为可在目标库执行（`psql -v ON_ERROR_STOP=1`）的迁移脚本。

## 循环引用检验表示例

`sql/00_demo_source_schema.sql` 构成如下循环图（全部整数列、零行）：

```
dept.mgr_emp        → employee.emp_id      ┐
employee.dept_id    → dept.dept_id         ┘ 跨表循环
employee.mentor_id  → employee.emp_id         自引用
project(dept_id,dept_code) MATCH FULL → dept(dept_id,dept_code)  复合外键
assignment(emp_id,project_id) → employee / project              复合主键+双外键
```

对应请求见 `examples/request_cyclic_four_tables.json`；无解示例见
`examples/request_unsat_conflict.json`；闭包扩展示例见
`examples/request_closure_single.json`。

## 本地自测（无需 Docker）

仓库测试用 `pgserver` 自带的 PostgreSQL 二进制在本机起两个实例：

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt pytest pgserver
# 准备源库（装结构、建只读角色）后：
VERIFY_CLEANUP=detach \
SOURCE_DSN="postgresql://app_ro:app_ro_pwd@/appdb?host=/tmp/pgdata/source" \
VERIFY_DSN="postgresql://postgres@/postgres?host=/tmp/pgdata/verify" \
PYTHONPATH=. .venv/bin/python -m pytest tests/
```

`VERIFY_CLEANUP`：
- `drop`（默认，Docker 镜像使用）：每个作业后 `DROP DATABASE`；
- `detach`：仅用于禁止 checkpoint 信号的受限沙箱，断开连接并将库改名隔离。
  标准 PostgreSQL 镜像上请保持 `drop`。

## 目录结构

```
app/            FastAPI + 内省 + z3 求解 + SQL 渲染 + 隔离库验证
  migration.py  增量迁移：结构校验、按主键差异计算、单事务迁移脚本渲染
  migverify.py  迁移脚本三场景隔离空库验证（升级/已是新版/两版都不匹配）
sql/            循环引用检验表源结构、登记/报告表演示表、源库初始化（只读角色）
examples/       循环四表、无解冲突、闭包扩展、登记/报告新旧两版请求示例
tests/          51 个单元/集成/端到端测试（真实 PostgreSQL）
docker-compose.yml / Dockerfile / requirements.txt
```
