# M07: 湖查询 — DuckDB 作为 "Athena"

## 学习目标

- 如何直接查询 Parquet 文件而无需加载到数据库
- 谓词下推：过滤器如何在存储层应用
- 使用 `EXPLAIN ANALYZE` 衡量扫描量
- 为什么数据湖支持仓库无法提供的即席探索

## 概述

数据湖（精炼 Parquet 文件）可直接查询——无需 ETL 加载到仓库。DuckDB 直接读取 Parquet 文件，自动应用分区裁剪、列裁剪和谓词下推。

## 核心概念

### 1. 读时 Schema

湖不在写入时强制执行 Schema（除了 Parquet 的列类型）。查询时，DuckDB 从 Parquet 元数据推断 Schema。这是**读时 Schema**——灵活，适合探索，但你需要知道存在哪些列。仓库（M08）使用**写时 Schema**——在加载数据之前通过显式 DDL 创建表。

### 2. 谓词下推

`WHERE region = 'UK'` 时，DuckDB 将此过滤器下推到 Parquet 读取器。读取器使用 Parquet 内部统计信息（每个行组的最小/最大值）跳过不可能匹配的行组。

### 3. 湖 vs 仓库

| | 数据湖 (M07) | 数据仓库 (M08) |
|---|---|---|
| **Schema** | 读时 Schema | 写时 Schema (DDL) |
| **用例** | 即席查询、数据科学 | 可重复 BI、KPI |
| **新鲜度** | 即时（文件落地即可查） | ETL 加载步骤后 |
| **工具** | DuckDB `read_parquet()` | DuckDB DDL/DML |

两者从同一数据源服务不同的消费者。

## 演练

```bash
uv run python -m etl_playground.m07_lake_queries.exercise
```

## 面试关联

- "读时 Schema 和写时 Schema 有什么区别？"
- "为什么直接查询湖而不是仓库？"
- "谓词下推如何提高查询性能？"
- "何时使用 Athena 而非 Redshift？"

## 下一步

→ [M08: 星型模型仓库](../m08_warehouse/concept_cn.md)
