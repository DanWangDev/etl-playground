# M04: 精炼输出 — 分区 Parquet

## 学习目标

- 如何选择与查询模式对齐的分区键
- 小文件问题及其避免方法
- Repartition vs Coalesce — 何时使用
- Hive 风格分区布局
- DuckDB 如何编目 Parquet 文件（Glue Catalog 等价）

## 概述

转换后，数据进入**精炼区**——数据湖的查询优化层。关键决策是分区。

## 核心概念

### 1. 分区键选择

**好的键**：低基数，与查询过滤器对齐。

| 键 | 基数 | 好？ | 原因 |
|-----|------|------|------|
| `event_date` | ~365/年 | ✅ | 时间范围查询按日期过滤 |
| `region` | 4 | ✅ | 地理过滤器：`WHERE region = 'UK'` |
| `customer_id` | 100K+ | ❌ | 太多唯一值 → 数百万微小文件 |
| `content_id` | 1K | ❌ | 不均匀分布（幂律）→ 数据倾斜 |

**组合基数**：30 天 × 4 个地区 = **约 120 个分区/月** — 可管理且查询对齐。

### 2. 小文件问题

如果按 `customer_id` 分区（100K 个唯一值），每个文件都很小（< 128 MB）。Hadoop/Spark 每个文件的元数据开销约 150 字节。100K 个文件 → 仅列出目录就需要 15 MB 元数据。查询规划变得缓慢。

### 3. Repartition vs Coalesce

| 操作 | Shuffle？ | 使用场景 |
|------|----------|---------|
| `repartition(N)` | 是（完全 Shuffle） | 增加分区数或更改分区列 |
| `coalesce(N)` | 否（节点内合并） | 减少分区数，N < 当前值 |

### 4. Hive 风格分区布局

`event_date=2026-08-01/region=UK/part-00000.parquet` 模式被 Hive、Spark、DuckDB 和 Athena 识别。查询 `WHERE event_date = '2026-08-01'` 时，引擎仅读取该目录——**分区裁剪**。

## 演练

```bash
uv run python -m etl_playground.m04_curated_output.exercise
```

## 面试关联

- "如何为数据湖选择分区键？"
- "什么是小文件问题，如何避免？"
- "为什么 customer_id 是不好的分区键？"
- "repartition 和 coalesce 有什么区别？"

## 下一步

→ [M05: 性能基准](../m05_benchmark/concept_cn.md)
