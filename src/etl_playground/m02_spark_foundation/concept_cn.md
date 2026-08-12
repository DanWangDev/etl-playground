# M02: PySpark 基础

## 学习目标

- 理解转换与动作的区别（惰性求值）
- 如何使用 `StructType` 定义显式 Schema
- 如何验证数据并捕获被拒绝的记录
- 使用 `explain()` 阅读 Spark 执行计划
- 为什么 Spark 在执行前构建 DAG

## 概述

这是你第一次动手接触 PySpark。与使用 pandas 直接读取 CSV 不同，你将体验 Spark 的定义特性：**惰性求值**。

```
┌─────────────────────────────────────────────────────────────┐
│                  SPARK 基础 (M02)                            │
│                                                             │
│  data/raw/viewing_events/.../events.csv                     │
│          │                                                  │
│          ▼                                                  │
│  ┌──────────────────┐                                       │
│  │  spark.read.csv  │  ← 惰性：构建逻辑计划                  │
│  │  带 Schema       │    尚未读取数据                        │
│  └────────┬─────────┘                                       │
│           │                                                 │
│           ▼                                                 │
│  ┌──────────────────┐                                       │
│  │  .filter(...)    │  ← 惰性：向 DAG 添加 Filter 节点      │
│  │  .select(...)    │  ← 惰性：向 DAG 添加 Project 节点     │
│  └────────┬─────────┘                                       │
│           │                                                 │
│           ▼                                                 │
│  ┌──────────────────┐                                       │
│  │  .count()        │  ← 动作：触发执行！                   │
│  │  .write.parquet  │  ← 动作：运行完整 DAG                 │
│  └──────────────────┘                                       │
│                                                             │
│  被拒绝的行 → data/rejected/ (死信队列)                      │
└─────────────────────────────────────────────────────────────┘
```

## 核心概念

### 1. 惰性求值

这是最重要的 Spark 概念。转换（`.filter()`、`.select()`、`.withColumn()`）不会立即执行——它们只是构建计划。只有**动作**（`.count()`、`.collect()`、`.write`）才会触发实际计算。

```python
# 这里什么都没发生 — 只是在构建 DAG
df = spark.read.csv("data.csv")
df = df.filter(F.col("region").isin(["UK", "US"]))

# 现在 Spark 才读取文件并应用过滤
count = df.count()  # ← 动作
```

**为什么？** Spark 可以在执行前优化整个管道。它可以重新排序过滤条件、将谓词下推到存储层、跳过不必要的工作。

### 2. 转换 vs 动作

| 转换（惰性） | 动作（触发执行） |
|---|---|
| `select()`, `filter()`, `withColumn()` | `count()`, `collect()`, `show()` |
| `groupBy()`, `join()`, `orderBy()` | `write`, `first()`, `take()` |
| `distinct()`, `dropDuplicates()` | `foreach()`, `reduce()` |

你可以串联数十个转换——Spark 先构建完整的 DAG，然后优化并执行。

### 3. 显式 Schema

```python
VIEWING_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),  # NOT NULL
        StructField("customer_id", StringType(), True),  # 可为空
        StructField("watch_minutes", DoubleType(), True),
        # ...
    ]
)
```

**为什么显式而非推断？**
- **快速失败**：如果 Schema 发生变化，在读取时就能捕获，而非 3 个阶段之后
- **文档化契约**：任何阅读代码的人都能确切知道存在哪些列
- **避免推断成本**：Spark 无需扫描文件两次来猜测类型

### 4. 死信队列

畸形行不会使管道崩溃——它们被捕获并写入 `data/rejected/`，并附有 `rejection_reason`：

```
data/rejected/ingest_date=2026-08-12/
├── INVALID_TIMESTAMP: 32 rows
├── COMPLETION_RATE_OUT_OF_RANGE: 11 rows
└── INVALID_REGION=XX: 3 rows
```

这是生产模式：永远不要静默丢弃坏数据。

## 阅读执行计划

```python
df.explain(extended=True)
```

输出显示：
- **解析的逻辑计划**：你要求的内容
- **分析的逻辑计划**：类型解析后
- **优化的逻辑计划**：Catalyst 优化器（谓词下推、常量折叠）之后
- **物理计划**：实际执行方式（FileScan、Filter、Project）

## 演练

```bash
uv run python -m etl_playground.m02_spark_foundation.exercise
```

练习内容：
1. 为所有 3 个数据集定义显式 Schema
2. 读取原始 CSV（惰性——尚未读取数据）
3. 打印逻辑计划以证明尚未执行
4. 验证：过滤畸形行，捕获被拒绝的记录
5. 通过 `.write.parquet()` 触发执行
6. 显示物理执行计划

在浏览器中打开 Spark UI（`http://localhost:4040`）查看 Job、Stage 和 Task。

## 常见陷阱

- **SparkSession 创建开销大**：创建一个约需 2 秒。在管道中复用它。
- **Schema 推断代价高**：Spark 会读取文件两次——一次用于推断，一次用于数据。
- **Spark UI 端口**：如果 4040 被占用，Spark 会选择 4041、4042 等。检查控制台输出。
- **Windows winutils.exe**：我们的 `shared/spark.py` 自动处理此问题。

## 面试关联

- "什么是惰性求值，Spark 为什么使用它？"
- "如何在读取 CSV 时强制使用 Schema？"
- "什么是死信队列，为什么需要它？"
- "`explain()` 能告诉你关于查询的什么信息？"

## 下一步

→ [M03: ETL 转换](../m03_etl_transform/concept_cn.md)
