# ETL Playground — 内容分析数据管道

[English](README.md)

> 动手学习数据工程——从头构建一个可信的、面试就绪的 ETL 管道。
> PySpark、DuckDB、Streamlit。零云成本，完全离线运行。

## 工作原理

```
合成数据生成器 (M01)
        |
        v
  本地文件系统: data/raw/
  CSV/JSON, 不可变源数据 (S3 等价)
        |
        v
  PySpark ETL (M02-M06)
  schema → validate → dedupe → join → aggregate (Glue 等价)
        |
        v
  本地文件系统: data/curated/
  分区 Parquet 数据集 (S3 等价)
        |
     +--+------------------+
     |                     |
     v                     v
DuckDB 湖查询           DuckDB 星型模型 (M08)
(Athena 等价)           dim_content, dim_region,
     |                   dim_device, dim_date,
     |                   fact_viewing
     |                     |
     +----------+----------+
                v
       Streamlit 仪表板 (M09)
       (QuickSight 等价)
```

## 快速开始

**前置条件:** Python 3.11+, JDK 11-21, [uv](https://docs.astral.sh/uv/), uv sync --extra dev

```bash
git clone https://github.com/DanWangDev/etl-playground.git
cd etl-playground
cp .env.example .env
uv sync
uv run python -m etl_playground.m01_data_generator.exercise --scale 10000
```

## 学习路径 (9 个模块)

| # | 模块 | 构建内容 | 核心概念 |
|---|------|---------|---------|
| 01 | **数据生成器** | 3 张合成表 (50万–2000万行) | 事件建模、分布、数据质量注入 |
| 02 | **Spark 基础** | 读取、验证、写入 Parquet | 惰性求值、转换 vs 动作、Schema |
| 03 | **ETL 转换** | 去重、关联、派生、聚合 | Shuffle、广播连接、空值处理 |
| 04 | **精炼输出** | 分区 Parquet | 分区策略、合并、小文件问题 |
| 05 | **性能基准** | CSV vs Parquet 对比 | 列裁剪、压缩、分区裁剪 |
| 06 | **增量 ETL** | 幂等运行、迟到数据、回填 | 幂等性、水位线、回填策略 |
| 07 | **湖查询** | DuckDB 分析查询 | 谓词下推、扫描度量 |
| 08 | **数据仓库** | 星型维度模型 | 事实粒度、维度表、参照完整性 |
| 09 | **仪表板** | Streamlit 报表应用 | KPI、趋势图、筛选器 |

## 运行练习

```bash
uv run python -m etl_playground.m01_data_generator.exercise --scale 500000
uv run python -m etl_playground.m02_spark_foundation.exercise
make dashboard    # 启动 Streamlit 仪表板
make test         # 运行所有测试
```

## 技术栈

![Python 3.11+](https://img.shields.io/badge/Python_3.11+-3776AB?style=flat-square&logo=python&logoColor=white)
![PySpark 3.5](https://img.shields.io/badge/PySpark_3.5-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=flat-square&logo=duckdb&logoColor=black)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)

## 架构：AWS → 本地映射

本项目教授直接映射到 AWS 的概念，无需成本：

| AWS 组件 | 本地替代 | 相同概念 |
|----------|---------|---------|
| Amazon S3 | 本地文件系统 (`data/raw/`, `data/curated/`) | 不可变原始区，分区精炼区 |
| AWS Glue / PySpark | `pyspark` 本地模式 | 相同 DataFrame API，分区，Shuffle，DAG |
| Glue Data Catalog | YAML Schema + DuckDB | Schema 注册，表元数据 |
| Amazon Athena | DuckDB 湖查询 | Parquet 谓词下推，扫描度量 |
| Redshift Serverless | DuckDB 仓库 | 星型模型，事实粒度，维度建模 |
| Amazon QuickSight | Streamlit | KPI、图表、筛选器 |
| Amazon Kinesis (二期) | Redpanda | 流式摄入，Kafka API |

**成本: ￥0.** 一切在本地运行，无需 AWS 账户。

## 关键设计决策

| 决策 | 原因 |
|------|------|
| **DuckDB 双角色** (Athena + Redshift) | 原生 Parquet 读取+谓词下推（湖查询）和完整 DDL/DML（仓库） |
| **PySpark，非 Polars/Pandas** | 暴露分区、Shuffle、广播连接、DAG、惰性求值——核心数仓概念 |
| **event_date + region 分区** | 查询对齐的低基数键。避免 customer_id 分区的小文件问题 |
| **核心模块无需 Docker** | PySpark、DuckDB、Streamlit 原生运行 |
| **50万→2000万规模切换** | `.env` 控制数据量。小规模快速迭代，大规模基准可信 |

## 面试准备

完成本游乐场后，你可以基于实践经验回答以下问题：

- 为什么用 Spark 而不是单进程 Python 脚本？
- 管道中什么导致了 Shuffle，如何观察到的？
- 为什么用 Parquet，为什么选择这些分区键？
- 如何使增量 ETL 幂等？
- 为什么同时保留数据湖和数据仓库？

详见 `docs/architecture.md`。

## 许可证

MIT。数据集为明确的合成数据——不包含真实客户或个人数据。
