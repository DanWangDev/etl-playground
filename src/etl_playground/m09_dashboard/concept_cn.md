# M09: Streamlit 仪表板 — "QuickSight" 等价

## 学习目标

- 如何从 Parquet 湖数据构建分析仪表板
- 分离查询逻辑和可视化组件
- 过滤器驱动的交互式分析
- 使用 Streamlit 构建 KPI 卡片、趋势图和表格

## 概述

仪表板证明管道提供了可消费的分析价值。它通过 DuckDB 直接查询 Parquet 湖，并渲染 5 个带有交互式过滤器的可视化组件。

## 核心概念

### 1. 模块化页面组件

每个可视化是 `pages/` 中的独立模块。每个模块导出单个 `render_*()` 函数，接收 DuckDB 连接、parquet 路径、日期范围和地区过滤器。`app.py` 编排器处理布局和用户输入。

### 2. 过滤器驱动查询

```
Region 过滤器: ['UK', 'US'] → SQL: region IN ('UK', 'US')
Date 过滤器: WHERE event_date BETWEEN '...' AND '...'
```

每个可视化在用户更改过滤器时重新查询。

### 3. DuckDB 集成

DuckDB 直接读取 Parquet——无中间加载步骤。50 万行数据查询在 < 50ms 内完成。

## 演练

```bash
make dashboard
```

在浏览器中打开 `http://localhost:8501`。

## 面试关联

- "如何为业务利益相关者构建仪表板？"
- "仪表板和报告有什么区别？"
- "为什么选择 Streamlit 而非 Tableau 或 QuickSight？"
- "如何在仪表板中处理过滤器状态？"

## 管道完成！

你现在拥有一个完整的端到端 ETL 管道：

```
M01 → M02 → M03 → M04 → M05 → M06 → M07 → M08 → M09
 生成 → 读取 → 转换 → 加载 → 基准 → 增量 → 湖 → 仓库 → 仪表板
```

详见 [`docs/architecture.md`](../../../docs/architecture.md) 获取完整架构和面试要点。
