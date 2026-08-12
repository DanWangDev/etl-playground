# M09: Streamlit Dashboard — "QuickSight" Equivalent

## What You'll Learn

- How to build analytical dashboards from Parquet lake data
- Separating query logic from visualization components
- Filter-driven interactive analytics
- KPI cards, trend charts, and data tables with Streamlit

## Overview

The dashboard proves that the pipeline delivers consumable analytical value. It queries the Parquet lake directly through DuckDB and renders 5 visuals with interactive filters.

```
┌──────────────────────────────────────────────────────────────┐
│                STREAMLIT DASHBOARD (M09)                      │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  SIDEBAR: Region (multi-select) + Date Range picker  │    │
│  └──────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐           │
│  │ Total   │ │ Watch   │ │ Avg     │ │ Unique  │           │
│  │ Views   │ │ Hours   │ │ Compl.  │ │ Content │           │
│  │ 2.4M    │ │ 180K h  │ │ 47%     │ │ 1,000   │           │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘           │
│                                                              │
│  ┌─────────────────────┐ ┌─────────────────────┐            │
│  │ Weekly Trend        │ │ Completion by       │            │
│  │ (bar chart)         │ │ Device (bar chart)  │            │
│  └─────────────────────┘ └─────────────────────┘            │
│                                                              │
│  ┌─────────────────────┐ ┌─────────────────────┐            │
│  │ Top 10 Titles       │ │ Launch Success      │            │
│  │ (data table)        │ │ by Region (table)   │            │
│  └─────────────────────┘ └─────────────────────┘            │
└──────────────────────────────────────────────────────────────┘
```

## Key Concepts

### 1. Modular Page Components

Each visual is a separate module in `pages/`:

```
pages/
├── kpi_cards.py       # 4 metric cards (views, hours, completion, content)
├── trends.py          # Bar chart: daily watch hours
├── completion.py      # Bar chart: avg completion by device type
├── top_titles.py      # Data table: top 10 by watch hours
└── launch_success.py  # Data table: on-time/delayed/cancelled by region
```

Each module exports a single `render_*()` function that takes the DuckDB connection, parquet path, date range, and region filter. The `app.py` orchestrator handles layout and user input.

### 2. Filter-Driven Queries

Every visual re-queries when the user changes a filter:

```python
# Region filter: ['UK', 'US'] → SQL: region IN ('UK', 'US')
region_filter = ", ".join(f"'{r}'" for r in selected_regions)

# Date filter: from Streamlit date_input widget
WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
```

### 3. DuckDB Integration

```python
conn = duckdb.connect(":memory:")
df = conn.execute(sql).df()  # Run SQL, get pandas DataFrame
st.altair_chart(chart)        # Render as interactive chart
```

DuckDB reads Parquet directly — no intermediate loading step. Queries on 500K rows complete in < 50ms.

## Walkthrough

```bash
# Launch the dashboard
make dashboard
# or:
uv run streamlit run src/etl_playground/m09_dashboard/app.py
```

Open `http://localhost:8501` in your browser.

## Gotchas

- **Data must exist**: Run M01 → M02 → M03 first to populate `data/curated/viewing_events_enriched/`.
- **Streamlit reruns on every interaction**: Keep queries fast (< 100ms). DuckDB on Parquet easily meets this.
- **Altair vs Plotly**: Altair is declarative (Vega-Lite). For complex interactivity, use Plotly.

## Interview Connection

- "How would you build a dashboard for business stakeholders?"
- "What's the difference between a dashboard and a report?"
- "Why Streamlit instead of Tableau or QuickSight?"
- "How do you handle filter state in a dashboard?"

## You've Completed the Pipeline!

You now have a working, end-to-end ETL pipeline:

```
M01 → M02 → M03 → M04 → M05 → M06 → M07 → M08 → M09
 Gen → Read → Trans → Load → Bench → Incr → Lake → WH → Dashboard
```

See [`docs/architecture.md`](../../../docs/architecture.md) for the full architecture and interview talking points.
