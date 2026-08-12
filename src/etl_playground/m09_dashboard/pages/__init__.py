"""Dashboard page components for the Streamlit app.

Each module renders one visual:
- kpi_cards: Metric cards (views, watch hours, completion, unique content)
- trends: Weekly engagement trend bar chart
- completion: Completion rate by device type bar chart
- top_titles: Top 10 titles by watch hours table
- launch_success: Launch success rate by region table
"""

from etl_playground.m09_dashboard.pages.completion import render_completion_by_device
from etl_playground.m09_dashboard.pages.kpi_cards import render_kpi_cards
from etl_playground.m09_dashboard.pages.launch_success import render_launch_success
from etl_playground.m09_dashboard.pages.top_titles import render_top_titles
from etl_playground.m09_dashboard.pages.trends import render_trend_chart

__all__ = [
    "render_kpi_cards",
    "render_trend_chart",
    "render_completion_by_device",
    "render_top_titles",
    "render_launch_success",
]
