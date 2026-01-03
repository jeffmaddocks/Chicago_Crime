from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
from dash import dcc, html

from chicago_crime.analytics import queries
from chicago_crime.config import get_settings


def filter_panel() -> html.Div:
    settings = get_settings()
    min_date, max_date = queries.get_available_date_range()
    if max_date is None:
        default_end = datetime.now(timezone.utc)
    else:
        default_end = max_date
    default_start = default_end - timedelta(days=30)
    if min_date and default_start < min_date:
        default_start = min_date

    primary_options = [
        {"label": pt, "value": pt} for pt in queries.distinct_primary_types()
    ]
    district_options = [
        {"label": str(dist), "value": dist} for dist in queries.distinct_districts()
    ]

    return html.Div(
        [
            html.H3("Filters"),
            html.Label("Date range"),
            dcc.DatePickerRange(
                id="date-range",
                start_date=default_start.date() if default_start else None,
                end_date=default_end.date() if default_end else None,
                min_date_allowed=min_date.date() if min_date else None,
                max_date_allowed=max_date.date() if max_date else None,
                updatemode="singledate",
            ),
            html.Label("Primary type"),
            dcc.Dropdown(
                id="primary-type",
                options=primary_options,
                multi=True,
                placeholder="All types",
            ),
            html.Label("District"),
            dcc.Dropdown(
                id="district",
                options=district_options,
                clearable=True,
                placeholder="All districts",
            ),
            html.Label("Flags"),
            dcc.Checklist(
                id="flags",
                options=[
                    {"label": "Arrest", "value": "arrest"},
                    {"label": "Domestic", "value": "domestic"},
                ],
            ),
            html.Label("Map mode"),
            dcc.RadioItems(
                id="map-mode",
                options=[
                    {"label": "Auto", "value": "auto"},
                    {"label": "Points", "value": "points"},
                    {"label": "Choropleth", "value": "choropleth"},
                ],
                value=settings.map_mode_default,
            ),
            html.Label("Choropleth metric"),
            dcc.Dropdown(
                id="choropleth-metric",
                options=[
                    {"label": "Crime Count", "value": "count"},
                    {"label": "Arrest Rate", "value": "arrest_rate"},
                ],
                value=settings.choropleth_metric_default,
                clearable=False,
            ),
            html.Button("Download CSV of filtered data", id="download-btn"),
            dcc.Download(id="download-data"),
            html.Div(id="data-freshness", style={"marginTop": "1rem"}),
        ],
        style={
            "padding": "1rem",
            "background": "#f5f7fb",
            "borderRight": "1px solid #e0e0e0",
            "minWidth": "166px",
            "width": "166px",
        },
    )
