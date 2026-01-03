from __future__ import annotations

from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any
import plotly.express as px
from dash import Input, Output, State, callback, dcc

from chicago_crime.analytics import aggregations, geo, queries
from chicago_crime.config import get_settings
from chicago_crime.ingest.state import load_state


class LRUCache:
    def __init__(self, max_size: int = 32) -> None:
        self.max_size = max_size
        self._data: OrderedDict[tuple, Any] = OrderedDict()

    def get(self, key: tuple) -> Any | None:
        if key not in self._data:
            return None
        self._data.move_to_end(key)
        return self._data[key]

    def set(self, key: tuple, value: Any) -> None:
        self._data[key] = value
        self._data.move_to_end(key)
        if len(self._data) > self.max_size:
            self._data.popitem(last=False)


_cache = LRUCache(max_size=64)
_geojson_cache: dict[str, Any] | None = None


def _parse_dates(start_date: str | None, end_date: str | None) -> tuple[datetime | None, datetime | None]:
    start = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc) if start_date else None
    if end_date:
        end = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
        end = end.replace(hour=23, minute=59, second=59)
    else:
        end = None
    return start, end


def _filters_key(start_date, end_date, primary_types, district, flags, map_mode, metric) -> tuple:
    return (
        start_date,
        end_date,
        tuple(primary_types) if primary_types else None,
        district,
        tuple(flags) if flags else None,
        map_mode,
        metric,
    )


def _get_filter_values(start_date, end_date, primary_types, district, flags):
    date_start, date_end = _parse_dates(start_date, end_date)
    arrest = True if flags and "arrest" in flags else None
    domestic = True if flags and "domestic" in flags else None
    return date_start, date_end, primary_types, district, arrest, domestic


def _empty_figure(title: str):
    fig = px.scatter()
    fig.update_layout(title=title, annotations=[{"text": "No data", "showarrow": False}])
    return fig


def _load_geojson() -> dict[str, Any]:
    global _geojson_cache
    if _geojson_cache is None:
        settings = get_settings()
        geojson = geo.load_community_areas_geojson()
        if geojson:
            geojson = geo.ensure_feature_id_key(geojson, settings.community_area_number_field)
        _geojson_cache = geojson or {}
    return _geojson_cache


@callback(
    Output("time-series", "figure"),
    Output("top-types", "figure"),
    Output("top-community", "figure"),
    Output("heatmap", "figure"),
    Output("map", "figure"),
    Output("arrest-rate", "figure"),
    Output("map-warning", "children"),
    Input("date-range", "start_date"),
    Input("date-range", "end_date"),
    Input("primary-type", "value"),
    Input("district", "value"),
    Input("flags", "value"),
    Input("map-mode", "value"),
    Input("choropleth-metric", "value"),
)

def update_charts(start_date, end_date, primary_types, district, flags, map_mode, metric):
    date_start, date_end, primary_types, district, arrest, domestic = _get_filter_values(
        start_date, end_date, primary_types, district, flags
    )
    settings = get_settings()
    map_mode = map_mode or settings.map_mode_default
    metric = metric or settings.choropleth_metric_default
    cache_key = _filters_key(start_date, end_date, primary_types, district, flags, map_mode, metric)

    cached = _cache.get(cache_key)
    if cached:
        return cached

    ts = queries.time_series_counts(date_start, date_end, primary_types, district, arrest, domestic)
    if ts.empty:
        time_series_fig = _empty_figure("Time Series")
    else:
        time_series_fig = px.line(ts, x="bucket", y="count", title="Incidents Over Time")

    top_types = queries.top_n_primary_types(date_start, date_end, primary_types, district, arrest, domestic)
    if top_types.empty:
        top_fig = _empty_figure("Top Primary Types")
    else:
        top_fig = px.bar(top_types, x="primary_type", y="count", title="Top Primary Types")

    heatmap_df = queries.dow_hour_heatmap(date_start, date_end, primary_types, district, arrest, domestic)
    if heatmap_df.empty:
        heatmap_fig = _empty_figure("Day/Hour Heatmap")
    else:
        heatmap_fig = px.density_heatmap(
            heatmap_df,
            x="hour",
            y="dow",
            z="count",
            color_continuous_scale="Blues",
            title="Day of Week vs Hour",
        )
        heatmap_fig.update_yaxes(ticktext=["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"], tickvals=["0", "1", "2", "3", "4", "5", "6"])

    warning = ""
    map_df = queries.filter_crimes(date_start, date_end, primary_types, district, arrest, domestic)
    if map_df.empty:
        map_fig = _empty_figure("Map")
    else:
        use_points = map_mode == "points"
        use_choropleth = map_mode == "choropleth"
        if map_mode == "auto":
            max_days = settings.map_max_days_points
            within_days = date_start and date_end and (date_end - date_start).days <= max_days
            within_points = len(map_df) <= settings.max_map_points
            use_points = bool(within_days and within_points)
            use_choropleth = not use_points
            if use_choropleth:
                warning = "Auto-switched to choropleth for performance."

        if use_points:
            map_df = map_df.dropna(subset=["latitude", "longitude"])
            if len(map_df) > settings.max_map_points:
                warning = f"Map points exceed {settings.max_map_points}; downsampled for performance."
                map_df = aggregations.downsample(map_df, settings.max_map_points)
            if map_df.empty:
                map_fig = _empty_figure("Map")
            else:
                map_fig = px.scatter_mapbox(
                    map_df,
                    lat="latitude",
                    lon="longitude",
                    hover_data=["primary_type", "description", "date", "community_area_name"],
                    zoom=9,
                    title="Incident Map (Points)",
                    labels={"community_area_name": "Community"},
                )
                map_fig.update_layout(mapbox_style=geo.MAPBOX_STYLE, margin={"l": 0, "r": 0, "t": 40, "b": 0})
        else:
            geojson = _load_geojson()
            if not geojson:
                warning = "Community area boundaries not available yet — run ingest."
                map_fig = _empty_figure("Choropleth Map")
            else:
                if metric == "arrest_rate":
                    ca_df = queries.community_area_arrest_rate(
                        date_start, date_end, primary_types, district, arrest, domestic
                    )
                    color_col = "arrest_rate"
                    title = "Arrest Rate by Community Area"
                else:
                    ca_df = queries.community_area_counts(
                        date_start, date_end, primary_types, district, arrest, domestic
                    )
                    color_col = "crime_count"
                    title = "Crimes by Community Area"
                if ca_df.empty:
                    map_fig = _empty_figure("Choropleth Map")
                else:
                    hover_data = {}
                    if "crime_count" in ca_df.columns:
                        hover_data["crime_count"] = True
                    if "arrest_rate" in ca_df.columns:
                        hover_data["arrest_rate"] = ":.1%"
                    map_fig = px.choropleth_mapbox(
                        ca_df,
                        geojson=geojson,
                        locations="community_area",
                        featureidkey="id",
                        color=color_col,
                        hover_name="community_area_name",
                        hover_data=hover_data,
                        center={"lat": 41.8781, "lon": -87.6298},
                        zoom=9,
                        title=title,
                        mapbox_style=geo.MAPBOX_STYLE,
                        labels={"community_area_name": "Community", "crime_count": "Crimes", "arrest_rate": "Arrest Rate"},
                    )

    arrest_rate = queries.arrest_rate_by_type(date_start, date_end, primary_types, district, arrest, domestic)
    if arrest_rate.empty:
        arrest_fig = _empty_figure("Arrest Rate")
    else:
        arrest_rate["arrest_rate"] = arrest_rate["arrest_rate"] * 100
        arrest_fig = px.bar(
            arrest_rate,
            x="primary_type",
            y="arrest_rate",
            title="Arrest Rate by Primary Type (%)",
        )

    top_community = queries.community_area_counts(date_start, date_end, primary_types, district, arrest, domestic)
    if top_community.empty:
        top_community_fig = _empty_figure("Top Community Areas")
    else:
        top_community_fig = px.bar(
            top_community.head(15),
            x="community_area_name",
            y="crime_count",
            title="Top Community Areas",
            labels={"community_area_name": "Community", "crime_count": "Crimes"},
        )

    payload = (time_series_fig, top_fig, top_community_fig, heatmap_fig, map_fig, arrest_fig, warning)
    _cache.set(cache_key, payload)
    return payload


@callback(
    Output("download-data", "data"),
    Input("download-btn", "n_clicks"),
    State("date-range", "start_date"),
    State("date-range", "end_date"),
    State("primary-type", "value"),
    State("district", "value"),
    State("flags", "value"),
    prevent_initial_call=True,
)

def download_data(n_clicks, start_date, end_date, primary_types, district, flags):
    date_start, date_end, primary_types, district, arrest, domestic = _get_filter_values(
        start_date, end_date, primary_types, district, flags
    )
    df = queries.filter_crimes(date_start, date_end, primary_types, district, arrest, domestic)
    return dcc.send_data_frame(df.to_csv, "filtered_crimes.csv", index=False)


@callback(
    Output("data-freshness", "children"),
    Input("refresh-interval", "n_intervals"),
)

def update_freshness(_):
    settings = get_settings()
    if not settings.state_path.exists():
        return "Data freshness: no ingest state found."
    state = load_state(settings.state_path)
    watermark = state.watermark_max_date.isoformat() if state.watermark_max_date else "unknown"
    last_run = state.last_run_at.isoformat() if state.last_run_at else "unknown"
    return f"Data freshness: last run {last_run}, watermark {watermark}."
