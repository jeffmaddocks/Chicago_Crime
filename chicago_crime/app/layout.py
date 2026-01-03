from dash import dcc, html

from chicago_crime.app.components import filter_panel


def create_layout() -> html.Div:
    return html.Div(
        [
            dcc.Interval(id="refresh-interval", interval=60 * 1000, n_intervals=0),
            html.Div(
                [
                    filter_panel(),
                    html.Div(
                        [
                            html.H1("Chicago Crime Analytics"),
                            html.Div(
                                [
                                    dcc.Graph(
                                        id="map",
                                        style={"gridColumn": "1 / -1", "height": "900px"},
                                    ),
                                    dcc.Graph(id="time-series"),
                                    dcc.Graph(id="top-types"),
                                    dcc.Graph(id="top-community"),
                                    dcc.Graph(id="heatmap"),
                                    dcc.Graph(id="arrest-rate"),
                                ],
                                style={
                                    "display": "grid",
                                    "gridTemplateColumns": "repeat(auto-fit, minmax(320px, 1fr))",
                                    "gap": "1rem",
                                },
                            ),
                            html.Div(id="map-warning", style={"marginTop": "0.5rem"}),
                        ],
                        style={"padding": "1.5rem", "flex": "1"},
                    ),
                ],
                style={"display": "flex", "minHeight": "100vh"},
            ),
        ]
    )
