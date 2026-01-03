from __future__ import annotations

from dash import Dash

from chicago_crime.app import callbacks  # noqa: F401
from chicago_crime.app.layout import create_layout
from chicago_crime.config import get_settings
from chicago_crime.logging_config import setup_logging


def create_app() -> Dash:
    settings = get_settings()
    setup_logging(settings.log_level)
    app = Dash(__name__)
    app.layout = create_layout()
    return app


def main() -> None:
    settings = get_settings()
    app = create_app()
    app.run(host=settings.dash_host, port=settings.dash_port, debug=False)


if __name__ == "__main__":
    main()
