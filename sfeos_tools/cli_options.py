"""Reusable CLI option decorators for consistent connection handling."""

import click


def auth_options(f):
    """Add standardized authentication options to a CLI command.

    Adds the following options:
    - --use-ssl/--no-ssl: SSL connection flag
    - --user: Username
    - --password: Password
    """
    f = click.option(
        "--use-ssl/--no-ssl",
        default=None,
        help="Use SSL connection (default: true or ES_USE_SSL env var)",
    )(f)
    f = click.option(
        "--user",
        type=str,
        default=None,
        help="Username (default: ES_USER env var)",
    )(f)
    f = click.option(
        "--password",
        type=str,
        default=None,
        help="Password (default: ES_PASS env var)",
    )(f)
    return f


def database_connection_options(f):
    """Add database-specific connection options to a CLI command.

    Adds the following options:
    - --host: Database host
    - --port: Database port
    """
    f = click.option(
        "--host",
        type=str,
        default=None,
        help="Database host (default: localhost or ES_HOST env var)",
    )(f)
    f = click.option(
        "--port",
        type=int,
        default=None,
        help="Database port (default: 9200 for ES, 9202 for OS, or ES_PORT env var)",
    )(f)
    return f


def database_options(f):
    """Add standardized database connection options to a CLI command.

    Combines database_connection_options and auth_options.
    Adds the following options:
    - --host: Database host
    - --port: Database port
    - --use-ssl/--no-ssl: SSL connection flag
    - --user: Database username
    - --password: Database password
    """
    f = database_connection_options(f)
    f = auth_options(f)
    return f


def stac_api_options(f):
    """Add standardized STAC API connection options to a CLI command.

    Adds the following option:
    - --stac-url: STAC API base URL
    """
    f = click.option(
        "--stac-url",
        default="http://localhost:8080",
        help="STAC API base URL (default: http://localhost:8080)",
    )(f)
    return f
