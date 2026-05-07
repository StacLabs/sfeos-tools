"""Reusable CLI option decorators for consistent connection handling."""

import os
import sys
from typing import Optional, Tuple

import click


def auth_options(f):
    """Add standardized authentication options to a CLI command.

    Adds the following options:
    - --use-ssl/--no-ssl: SSL connection flag
    - --user: Username
    - --password: Password
    - --api-key: API key for authentication

    Environment variables:
    - ES_USE_SSL: Default for --use-ssl/--no-ssl
    - ES_USER: Default for --user
    - ES_PASS: Default for --password
    - ES_API_KEY: Default for --api-key
    """
    f = click.option(
        "--use-ssl/--no-ssl",
        default=None,
        envvar="ES_USE_SSL",
        help="Use SSL connection (default: ES_USE_SSL env var)",
    )(f)
    f = click.option(
        "--user",
        type=str,
        default=None,
        envvar="ES_USER",
        help="Username (default: ES_USER env var)",
    )(f)
    f = click.option(
        "--password",
        type=str,
        default=None,
        envvar="ES_PASS",
        help="Password (default: ES_PASS env var)",
    )(f)
    f = click.option(
        "--api-key",
        type=str,
        default=None,
        envvar="ES_API_KEY",
        help="API key for authentication (default: ES_API_KEY env var)",
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
    - --api-key: API key for authentication
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


def lang_options(f):
    """Add language option to CLI command.

    Adds the following option:
    - --lang: Language of raw data (default: None)
    """
    f = click.option(
        "--lang",
        type=str,
        default=None,
        help="Language of raw data (default: None)",
    )(f)
    return f


def set_es_env_vars(
    host=None, port=None, use_ssl=None, user=None, password=None, api_key=None
):
    """Set Elasticsearch/OpenSearch environment variables from CLI options.

    This function centralizes the logic for setting environment variables
    from CLI parameters, allowing them to be used by the underlying
    Elasticsearch/OpenSearch client configurations.

    Args:
        host: Database host
        port: Database port
        use_ssl: SSL connection flag (True/False/None)
        user: Username for authentication
        password: Password for authentication
        api_key: API key for authentication
    """
    if host:
        os.environ["ES_HOST"] = host
    if port:
        os.environ["ES_PORT"] = str(port)
    if use_ssl is not None:
        os.environ["ES_USE_SSL"] = "true" if use_ssl else "false"
    if user:
        os.environ["ES_USER"] = user
    if password:
        os.environ["ES_PASS"] = password
    if api_key:
        os.environ["ES_API_KEY"] = api_key


def validate_auth_options(
    user: Optional[str], password: Optional[str], api_key: Optional[str]
) -> None:
    """Validate authentication parameters for mutual exclusivity and completeness.

    Ensures that:
    - Either API key OR username/password is provided, not both
    - If username is provided, password must also be provided (and vice versa)

    Args:
        user: Username for basic authentication (or None)
        password: Password for basic authentication (or None)
        api_key: API key for authentication (or None)

    Raises:
        SystemExit: If validation fails, prints error and exits with code 1
    """
    if api_key and (user or password):
        click.echo(
            click.style(
                "✗ Authentication error: Please provide EITHER user/password OR an api_key, not both.",
                fg="red",
            )
        )
        sys.exit(1)

    if (user and not password) or (password and not user):
        click.echo(
            click.style(
                "✗ Authentication error: Both user AND password must be provided together.",
                fg="red",
            )
        )
        sys.exit(1)


def prepare_auth_headers_and_verify(
    user: Optional[str],
    password: Optional[str],
    api_key: Optional[str],
    use_ssl: Optional[bool],
) -> Tuple[dict, Optional[Tuple[str, str]], bool]:
    """Prepare authentication headers, auth tuple, and SSL verification settings.

    Args:
        user: Username for basic authentication (or None)
        password: Password for basic authentication (or None)
        api_key: API key for authentication (or None)
        use_ssl: SSL verification flag (True/False/None)

    Returns:
        Tuple of (headers dict, auth tuple or None, verify bool)
    """
    headers = {}
    auth = None

    if api_key:
        headers["Authorization"] = f"ApiKey {api_key}"
    elif user and password:
        auth = (user, password)

    verify = True
    if use_ssl is False:
        verify = False

    return headers, auth, verify


def configure_session_auth(
    session,
    user: Optional[str],
    password: Optional[str],
    api_key: Optional[str],
    use_ssl: Optional[bool],
) -> None:
    """Configure a requests.Session with authentication and SSL settings.

    Args:
        session: requests.Session object to configure
        user: Username for basic authentication (or None)
        password: Password for basic authentication (or None)
        api_key: API key for authentication (or None)
        use_ssl: SSL verification flag (True/False/None)
    """
    if api_key:
        session.headers.update({"Authorization": f"ApiKey {api_key}"})
    elif user and password:
        session.auth = (user, password)

    if use_ssl is False:
        session.verify = False
