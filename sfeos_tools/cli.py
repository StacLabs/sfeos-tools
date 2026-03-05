"""SFEOS CLI Tools - Utilities for managing stac-fastapi-elasticsearch-opensearch deployments.

This tool provides various utilities for managing and maintaining SFEOS deployments,
including database migrations, maintenance tasks, and more.

Usage:
    sfeos-tools add-bbox-shape --backend elasticsearch
    sfeos-tools add-bbox-shape --backend opensearch
"""

import asyncio
import logging
import sys

import click

try:
    from importlib.metadata import version as _get_version
except ImportError:
    from importlib_metadata import version as _get_version  # type: ignore[no-redef]

__version__ = _get_version("sfeos-tools")

from .bbox_shape import run_add_bbox_shape
from .catalog_ingestion import ingest_from_xml
from .cli_options import auth_options, database_options, stac_api_options
from .data_loader import load_items
from .reindex import run as unified_reindex_run

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.group()
@click.version_option(version=__version__, prog_name="sfeos-tools")
def cli():
    """SFEOS Tools - Utilities for managing stac-fastapi-elasticsearch-opensearch deployments."""
    pass


@cli.command("add-bbox-shape")
@click.option(
    "--backend",
    type=click.Choice(["elasticsearch", "opensearch"], case_sensitive=False),
    required=True,
    help="Database backend to use",
)
@database_options
def add_bbox_shape(backend, host, port, use_ssl, user, password):
    """Add bbox_shape field to existing collections for spatial search support.

    This migration is required for collections created before spatial search
    was added. Collections created or updated after this feature will
    automatically have the bbox_shape field.

    Examples:
        sfeos_tools.py add-bbox-shape --backend elasticsearch
        sfeos_tools.py add-bbox-shape --backend opensearch --host db.example.com --port 9200
        sfeos_tools.py add-bbox-shape --backend elasticsearch --no-ssl --host localhost
    """
    import os

    # Set environment variables from CLI options if provided
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

    try:
        asyncio.run(run_add_bbox_shape(backend.lower()))
        click.echo(click.style("✓ Migration completed successfully", fg="green"))
    except KeyboardInterrupt:
        click.echo(click.style("\n✗ Migration interrupted by user", fg="yellow"))
        sys.exit(1)
    except Exception as e:
        error_msg = str(e)
        click.echo(click.style(f"✗ Migration failed: {error_msg}", fg="red"))

        # Provide helpful hints for common errors
        if "TLS" in error_msg or "SSL" in error_msg:
            click.echo(
                click.style(
                    "\n💡 Hint: If you're connecting to a local Docker Compose instance, "
                    "try adding --no-ssl flag",
                    fg="yellow",
                )
            )
        elif "Connection refused" in error_msg:
            click.echo(
                click.style(
                    "\n💡 Hint: Make sure your database is running and accessible at the specified host:port",
                    fg="yellow",
                )
            )
        sys.exit(1)


@cli.command("reindex")
@click.option(
    "--backend",
    type=click.Choice(["elasticsearch", "opensearch"], case_sensitive=False),
    required=True,
    help="Database backend to use",
)
@database_options
@click.option(
    "--yes",
    is_flag=True,
    help="Skip confirmation prompt",
)
def reindex(backend, host, port, use_ssl, user, password, yes):
    """Reindex all STAC indexes to the next version and update aliases.

    For Elasticsearch, this runs a migration that:
    - Creates/updates index templates
    - Reindexes collections and item indexes to a new version
    - Applies asset migration script for compatibility
    - Switches aliases to the new indexes
    """
    import os

    backend = backend.lower()

    if not yes:
        proceed = click.confirm(
            "This will reindex all collections and item indexes and update aliases. Proceed?",
            default=False,
        )
        if not proceed:
            click.echo(click.style("Aborted", fg="yellow"))
            return

    # Set environment variables from CLI options if provided
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

    try:
        asyncio.run(unified_reindex_run(backend))
        click.echo(
            click.style(
                f"✓ Reindex ({backend.title()}) completed successfully", fg="green"
            )
        )
    except KeyboardInterrupt:
        click.echo(click.style("\n✗ Reindex interrupted by user", fg="yellow"))
        sys.exit(1)
    except Exception as e:
        error_msg = str(e)
        click.echo(click.style(f"✗ Reindex failed: {error_msg}", fg="red"))
        # Provide helpful hints for common errors
        if "TLS" in error_msg or "SSL" in error_msg:
            click.echo(
                click.style(
                    "\n💡 Hint: If you're connecting to a local Docker Compose instance, try adding --no-ssl flag",
                    fg="yellow",
                )
            )
        elif "Connection refused" in error_msg:
            click.echo(
                click.style(
                    "\n💡 Hint: Make sure your database is running and accessible at the specified host:port",
                    fg="yellow",
                )
            )
        sys.exit(1)


@cli.command("load-data")
@stac_api_options
@click.option(
    "--collection-id",
    default="test-collection",
    help="ID of the collection to which items are added",
)
@click.option("--use-bulk", is_flag=True, help="Use bulk insert method for items")
@click.option(
    "--data-dir",
    type=click.Path(exists=True),
    default="sample_data/",
    help="Directory containing collection.json and feature collection file",
)
def load_data(stac_url: str, collection_id: str, use_bulk: bool, data_dir: str) -> None:
    """Load STAC items into the database via STAC API.

    This command loads a STAC collection and its items from local JSON files
    into a STAC API instance. It expects a directory containing:
    - collection.json: The STAC collection definition
    - One or more feature collection JSON files with STAC items

    Examples:
        sfeos-tools load-data --stac-url http://localhost:8080
        sfeos-tools load-data --stac-url http://localhost:8080 --collection-id my-collection --use-bulk
        sfeos-tools load-data --stac-url http://localhost:8080 --data-dir /path/to/data
    """
    from httpx import Client

    try:
        with Client(base_url=stac_url) as client:
            load_items(client, collection_id, use_bulk, data_dir)
        click.echo(click.style("✓ Data loading completed successfully", fg="green"))
    except KeyboardInterrupt:
        click.echo(click.style("\n✗ Data loading interrupted by user", fg="yellow"))
        sys.exit(1)
    except Exception as e:
        error_msg = str(e)
        click.echo(click.style(f"✗ Data loading failed: {error_msg}", fg="red"))
        sys.exit(1)


@cli.command("ingest-catalog")
@click.option(
    "--xml-file",
    type=click.Path(exists=True),
    required=True,
    help="Path to RDF/XML file containing SKOS concepts",
)
@stac_api_options
@auth_options
def ingest_catalog(
    xml_file: str, stac_url: str, use_ssl: bool, user: str, password: str
) -> None:
    """Ingest SKOS/RDF-XML file to create STAC catalogs and sub-catalogs.

    This command parses an RDF/XML file containing SKOS concepts and creates
    a hierarchical catalog structure in the STAC API. It handles:
    - Creating catalogs from SKOS concepts
    - Establishing parent-child relationships (skos:narrower)
    - Preserving semantic links (skos:related, skos:exactMatch, etc.)
    - Including metadata from definitions and modification dates

    Examples:
        sfeos-tools ingest-catalog --xml-file thesaurus.rdf
        sfeos-tools ingest-catalog --xml-file thesaurus.rdf --stac-url http://localhost:8080
        sfeos-tools ingest-catalog --xml-file /path/to/concepts.xml --stac-url https://my-stac-api.com
    """
    try:
        ingest_from_xml(
            xml_file, stac_url, user=user, password=password, use_ssl=use_ssl
        )
        click.echo(
            click.style("✓ Catalog ingestion completed successfully", fg="green")
        )
    except KeyboardInterrupt:
        click.echo(click.style("\n✗ Ingestion interrupted by user", fg="yellow"))
        sys.exit(1)
    except FileNotFoundError as e:
        click.echo(click.style(f"✗ File not found: {e}", fg="red"))
        sys.exit(1)
    except Exception as e:
        error_msg = str(e)
        click.echo(click.style(f"✗ Ingestion failed: {error_msg}", fg="red"))

        # Provide helpful hints for common errors
        if "Connection refused" in error_msg:
            click.echo(
                click.style(
                    "\n💡 Hint: Make sure your STAC API is running and accessible at the specified URL",
                    fg="yellow",
                )
            )
        elif "parse" in error_msg.lower():
            click.echo(
                click.style(
                    "\n💡 Hint: Verify that the XML file is valid RDF/XML format",
                    fg="yellow",
                )
            )
        sys.exit(1)


@cli.command("viewer")
@stac_api_options
@click.option(
    "--port",
    type=int,
    default=8501,
    help="Port for the Streamlit viewer (default: 8501)",
)
def viewer(stac_url: str, port: int) -> None:
    """Launch interactive Streamlit viewer for exploring STAC collections and items.

    This command starts a local web-based viewer that allows you to:
    - Browse STAC collections
    - View items on an interactive map
    - Search and filter items
    - Inspect item metadata

    Examples:
        sfeos-tools viewer
        sfeos-tools viewer --stac-url http://localhost:8080
        sfeos-tools viewer --stac-url https://my-stac-api.com --port 8502
    """
    try:
        import sys
        from pathlib import Path

        import streamlit.web.cli as stcli

        # Get the path to viewer.py
        viewer_path = Path(__file__).parent / "viewer.py"

        # Set environment variable for the STAC URL
        import os

        os.environ["SFEOS_STAC_URL"] = stac_url

        click.echo(click.style("🚀 Starting SFEOS Viewer...", fg="green"))
        click.echo(click.style(f"📡 STAC API: {stac_url}", fg="cyan"))
        click.echo(
            click.style(f"🌐 Viewer will open at: http://localhost:{port}", fg="cyan")
        )
        click.echo(click.style("\n💡 Press Ctrl+C to stop the viewer\n", fg="yellow"))

        # Run streamlit
        sys.argv = [
            "streamlit",
            "run",
            str(viewer_path),
            "--server.port",
            str(port),
            "--server.headless",
            "true",
            "--browser.gatherUsageStats",
            "false",
        ]
        sys.exit(stcli.main())

    except ImportError:
        click.echo(
            click.style(
                "✗ Streamlit is not installed. Install with: pip install sfeos-tools[viewer]",
                fg="red",
            )
        )
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo(click.style("\n✓ Viewer stopped", fg="yellow"))
        sys.exit(0)
    except Exception as e:
        error_msg = str(e)
        click.echo(click.style(f"✗ Failed to start viewer: {error_msg}", fg="red"))
        sys.exit(1)


if __name__ == "__main__":
    cli()
