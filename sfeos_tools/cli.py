"""SFEOS CLI Tools - Utilities for managing stac-fastapi-elasticsearch-opensearch deployments.

This tool provides various utilities for managing and maintaining SFEOS deployments,
including database migrations, maintenance tasks, and more.

Usage:
    sfeos-tools add-bbox-shape --backend elasticsearch
    sfeos-tools add-bbox-shape --backend opensearch
"""

import asyncio
import json
import logging
import os
import sys
import webbrowser
from urllib.parse import urljoin

import click
import requests

try:
    import networkx as nx
except ImportError:
    nx = None

try:
    from pyvis.network import Network
except ImportError:
    Network = None

try:
    from importlib.metadata import version as _get_version
except ImportError:
    from importlib_metadata import version as _get_version  # type: ignore[no-redef]

__version__ = _get_version("sfeos-tools")

from .bbox_shape import run_add_bbox_shape
from .catalog_ingestion import ingest_from_xml
from .cli_options import (
    auth_options,
    configure_session_auth,
    database_options,
    lang_options,
    prepare_auth_headers_and_verify,
    set_es_env_vars,
    stac_api_options,
    validate_auth_options,
)
from .data_loader import load_items
from .graph_visualization import enhance_html_with_legend, get_layout_options
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
def add_bbox_shape(backend, host, port, use_ssl, user, password, api_key):
    """Add bbox_shape field to existing collections for spatial search support.

    This migration is required for collections created before spatial search
    was added. Collections created or updated after this feature will
    automatically have the bbox_shape field.

    Examples:
        sfeos_tools.py add-bbox-shape --backend elasticsearch
        sfeos_tools.py add-bbox-shape --backend opensearch --host db.example.com --port 9200
        sfeos_tools.py add-bbox-shape --backend elasticsearch --no-ssl --host localhost
        sfeos_tools.py add-bbox-shape --backend elasticsearch --api-key your-api-key
    """
    set_es_env_vars(
        host=host,
        port=port,
        use_ssl=use_ssl,
        user=user,
        password=password,
        api_key=api_key,
    )

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
def reindex(backend, host, port, use_ssl, user, password, api_key, yes):
    """Reindex all STAC indexes to the next version and update aliases.

    For Elasticsearch, this runs a migration that:
    - Creates/updates index templates
    - Reindexes collections and item indexes to a new version
    - Applies asset migration script for compatibility
    - Switches aliases to the new indexes
    """
    backend = backend.lower()

    if not yes:
        proceed = click.confirm(
            "This will reindex all collections and item indexes and update aliases. Proceed?",
            default=False,
        )
        if not proceed:
            click.echo(click.style("Aborted", fg="yellow"))
            return

    set_es_env_vars(
        host=host,
        port=port,
        use_ssl=use_ssl,
        user=user,
        password=password,
        api_key=api_key,
    )

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
@auth_options
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
def load_data(
    stac_url: str,
    use_ssl: bool,
    user: str,
    password: str,
    api_key: str,
    collection_id: str,
    use_bulk: bool,
    data_dir: str,
) -> None:
    """Load STAC items into the database via STAC API.

    This command loads a STAC collection and its items from local JSON files
    into a STAC API instance. It expects a directory containing:
    - collection.json: The STAC collection definition
    - One or more feature collection JSON files with STAC items

    Supports both basic authentication (username/password) and API key authentication.

    Examples:
        sfeos-tools load-data --stac-url http://localhost:8080
        sfeos-tools load-data --stac-url http://localhost:8080 --collection-id my-collection --use-bulk
        sfeos-tools load-data --stac-url http://localhost:8080 --data-dir /path/to/data
        sfeos-tools load-data --stac-url https://my-api.com --api-key my-key
        sfeos-tools load-data --stac-url https://my-api.com --user admin --password secret
    """
    from httpx import Client

    validate_auth_options(user, password, api_key)

    try:
        headers, auth, verify = prepare_auth_headers_and_verify(
            user, password, api_key, use_ssl
        )

        with Client(
            base_url=stac_url, headers=headers, auth=auth, verify=verify
        ) as client:
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
@lang_options
def ingest_catalog(
    xml_file: str, stac_url: str, use_ssl: bool, user: str, password: str, api_key: str, lang: str
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
    validate_auth_options(user, password, api_key)

    try:
        ingest_from_xml(
            xml_file,
            stac_url,
            user=user,
            password=password,
            use_ssl=use_ssl,
            api_key=api_key,
            lang=lang
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

        # Provide helpful hints for specific error types
        if isinstance(e, requests.exceptions.ConnectionError):
            click.echo(
                click.style(
                    "\nHint: Connection refused. Make sure your STAC API is running and accessible at the specified URL",
                    fg="yellow",
                )
            )
        elif isinstance(e, requests.exceptions.Timeout):
            click.echo(
                click.style(
                    "\nHint: Request timeout. The STAC API took too long to respond. Check your network connection and the API status",
                    fg="yellow",
                )
            )
        elif isinstance(e, requests.exceptions.RequestException):
            click.echo(
                click.style(
                    "\nHint: Network request failed. Check your STAC API URL and network connectivity",
                    fg="yellow",
                )
            )
        elif isinstance(e, Exception) and "parse" in error_msg.lower():
            click.echo(
                click.style(
                    "\nHint: XML parsing error. Verify that the XML file is valid RDF/XML format",
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


def _fetch_all_paginated(
    session: requests.Session, initial_url: str, items_key: str
) -> list:
    """Help exhaust a STAC paginated endpoint by following 'next' links.

    Handles both traditional STAC pagination (rel="next" links) and limit-based
    pagination (numberMatched/numberReturned fields).

    Args:
        session: Requests session for HTTP calls
        initial_url: Starting URL for the paginated endpoint
        items_key: Key in the JSON response containing the items (e.g., 'catalogs', 'children')

    Returns:
        List of all items across all pages
    """
    items = []
    next_url = initial_url

    while next_url:
        try:
            response = session.get(next_url, timeout=10)
            response.raise_for_status()
            data = response.json()

            items.extend(data.get(items_key, []))

            links = data.get("links", [])
            next_link = next(
                (link for link in links if link.get("rel") == "next"), None
            )

            if next_link:
                next_url = next_link["href"]
            else:
                number_matched = data.get("numberMatched")
                number_returned = data.get("numberReturned")

                if (
                    number_matched is not None
                    and number_returned is not None
                    and len(items) < number_matched
                ):
                    limit = number_returned
                    offset = len(items)
                    separator = "&" if "?" in initial_url else "?"
                    next_url = f"{initial_url}{separator}limit={limit}&offset={offset}"
                else:
                    next_url = None

        except requests.exceptions.RequestException:
            break

    return items


@cli.command("crawl-graph")
@click.option(
    "--url",
    default="http://localhost:8080",
    help="The base URL of the SFEOS API.",
)
@click.option(
    "--output",
    type=click.Choice(["text", "json"]),
    default="text",
    help="Format to display the graph.",
)
@auth_options
def crawl_graph(
    url: str, output: str, use_ssl: bool, user: str, password: str, api_key: str
) -> None:
    """Crawl the SFEOS Multi-Tenant Catalogs and Collections to build and display the DAG.

    This command traverses the SFEOS API hierarchy, discovering all catalogs and
    collections regardless of whether the backend has a dedicated graph endpoint.
    It automatically detects true root catalogs by analyzing graph structure and
    handles STAC-compliant pagination to ensure no entities are missed.

    Supports both basic authentication (username/password) and API key authentication.

    Examples:
        sfeos-tools crawl-graph
        sfeos-tools crawl-graph --url http://localhost:8080
        sfeos-tools crawl-graph --url https://my-sfeos-api.com --output json
        sfeos-tools crawl-graph --url https://my-sfeos-api.com --api-key my-key
        sfeos-tools crawl-graph --url https://my-sfeos-api.com --user admin --password secret
    """
    if nx is None:
        click.echo(
            click.style(
                "✗ networkx is not installed. Install with: pip install networkx",
                fg="red",
            )
        )
        sys.exit(1)

    validate_auth_options(user, password, api_key)

    click.echo(click.style(f"🌲 Crawling SFEOS Multi-Tenant API at {url}...", fg="cyan"))

    dag = nx.DiGraph()
    session = requests.Session()
    configure_session_auth(session, user, password, api_key, use_ssl)
    url = url if url.endswith("/") else url + "/"
    catalogs_endpoint = urljoin(url, "catalogs?limit=100")
    try:
        all_catalogs = _fetch_all_paginated(session, catalogs_endpoint, "catalogs")

        for catalog in all_catalogs:
            dag.add_node(catalog["id"], type="Catalog")

    except requests.exceptions.RequestException as e:
        click.echo(click.style(f"❌ Failed to reach {catalogs_endpoint}: {e}", fg="red"))
        sys.exit(1)

    with click.progressbar(
        all_catalogs, label="Traversing Hierarchy", show_pos=True
    ) as bar:
        for catalog in bar:
            current_id = catalog["id"]

            try:
                children_endpoint = urljoin(url, f"catalogs/{current_id}/children")
                children = _fetch_all_paginated(session, children_endpoint, "children")

                for child in children:
                    child_id = child["id"]
                    child_type = child.get("type", "Unknown")

                    if child_id not in dag:
                        dag.add_node(child_id, type=child_type)

                    dag.add_edge(current_id, child_id)

            except requests.exceptions.RequestException:
                click.echo(
                    click.style(
                        f"\n⚠️ Failed to fetch children for {current_id}",
                        fg="yellow",
                    )
                )

            try:
                collections_endpoint = urljoin(
                    url, f"catalogs/{current_id}/collections"
                )
                collections = _fetch_all_paginated(
                    session, collections_endpoint, "collections"
                )

                for collection in collections:
                    collection_id = collection["id"]

                    if collection_id not in dag:
                        dag.add_node(collection_id, type="Collection")

                    dag.add_edge(current_id, collection_id)

            except requests.exceptions.RequestException:
                pass

    true_roots = [n for n, d in dag.in_degree() if d == 0]

    dag.add_node("ROOT", type="Virtual")
    for root in true_roots:
        dag.add_edge("ROOT", root)

    click.echo(
        click.style(
            f"\n✅ Crawl complete! Discovered {dag.number_of_nodes() - 1} entities.",
            fg="green",
        )
    )

    if output == "json":
        graph_json = nx.node_link_data(dag)
        click.echo(json.dumps(graph_json, indent=2))
    else:
        click.echo(click.style("\n--- SFEOS Topology ---", fg="cyan", bold=True))
        _print_tree(dag, "ROOT", level=0, printed_nodes=set())


def _print_tree(
    graph: nx.DiGraph, node_id: str, level: int, printed_nodes: set
) -> None:
    """Recursively prints a terminal-friendly tree view of the graph.

    Handles poly-hierarchies safely by tracking printed nodes. If a node has
    multiple parents and is reached via a second parent, it displays a poly-link
    indicator instead of recursing again.

    Args:
        graph: NetworkX DiGraph of the catalog hierarchy
        node_id: Current node to print
        level: Current depth level for indentation
        printed_nodes: Set of nodes already fully printed (to prevent infinite loops)
    """
    indent = "  " * level
    if level > 0:
        node_data = graph.nodes.get(node_id, {})
        node_type = node_data.get("type", "Unknown")
        icon = "📁" if node_type == "Catalog" else "📄"

        if node_id in printed_nodes:
            click.echo(f"{indent}└─ {icon} {node_id} (🔗 Poly-Linked)")
            return

        click.echo(f"{indent}└─ {icon} {node_id}")
        printed_nodes.add(node_id)

    children = list(graph.successors(node_id))
    for child in sorted(children):
        _print_tree(graph, child, level + 1, printed_nodes)


@cli.command("visualize-graph")
@click.option(
    "--url",
    default="http://localhost:8080",
    help="The base URL of the SFEOS API.",
)
@click.option(
    "--layout",
    type=click.Choice(
        ["hierarchical", "hierarchical-lr", "force", "spring"], case_sensitive=False
    ),
    default="hierarchical",
    help="Graph layout style: hierarchical (top-to-bottom tree), hierarchical-lr (left-to-right tree), force (physics-based), or spring (organic).",
)
@auth_options
def visualize_graph(
    url: str, layout: str, use_ssl: bool, user: str, password: str, api_key: str
) -> None:
    """Crawl SFEOS API and open an interactive web visualization of the DAG.

    This command builds a physics-simulated, drag-and-drop HTML dashboard showing
    the complete catalog hierarchy. Poly-hierarchical nodes are highlighted as
    orange diamonds, and the visualization opens automatically in your browser.

    Supports both basic authentication (username/password) and API key authentication.

    Examples:
        sfeos-tools visualize-graph
        sfeos-tools visualize-graph --url http://localhost:8080
        sfeos-tools visualize-graph --layout hierarchical-lr
        sfeos-tools visualize-graph --layout force
        sfeos-tools visualize-graph --url https://my-sfeos-api.com --layout spring
        sfeos-tools visualize-graph --url https://my-sfeos-api.com --api-key my-key
        sfeos-tools visualize-graph --url https://my-sfeos-api.com --user admin --password secret
    """
    if nx is None:
        click.echo(
            click.style(
                "✗ networkx is not installed. Install with: pip install networkx",
                fg="red",
            )
        )
        sys.exit(1)

    if Network is None:
        click.echo(
            click.style(
                "✗ pyvis is not installed. Install with: pip install sfeos-tools[visualizer]",
                fg="red",
            )
        )
        sys.exit(1)

    validate_auth_options(user, password, api_key)

    click.echo(click.style(f"🕸️ Crawling {url} for visualization...", fg="cyan"))

    dag = nx.DiGraph()
    session = requests.Session()
    configure_session_auth(session, user, password, api_key, use_ssl)
    url = url if url.endswith("/") else url + "/"
    catalogs_endpoint = urljoin(url, "catalogs?limit=100")
    try:
        all_catalogs = _fetch_all_paginated(session, catalogs_endpoint, "catalogs")

        for catalog in all_catalogs:
            dag.add_node(
                catalog["id"],
                label=catalog.get("title", catalog["id"]),
                title=f"ID: {catalog['id']}",
                type="Catalog",
            )

    except requests.exceptions.RequestException as e:
        click.echo(click.style(f"❌ Failed to reach {catalogs_endpoint}: {e}", fg="red"))
        sys.exit(1)

    with click.progressbar(
        all_catalogs, label="Building Web Graph", show_pos=True
    ) as bar:
        for catalog in bar:
            current_id = catalog["id"]

            try:
                children_endpoint = urljoin(url, f"catalogs/{current_id}/children")
                children = _fetch_all_paginated(session, children_endpoint, "children")

                for child in children:
                    child_id = child["id"]
                    child_type = child.get("type", "Unknown")

                    if child_id not in dag:
                        dag.add_node(
                            child_id,
                            label=child.get("title", child_id),
                            title=f"ID: {child_id}",
                            type=child_type,
                        )

                    dag.add_edge(current_id, child_id)

            except requests.exceptions.RequestException:
                pass

            try:
                collections_endpoint = urljoin(
                    url, f"catalogs/{current_id}/collections"
                )
                collections = _fetch_all_paginated(
                    session, collections_endpoint, "collections"
                )

                for collection in collections:
                    collection_id = collection["id"]

                    if collection_id not in dag:
                        dag.add_node(
                            collection_id,
                            label=collection.get("title", collection_id),
                            title=f"ID: {collection_id}",
                            type="Collection",
                        )

                    dag.add_edge(current_id, collection_id)

            except requests.exceptions.RequestException:
                pass

    true_roots = [n for n, d in dag.in_degree() if d == 0]

    dag.add_node(
        "ROOT",
        label="🌐 STAC API",
        title="Virtual Root Node",
        color="#ff0040",
        size=30,
    )
    for root in true_roots:
        if root != "ROOT":
            dag.add_edge("ROOT", root)

    try:
        node_levels = nx.single_source_shortest_path_length(dag, "ROOT")
        for node_id, level_depth in node_levels.items():
            dag.nodes[node_id]["level"] = level_depth
    except Exception as e:
        click.secho(f"⚠️ Could not strictly align levels: {e}", fg="yellow")

    for node in dag.nodes:
        if node == "ROOT":
            continue

        node_type = dag.nodes[node].get("type", "Unknown")
        in_edges = dag.in_degree(node)
        out_edges = dag.out_degree(node)

        if in_edges > 1:
            dag.nodes[node]["color"] = "#ff9800"
            dag.nodes[node]["shape"] = "diamond"
            dag.nodes[node]["size"] = 32
            dag.nodes[node]["title"] += " (Poly-Linked)"
        elif node_type == "Collection":
            dag.nodes[node]["color"] = "#9C27B0"
            dag.nodes[node]["shape"] = "box"
            dag.nodes[node]["size"] = 28
        elif out_edges == 0:
            dag.nodes[node]["color"] = "#4CAF50"
            dag.nodes[node]["size"] = 25
        else:
            dag.nodes[node]["color"] = "#2196F3"
            dag.nodes[node]["size"] = 28

    click.echo(click.style("\n🎨 Rendering the visualization...", fg="cyan"))

    net = Network(
        height="100vh",
        width="100%",
        directed=True,
        bgcolor="#121212",
        font_color="white",
    )
    net.from_nx(dag)

    options_js = get_layout_options(layout)
    net.set_options(options_js)

    output_file = "sfeos_topology.html"
    net.write_html(output_file)

    with open(output_file, "r", encoding="utf-8") as f:
        html_content = f.read()

    html_content = enhance_html_with_legend(html_content, layout)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    filepath = f"file://{os.path.abspath(output_file)}"
    webbrowser.open(filepath)

    click.echo(
        click.style(
            f"✅ Dashboard opened in your browser! (saved to {output_file})",
            fg="green",
        )
    )


if __name__ == "__main__":
    cli()
