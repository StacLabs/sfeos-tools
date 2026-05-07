<!-- markdownlint-disable MD033 MD041 -->


<p align="left">
  <img src="https://github.com/Healy-Hyperspatial/sfeos-web/blob/main/sfeos-web/public/assets/sfeos-logo.png">
</p>

CLI tools for managing [stac-fastapi-elasticsearch-opensearch](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch) deployments.


<!-- **Jump to:** [Project Introduction](#project-introduction---what-is-sfeos) | [Quick Start](#quick-start) | [Table of Contents](#table-of-contents) -->

  [![Downloads](https://static.pepy.tech/badge/sfeos-tools?color=blue)](https://pepy.tech/project/sfeos-tools)
  [![GitHub contributors](https://img.shields.io/github/contributors/healy-hyperspatial/sfeos-tools?color=blue)](https://github.com/healy-hyperspatial/sfeos-tools/graphs/contributors)
  [![GitHub stars](https://img.shields.io/github/stars/healy-hyperspatial/sfeos-tools.svg?color=blue)](https://github.com/healy-hyperspatial/sfeos-tools/stargazers)
  [![GitHub forks](https://img.shields.io/github/forks/healy-hyperspatial/sfeos-tools.svg?color=blue)](https://github.com/healy-hyperspatial/sfeos-tools/network/members)
   [![PyPI version](https://img.shields.io/pypi/v/sfeos-tools.svg?color=blue)](https://pypi.org/project/sfeos-tools/)
  [![STAC](https://img.shields.io/badge/STAC-1.1.0-blue.svg)](https://github.com/radiantearth/stac-spec/tree/v1.1.0)

## Table of Contents

- [Installation](#installation)
  - [For Elasticsearch](#for-elasticsearch)
  - [For OpenSearch](#for-opensearch)
  - [For Viewer](#for-viewer)
  - [For Development](#for-development-both-backends)
- [Usage](#usage)
- [Interactive Demo](#interactive-demo-skos-to-stac)
- [Commands](#commands)
  - [add-bbox-shape](#add-bbox-shape)
  - [reindex](#reindex)
  - [load-data](#load-data)
  - [ingest-catalog](#ingest-catalog)
  - [crawl-graph](#crawl-graph)
  - [visualize-graph](#visualize-graph)
  - [viewer](#viewer)
- [Development](#development)
- [License](#license)

## Installation

### For Elasticsearch

```bash
pip install sfeos-tools[elasticsearch]
```

Or for local development:
```bash
pip install -e sfeos_tools[elasticsearch]
```

### For OpenSearch

```bash
pip install sfeos-tools[opensearch]
```

Or for local development:
```bash
pip install -e sfeos_tools[opensearch]
```

### For Crawler

To use the `crawl-graph` command:

```bash
pip install sfeos-tools[crawler]
```

Or for local development:
```bash
pip install -e sfeos_tools[crawler]
```

### For Visualizer

To use the `visualize-graph` command:

```bash
pip install sfeos-tools[visualizer]
```

Or for local development:
```bash
pip install -e sfeos_tools[visualizer]
```

### For Viewer

To use the interactive Streamlit viewer:

```bash
pip install sfeos-tools[viewer]
```

Or for local development:
```bash
pip install -e sfeos_tools[viewer]
```

### For All Features

To install all optional dependencies (Elasticsearch, OpenSearch, Crawler, Visualizer, and Viewer):

```bash
pip install sfeos-tools[all]
```

Or for local development:
```bash
pip install -e sfeos_tools[all]
```

### For Development (both backends)

```bash
pip install sfeos-tools[dev]
```

Or for local development:
```bash
pip install -e sfeos_tools[dev]
```

## Usage

After installation, the `sfeos-tools` command will be available:

```bash
# View available commands
sfeos-tools --help

# View version
sfeos-tools --version
```

## Interactive Demo: SKOS to STAC

Want to see the Multi-Tenant Catalogs extension and the `ingest-catalog` command in action? Check out our interactive Jupyter Notebook tutorial:

📓 **[SKOS-catalogs-ingestion-demo.ipynb](./demo-notebooks/SKOS-catalogs-ingestion-demo.ipynb)**

This notebook walks through a real-world GIS use case (a Traffic Signs taxonomy) and demonstrates:
- **Automated Semantic Ingestion:** Translating a SKOS RDF file directly into a STAC catalog hierarchy.
- **Poly-hierarchy (DAG):** Discovering a single spatial asset across multiple departmental catalogs (e.g., Regulatory vs. Warning signs).
- **Contextual Breadcrumbs:** How SFEOS dynamically rewrites `rel="parent"` and `rel="child"` links based on your navigation path.
- **Data Safety:** Deleting virtual organizational containers without destroying the underlying STAC features.

## Commands

### Standardized CLI Options

The CLI tools use standardized options across commands for consistency:

**Database Commands** (`add-bbox-shape`, `reindex`):
- `--host`: Database host (default: localhost or ES_HOST env var)
- `--port`: Database port (default: 9200 for ES, 9202 for OS, or ES_PORT env var)
- `--use-ssl/--no-ssl`: SSL connection flag (default: true or ES_USE_SSL env var)
- `--user`: Database username (default: ES_USER env var)
- `--password`: Database password (default: ES_PASS env var)
- `--api-key`: API key for authentication (default: ES_API_KEY env var)

**STAC API Commands** (`load-data`, `ingest-catalog`, `viewer`):
- `--stac-url`: STAC API base URL (default: http://localhost:8080)

**Authentication Options** (optional for STAC API commands):
- `--user`: Username for basic authentication
- `--password`: Password for basic authentication
- `--api-key`: API key for authentication
- `--use-ssl/--no-ssl`: SSL verification flag

### add-bbox-shape

Adds a `bbox_shape` field to existing collections for spatial search support. This migration is required for collections created before spatial search was added. Collections created or updated after this feature will automatically have the `bbox_shape` field.

```bash
sfeos-tools add-bbox-shape --backend [elasticsearch|opensearch] [options]
```

Options:
- `--backend`: Database backend to use (required, choices: elasticsearch, opensearch)
- `--host`: Database host (default: localhost or ES_HOST env var)
- `--port`: Database port (default: 9200 for ES, 9202 for OS, or ES_PORT env var)
- `--use-ssl/--no-ssl`: Use SSL connection (default: true or ES_USE_SSL env var)
- `--user`: Database username (default: ES_USER env var)
- `--password`: Database password (default: ES_PASS env var)
- `--api-key`: API key for authentication (default: ES_API_KEY env var)

Examples:
```bash
# Add bbox_shape with default settings
sfeos-tools add-bbox-shape --backend elasticsearch

# Add bbox_shape with custom host and port
sfeos-tools add-bbox-shape --backend elasticsearch --host db.example.com --port 9200

# Add bbox_shape with API key authentication
sfeos-tools add-bbox-shape --backend elasticsearch --api-key your-api-key

# Add bbox_shape with API key and custom host
sfeos-tools add-bbox-shape --backend opensearch --host prod.example.com --api-key your-api-key
```

### reindex

Reindexes all STAC indexes to the next version and updates aliases. This command performs the following actions:
- Creates/updates index templates
- Reindexes collections and item indexes to a new version
- Applies asset migration script for compatibility
- Switches aliases to the new indexes

```bash
sfeos-tools reindex --backend [elasticsearch|opensearch] [options]
```

Options:
- `--backend`: Database backend to use (required, choices: elasticsearch, opensearch)
- `--host`: Database host (default: localhost or ES_HOST env var)
- `--port`: Database port (default: 9200 for ES, 9202 for OS, or ES_PORT env var)
- `--use-ssl/--no-ssl`: Use SSL connection (default: true or ES_USE_SSL env var)
- `--user`: Database username (default: ES_USER env var)
- `--password`: Database password (default: ES_PASS env var)
- `--api-key`: API key for authentication (default: ES_API_KEY env var)
- `--yes`: Skip confirmation prompt

Examples:
```bash
# Reindex Elasticsearch with custom host and no SSL
sfeos-tools reindex --backend elasticsearch --host localhost --port 9200 --no-ssl --yes

# Reindex OpenSearch with default settings
sfeos-tools reindex --backend opensearch --yes

# Reindex with API key authentication
sfeos-tools reindex --backend elasticsearch --api-key your-api-key --yes

# Reindex with API key and custom host
sfeos-tools reindex --backend opensearch --host prod.example.com --api-key your-api-key --yes
```

### load-data

Load STAC collections and items from local JSON files into a STAC API instance. This command is useful for:
- Populating a new STAC API deployment with test data
- Migrating data between STAC API instances
- Bulk loading STAC collections and items

```bash
sfeos-tools load-data --stac-url <stac-api-url> [options]
```

Options:
- `--stac-url`: STAC API base URL (default: http://localhost:8080)
- `--collection-id`: ID of the collection to create/update (default: test-collection)
- `--data-dir`: Directory containing collection.json and feature collection files (default: sample_data/)
- `--use-bulk`: Use bulk insert method for items (faster for large datasets)
- `--use-ssl/--no-ssl`: SSL connection flag (default: true)
- `--user`: Username for basic authentication (default: ES_USER env var)
- `--password`: Password for basic authentication (default: ES_PASS env var)
- `--api-key`: API key for authentication (default: ES_API_KEY env var)

**Data Directory Structure:**

Your data directory should contain:
- `collection.json`: STAC collection definition
- One or more `.json` files: Feature collections with STAC items

**Authentication:**

The command supports both basic authentication (username/password) and API key authentication. Provide EITHER username/password OR an API key, not both.

Examples:
```bash
# Load data from default directory
sfeos-tools load-data --stac-url http://localhost:8080

# Load with custom collection ID and bulk insert
sfeos-tools load-data \
  --stac-url http://localhost:8080 \
  --collection-id my-collection \
  --use-bulk

# Load from custom directory
sfeos-tools load-data \
  --stac-url http://localhost:8080 \
  --data-dir /path/to/stac/data \
  --collection-id production-data

# Load with API key authentication
sfeos-tools load-data \
  --stac-url https://my-api.com \
  --api-key your-api-key \
  --collection-id production-data

# Load with basic authentication
sfeos-tools load-data \
  --stac-url https://my-api.com \
  --user admin \
  --password secret \
  --collection-id production-data
```

### ingest-catalog

**Note:** to enable this functionality, the `ENABLE_CATALOGS_ROUTE` env var needs to be set in SFEOS. 

Ingest SKOS/RDF-XML files to create STAC catalogs and sub-catalogs. This command parses RDF/XML files containing SKOS concepts and creates a hierarchical catalog structure in the STAC API. It handles:
- Creating catalogs from SKOS concepts
- Establishing parent-child relationships (skos:narrower)
- Preserving semantic links (skos:related, skos:exactMatch, etc.)
- Including metadata from definitions and modification dates

```bash
sfeos-tools ingest-catalog --xml-file <path-to-rdf-xml> [options]
```

Options:
- `--xml-file`: Path to RDF/XML file containing SKOS concepts (required)
- `--stac-url`: STAC API base URL (default: http://localhost:8080)
- `--lang`: Language code to filter multilingual RDF labels (e.g., `en`, `fr`, `es`). If not specified, the first available label is used (default: None)
- `--user`: Username for basic authentication (optional)
- `--password`: Password for basic authentication (optional)
- `--api-key`: API key for authentication (optional)
- `--use-ssl/--no-ssl`: Enable or disable SSL verification (optional)

**Language Filtering:**

The `--lang` option allows you to specify which language to extract from multilingual RDF files. If your RDF file contains labels in multiple languages (e.g., `<skos:prefLabel xml:lang="en">Observation Domain</skos:prefLabel>`), you can filter to a specific language. If `--lang` is omitted, the tool uses the first available label regardless of language, ensuring robust handling of both monolingual and multilingual RDF files.

Examples:
```bash
# Ingest test data from the tests directory (uses default localhost:8080)
sfeos-tools ingest-catalog --xml-file tests/skos-test-topics.rdf

# Ingest with explicit STAC API URL
sfeos-tools ingest-catalog --xml-file demo-notebooks/traffic-signs.rdf --stac-url http://localhost:8080

# Ingest with language filtering (extract only English labels)
sfeos-tools ingest-catalog --xml-file concepts.xml --stac-url http://localhost:8080 --lang en

# Ingest with language filtering (extract only French labels)
sfeos-tools ingest-catalog --xml-file concepts.xml --stac-url http://localhost:8080 --lang fr

# Ingest with basic authentication
sfeos-tools ingest-catalog --xml-file concepts.xml --stac-url https://my-stac-api.com --user myuser --password mypass

# Ingest with API key authentication
sfeos-tools ingest-catalog --xml-file concepts.xml --stac-url https://my-stac-api.com --api-key your-api-key

# Ingest with SSL verification disabled
sfeos-tools ingest-catalog --xml-file concepts.xml --stac-url https://my-stac-api.com --no-ssl

# Ingest with language filtering and API key authentication
sfeos-tools ingest-catalog --xml-file /path/to/concepts.xml --stac-url https://my-stac-api.com --lang en --api-key your-api-key --no-ssl
```

### crawl-graph

Crawl the SFEOS Multi-Tenant Catalogs and Collections to build and display the Directed Acyclic Graph (DAG) of your SFEOS instance. This command traverses the entire catalog hierarchy using breadth-first search, discovering all catalogs and collections regardless of whether the backend has a dedicated graph endpoint installed.

**Key Features:**
- Discovers all catalogs and collections in your SFEOS instance
- Works with any SFEOS deployment (no special endpoints required)
- Prevents infinite loops in poly-hierarchy DAGs using a visited set
- Two output formats: hierarchical text tree or JSON graph data
- Progress tracking with visual progress bar
- Graceful error handling for connection failures

```bash
sfeos-tools crawl-graph [options]
```

Options:
- `--url`: Base URL of the SFEOS API (default: http://localhost:8080)
- `--output`: Output format: `text` for hierarchical tree view or `json` for graph data (default: text)
- `--use-ssl/--no-ssl`: SSL connection flag (default: true)
- `--user`: Username for basic authentication (default: ES_USER env var)
- `--password`: Password for basic authentication (default: ES_PASS env var)
- `--api-key`: API key for authentication (default: ES_API_KEY env var)

**Authentication:**

The command supports both basic authentication (username/password) and API key authentication. Provide EITHER username/password OR an API key, not both.

**Requirements:**

The crawler requires networkx. Install with:
```bash
pip install sfeos-tools[crawler]
```

Examples:
```bash
# Crawl default localhost instance with text output
sfeos-tools crawl-graph

# Crawl a custom SFEOS instance
sfeos-tools crawl-graph --url https://my-sfeos-api.com

# Get JSON output for programmatic use
sfeos-tools crawl-graph --url http://localhost:8080 --output json

# Crawl custom instance and output as JSON
sfeos-tools crawl-graph --url https://my-sfeos-api.com --output json

# Crawl with API key authentication
sfeos-tools crawl-graph --url https://my-sfeos-api.com --api-key my-key --output json

# Crawl with basic authentication
sfeos-tools crawl-graph --url https://my-sfeos-api.com --user admin --password secret
```

**Example Output (Text Format):**
```
🌲 Crawling SFEOS Multi-Tenant API at http://localhost:8080...
Traversing Hierarchy  [####################################]  100%
✅ Crawl complete! Discovered 13 entities.

--- SFEOS Topology ---
  └─ 📁 tenant-a
    └─ 📁 forestry-dept
      └─ 📄 sentinel-2-l2a
  └─ 📁 tenant-b
    └─ 📄 landsat-8-c2-l2
```

The JSON output provides a complete graph representation suitable for analysis, visualization, or integration with other tools.

### visualize-graph

Generate an interactive, physics-simulated web visualization of your SFEOS catalog hierarchy. This command crawls the API and renders a beautiful, drag-and-drop HTML dashboard showing the complete DAG with color-coded nodes and automatic browser launch.

**Key Features:**
- Physics-simulated hierarchical tree layout with spring forces
- Color-coded nodes for quick visual understanding:
  - 🔵 Blue: Standard catalogs
  - 🟢 Green: Leaf catalogs (no children)
  - 🟣 Purple Boxes: Collections (leaf nodes in STAC hierarchy)
  - 🟠 Orange Diamonds: Poly-hierarchical nodes (multiple parents)
  - 🔴 Red: Virtual root API node
- Interactive drag-and-drop node manipulation
- Hover tooltips showing catalog/collection IDs and metadata
- Dark mode UI for comfortable viewing
- Automatic browser launch with generated HTML file
- Works with any SFEOS deployment (no special endpoints required)

```bash
sfeos-tools visualize-graph [options]
```

Options:
- `--url`: Base URL of the SFEOS API (default: http://localhost:8080)
- `--layout`: Graph layout style (default: hierarchical)
  - `hierarchical`: Tree layout flowing top-to-bottom (best for DAGs)
  - `hierarchical-lr`: Tree layout flowing left-to-right (alternative hierarchical view)
  - `force`: Force-directed layout using ForceAtlas2 algorithm (organic, physics-based)
  - `spring`: Spring-based layout using Barnes-Hut algorithm (natural node spreading)
- `--use-ssl/--no-ssl`: SSL connection flag (default: true)
- `--user`: Username for basic authentication (default: ES_USER env var)
- `--password`: Password for basic authentication (default: ES_PASS env var)
- `--api-key`: API key for authentication (default: ES_API_KEY env var)

**Authentication:**

The command supports both basic authentication (username/password) and API key authentication. Provide EITHER username/password OR an API key, not both.

**Requirements:**

The visualizer requires networkx and pyvis. Install with:
```bash
pip install sfeos-tools[visualizer]
```

Examples:
```bash
# Visualize default localhost instance (hierarchical layout)
sfeos-tools visualize-graph

# Visualize with left-to-right hierarchical layout
sfeos-tools visualize-graph --layout hierarchical-lr

# Visualize with force-directed layout
sfeos-tools visualize-graph --layout force

# Visualize with spring layout
sfeos-tools visualize-graph --layout spring

# Visualize a custom SFEOS instance with hierarchical layout
sfeos-tools visualize-graph --url https://my-sfeos-api.com

# Visualize custom instance with left-to-right layout
sfeos-tools visualize-graph --url https://my-sfeos-api.com --layout hierarchical-lr

# Visualize with API key authentication
sfeos-tools visualize-graph --url https://my-sfeos-api.com --api-key my-key

# Visualize with basic authentication
sfeos-tools visualize-graph --url https://my-sfeos-api.com --user admin --password secret
```

**Output:**

The command generates an `sfeos_topology.html` file and automatically opens it in your default browser. The visualization features:
- Hierarchical tree layout flowing top-to-bottom
- Nodes that can be dragged to explore relationships
- Spring physics that naturally separates nodes
- Poly-hierarchical nodes highlighted as orange diamonds with multiple incoming arrows
- Responsive design that fills your entire browser window

### viewer

Launch an interactive Streamlit-based web viewer for exploring STAC collections and items. The viewer provides:
- Interactive map visualization of STAC items
- Collection browser and selector
- Item search and filtering
- Metadata inspection
- **Asset preview and imagery display**
- Support for thumbnails, images (JPEG, PNG, TIFF), and other asset types

```bash
sfeos-tools viewer [options]
```

Options:
- `--stac-url`: STAC API base URL (default: http://localhost:8080)
- `--port`: Port for the Streamlit viewer (default: 8501)

**Requirements:**

The viewer requires additional dependencies. Install with:
```bash
pip install sfeos-tools[viewer]
```

Examples:
```bash
# Launch viewer with default settings (connects to http://localhost:8080)
sfeos-tools viewer

# Connect to a custom STAC API
sfeos-tools viewer --stac-url https://my-stac-api.com

# Use a different port
sfeos-tools viewer --port 8502

# Custom STAC API and port
sfeos-tools viewer --stac-url http://localhost:8080 --port 8502
```

The viewer will automatically open in your default web browser. Press `Ctrl+C` in the terminal to stop the viewer.

## Development

To develop sfeos-tools locally:

```bash
# Install in editable mode with dev dependencies
pip install -e ./sfeos_tools[dev]

# Run the CLI
sfeos-tools --help

# Run tests
pytest

# Format code
pre-commit install
pre-commit run --all-files
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
