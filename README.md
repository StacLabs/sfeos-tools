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

### For Viewer

To use the interactive Streamlit viewer:

```bash
pip install sfeos-tools[viewer]
```

Or for local development:
```bash
pip install -e sfeos_tools[viewer]
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

📓 **[SKOS-catalogs-ingestion-demo.ipynb](./SKOS-catalogs-ingestion-demo.ipynb)**

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

**STAC API Commands** (`load-data`, `ingest-catalog`, `viewer`):
- `--stac-url`: STAC API base URL (default: http://localhost:8080)

**Authentication Options** (optional for STAC API commands):
- `--user`: Username for basic authentication
- `--password`: Password for basic authentication
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
- `--yes`: Skip confirmation prompt

Examples:
```bash
# Reindex Elasticsearch with custom host and no SSL
sfeos-tools reindex --backend elasticsearch --host localhost --port 9200 --no-ssl --yes

# Reindex OpenSearch with default settings
sfeos-tools reindex --backend opensearch --yes
```

### load-data

**Note:** to enable this functionality, the `ENABLE_CATALOGS_ROUTE` env var needs to be set in SFEOS. 

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

**Data Directory Structure:**

Your data directory should contain:
- `collection.json`: STAC collection definition
- One or more `.json` files: Feature collections with STAC items

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
```

### ingest-catalog

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
- `--user`: Username for basic authentication (optional)
- `--password`: Password for basic authentication (optional)
- `--use-ssl/--no-ssl`: Enable or disable SSL verification (optional)

Examples:
```bash
# Ingest test data from the tests directory (uses default localhost:8080)
sfeos-tools ingest-catalog --xml-file tests/skos-test-topics.rdf

# Ingest with explicit STAC API URL
sfeos-tools ingest-catalog --xml-file traffic-signs.rdf --stac-url http://localhost:8080

# Ingest with basic authentication
sfeos-tools ingest-catalog --xml-file concepts.xml --stac-url https://my-stac-api.com --user myuser --password mypass

# Ingest with SSL verification disabled
sfeos-tools ingest-catalog --xml-file concepts.xml --stac-url https://my-stac-api.com --no-ssl

# Ingest with authentication and custom SSL settings
sfeos-tools ingest-catalog --xml-file /path/to/concepts.xml --stac-url https://my-stac-api.com --user admin --password secret --no-ssl
```

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
