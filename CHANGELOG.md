# SFEOS Tools Change Log

All notable changes to this project will be documented in this file.

The format is (loosely) based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

### Added

- Language option is added to ingest-catalog CLI command to filter ingestion based on language when the rdf/xml file is multilingual.
- Added SKOS concept url as exact match link for tracability

### Updated

- Fixed urljoin to keep routes when appending addresses.

## [v0.7.0] - 2026-04-30

### Added

- Added authentication support (`--api-key`, `--user`, `--password`, `--use-ssl/--no-ssl` options) to all STAC API commands (`load-data`, `crawl-graph`, `visualize-graph`). Supports both API key and basic authentication with mutual exclusivity validation. Includes comprehensive test suite with 9 authentication-specific test cases.

## [v0.6.0] - 2026-04-22

### Added

- Added `crawl-graph` command to crawl SFEOS Multi-Tenant Catalogs and Collections, building and displaying the DAG with support for poly-hierarchies. Features include STAC-compliant pagination (both `rel="next"` links and limit/offset), mathematically-deduced root catalog detection using NetworkX `in_degree()`, safe poly-hierarchy visualization with `(🔗 Poly-Linked)` indicators, and dual output formats (text tree and JSON graph). Requires optional `networkx` dependency (installable via `pip install sfeos-tools[crawler]`).
- Added `visualize-graph` command to generate an interactive, physics-simulated web visualization of the SFEOS catalog hierarchy. Features include drag-and-drop node manipulation, automatic browser launch, color-coded nodes (blue for standard catalogs, green for leaf nodes, orange diamonds for poly-hierarchical nodes), hierarchical tree layout with multiple layout options (hierarchical, hierarchical-lr, force, spring), and strict horizontal level alignment for DAGs. Requires optional `pyvis` dependency (installable via `pip install sfeos-tools[visualizer]`).

### Updated

- Fixed poly-hierarchy support in `ingest_from_xml()` to properly handle nodes with multiple parents. Changed hierarchy mapping from single parent (`g.value()`) to multiple parents (`g.objects()`), ensuring all parent-child relationships are created via POST requests to `/catalogs/{parent_id}/catalogs`. This enables complete DAG ingestion for complex taxonomies with diamond dependencies and multi-parent nodes.

## [v0.5.0] - 2026-04-18

### Added

- Added API key authentication support (`--api-key` option) for all database commands (`add-bbox-shape`, `reindex`) and STAC API commands (`ingest-catalog`). Includes `set_es_env_vars()` shared function for centralized environment variable setup and comprehensive test suite with 14 test cases.
- Added SKOS/RDF-XML demo notebook with example traffic signs taxonomy

## [v0.4.0] - 2026-03-14

### Added

- Added `ingest-catalog` command to create STAC catalogs and sub-catalogs from SKOS/RDF-XML files
- Added `catalog_ingestion` module with support for parsing RDF/XML thesaurus files and establishing hierarchical catalog relationships
- Added comprehensive test suite for catalog ingestion with 19 test cases covering XML parsing, hierarchy establishment, and semantic link preservation
- Added `cli_options` module with reusable decorators for standardized connection options across CLI commands

### Changed

- Standardized CLI connection options across all commands using `@database_options` and `@stac_api_options` decorators
- Renamed `load-data` command's `--base-url` option to `--stac-url` for consistency with other STAC API commands
- Moved test data files (`skos-test-topics.rdf`, `skos-test-topics-2.rdf`) to `tests/` directory for better organization

### Dependencies

- Added `rdflib>=6.0.0` for RDF/XML parsing
- Added `requests>=2.28.0` for HTTP requests

## [v0.3.0] - 2025-10-24

### Added

- Added `viewer` command to launch interactive Streamlit-based web viewer for exploring STAC collections and items. [#3](https://github.com/healy-hyperspatial/sfeos-tools/pull/3)

## [v0.2.0] - 2025-10-21

### Added

- Added `load-data` command to load STAC items into the database via the SFEOS STAC API [#2](https://github.com/healy-hyperspatial/sfeos-tools/pull/2)

## [v0.1.1] - 2025-10-21

### Changed

- Moved `add_bbox_shape` logic from `cli.py` to new `bbox_shape.py` module for better code organization [#1](https://github.com/healy-hyperspatial/sfeos-tools/pull/1)

### Added

- Comprehensive test suite for `bbox_shape` module with 9 test cases covering both Elasticsearch and OpenSearch backends [#1](https://github.com/healy-hyperspatial/sfeos-tools/pull/1)

### Removed

- Python 3.8 support [#1](https://github.com/healy-hyperspatial/sfeos-tools/pull/1)

## [v0.1.0] - 2025-10-15 [#1](https://github.com/healy-hyperspatial/sfeos-tools/pull/1)

### Added

- Initial release

[Unreleased]: https://github.com/healy-hyperspatial/sfeos-tools/compare/v0.7.0..main
[v0.7.0]: https://github.com/healy-hyperspatial/sfeos-tools/compare/v0.6.0...v0.7.0
[v0.6.0]: https://github.com/healy-hyperspatial/sfeos-tools/compare/v0.5.0...v0.6.0
[v0.5.0]: https://github.com/healy-hyperspatial/sfeos-tools/compare/v0.4.0...v0.5.0
[v0.4.0]: https://github.com/healy-hyperspatial/sfeos-tools/compare/v0.3.0...v0.4.0
[v0.3.0]: https://github.com/healy-hyperspatial/sfeos-tools/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/healy-hyperspatial/sfeos-tools/compare/v0.1.1...v0.2.0
[v0.1.1]: https://github.com/healy-hyperspatial/sfeos-tools/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/healy-hyperspatial/sfeos-tools/compare/v0.1.0
