"""Tests for the crawl-graph and visualize-graph CLI commands."""

import json
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from sfeos_tools.cli import _fetch_all_paginated, cli


class TestFetchAllPaginated:
    """Tests for the _fetch_all_paginated helper function."""

    def test_fetch_all_paginated_single_page(self):
        """Test fetching a single page with no pagination."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "catalogs": [{"id": "cat-1"}, {"id": "cat-2"}],
            "links": [],
        }
        mock_session.get.return_value = mock_response

        result = _fetch_all_paginated(
            mock_session, "http://localhost:8080/catalogs", "catalogs"
        )

        assert len(result) == 2
        assert result[0]["id"] == "cat-1"
        assert result[1]["id"] == "cat-2"

    def test_fetch_all_paginated_multiple_pages_with_next_link(self):
        """Test fetching multiple pages by following 'next' links."""
        mock_session = MagicMock()

        page1_response = MagicMock()
        page1_response.json.return_value = {
            "catalogs": [{"id": "cat-1"}, {"id": "cat-2"}],
            "links": [
                {"rel": "next", "href": "http://localhost:8080/catalogs?token=page2"}
            ],
        }

        page2_response = MagicMock()
        page2_response.json.return_value = {
            "catalogs": [{"id": "cat-3"}],
            "links": [],
        }

        mock_session.get.side_effect = [page1_response, page2_response]

        result = _fetch_all_paginated(
            mock_session, "http://localhost:8080/catalogs", "catalogs"
        )

        assert len(result) == 3
        assert result[0]["id"] == "cat-1"
        assert result[1]["id"] == "cat-2"
        assert result[2]["id"] == "cat-3"
        assert mock_session.get.call_count == 2

    def test_fetch_all_paginated_with_limit_offset(self):
        """Test fetching multiple pages using limit/offset pagination (numberMatched/numberReturned)."""
        mock_session = MagicMock()

        page1_response = MagicMock()
        page1_response.json.return_value = {
            "catalogs": [{"id": "cat-1"}, {"id": "cat-2"}],
            "numberMatched": 5,
            "numberReturned": 2,
            "links": [],
        }

        page2_response = MagicMock()
        page2_response.json.return_value = {
            "catalogs": [{"id": "cat-3"}, {"id": "cat-4"}],
            "numberMatched": 5,
            "numberReturned": 2,
            "links": [],
        }

        page3_response = MagicMock()
        page3_response.json.return_value = {
            "catalogs": [{"id": "cat-5"}],
            "numberMatched": 5,
            "numberReturned": 1,
            "links": [],
        }

        mock_session.get.side_effect = [page1_response, page2_response, page3_response]

        result = _fetch_all_paginated(
            mock_session, "http://localhost:8080/catalogs", "catalogs"
        )

        assert len(result) == 5
        assert result[0]["id"] == "cat-1"
        assert result[2]["id"] == "cat-3"
        assert result[4]["id"] == "cat-5"
        assert mock_session.get.call_count == 3


class TestCrawlGraph:
    """Tests for the crawl-graph command."""

    def test_crawl_graph_networkx_not_installed(self):
        """Test that crawl-graph fails gracefully when networkx is not installed."""
        runner = CliRunner()

        with patch("sfeos_tools.cli.nx", None):
            result = runner.invoke(cli, ["crawl-graph"])

            assert result.exit_code != 0
            assert "networkx is not installed" in result.output

    def test_crawl_graph_text_output(self):
        """Test crawl-graph with text output format."""
        runner = CliRunner()

        with patch("sfeos_tools.cli.requests.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value = mock_session

            mock_catalogs_response = MagicMock()
            mock_catalogs_response.json.return_value = {
                "catalogs": [
                    {"id": "tenant-a"},
                    {"id": "tenant-b"},
                ],
                "links": [],
            }

            mock_children_response = MagicMock()
            mock_children_response.json.return_value = {"children": [], "links": []}

            mock_collections_response = MagicMock()
            mock_collections_response.json.return_value = {
                "collections": [],
                "links": [],
            }

            mock_session.get.side_effect = [
                mock_catalogs_response,
                mock_children_response,
                mock_collections_response,
                mock_children_response,
                mock_collections_response,
            ]

            result = runner.invoke(
                cli, ["crawl-graph", "--stac-url", "http://localhost:8080"]
            )

            assert result.exit_code == 0
            assert "Crawl complete!" in result.output
            assert "SFEOS Topology" in result.output
            assert "tenant-a" in result.output
            assert "tenant-b" in result.output

    def test_crawl_graph_json_output(self):
        """Test crawl-graph with JSON output format."""
        runner = CliRunner()

        with patch("sfeos_tools.cli.requests.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value = mock_session

            mock_catalogs_response = MagicMock()
            mock_catalogs_response.status_code = 200
            mock_catalogs_response.json.return_value = {
                "catalogs": [
                    {"id": "tenant-a"},
                ]
            }

            mock_children_response = MagicMock()
            mock_children_response.status_code = 200
            mock_children_response.json.return_value = {"children": []}

            mock_collections_response = MagicMock()
            mock_collections_response.status_code = 200
            mock_collections_response.json.return_value = {"collections": []}

            mock_session.get.side_effect = [
                mock_catalogs_response,
                mock_children_response,
                mock_collections_response,
            ]

            result = runner.invoke(
                cli,
                [
                    "crawl-graph",
                    "--stac-url",
                    "http://localhost:8080",
                    "--output",
                    "json",
                ],
            )

            assert result.exit_code == 0
            assert "Crawl complete!" in result.output

            # Extract JSON from output (it comes after the "Crawl complete!" message)
            output_lines = result.output.split("\n")
            json_start = None
            for i, line in enumerate(output_lines):
                if line.strip().startswith("{"):
                    json_start = i
                    break

            assert json_start is not None, "No JSON found in output"
            json_str = "\n".join(output_lines[json_start:])
            json_output = json.loads(json_str)
            assert "directed" in json_output or "nodes" in json_output

    def test_crawl_graph_with_hierarchy(self):
        """Test crawl-graph with nested catalog hierarchy."""
        runner = CliRunner()

        with patch("sfeos_tools.cli.requests.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value = mock_session

            catalogs_response = MagicMock()
            catalogs_response.status_code = 200
            catalogs_response.json.return_value = {
                "catalogs": [
                    {"id": "tenant-a"},
                ]
            }

            children_response = MagicMock()
            children_response.status_code = 200
            children_response.json.return_value = {
                "children": [
                    {"id": "collection-1", "type": "Collection"},
                ]
            }

            collections_response = MagicMock()
            collections_response.status_code = 200
            collections_response.json.return_value = {"collections": []}

            mock_session.get.side_effect = [
                catalogs_response,
                children_response,
                collections_response,
            ]

            result = runner.invoke(
                cli, ["crawl-graph", "--stac-url", "http://localhost:8080"]
            )

            assert result.exit_code == 0
            assert "Discovered 2 entities" in result.output
            assert "tenant-a" in result.output
            assert "collection-1" in result.output

    def test_crawl_graph_detects_true_roots(self):
        """Test that crawl-graph correctly identifies true root catalogs using in_degree."""
        runner = CliRunner()

        with patch("sfeos_tools.cli.requests.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value = mock_session

            catalogs_response = MagicMock()
            catalogs_response.status_code = 200
            catalogs_response.json.return_value = {
                "catalogs": [
                    {"id": "root-catalog"},
                    {"id": "child-catalog"},
                    {"id": "grandchild-catalog"},
                ]
            }

            def response_factory(url):
                resp = MagicMock()
                resp.status_code = 200
                if "/children" in url:
                    if "root-catalog" in url:
                        resp.json.return_value = {
                            "children": [{"id": "child-catalog", "type": "Catalog"}]
                        }
                    elif "child-catalog" in url:
                        resp.json.return_value = {
                            "children": [
                                {"id": "grandchild-catalog", "type": "Catalog"}
                            ]
                        }
                    else:
                        resp.json.return_value = {"children": []}
                else:  # /collections
                    resp.json.return_value = {"collections": []}
                return resp

            mock_session.get.side_effect = [
                catalogs_response,
                response_factory("root-catalog/children"),
                response_factory("root-catalog/collections"),
                response_factory("child-catalog/children"),
                response_factory("child-catalog/collections"),
                response_factory("grandchild-catalog/children"),
                response_factory("grandchild-catalog/collections"),
            ]

            result = runner.invoke(
                cli, ["crawl-graph", "--stac-url", "http://localhost:8080"]
            )

            assert result.exit_code == 0
            assert "Discovered 3 entities" in result.output
            assert "root-catalog" in result.output
            assert "child-catalog" in result.output
            assert "grandchild-catalog" in result.output

    def test_crawl_graph_handles_poly_hierarchy(self):
        """Test that crawl-graph safely handles poly-hierarchies (nodes with multiple parents)."""
        runner = CliRunner()

        with patch("sfeos_tools.cli.requests.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value = mock_session

            catalogs_response = MagicMock()
            catalogs_response.json.return_value = {
                "catalogs": [
                    {"id": "parent-a"},
                    {"id": "parent-b"},
                    {"id": "shared-child"},
                ],
                "links": [],
            }

            def response_factory(url):
                resp = MagicMock()
                resp.json.return_value = {
                    "children": [],
                    "collections": [],
                    "links": [],
                }
                if "/children" in url:
                    if "parent-a" in url:
                        resp.json.return_value = {
                            "children": [{"id": "shared-child", "type": "Collection"}],
                            "links": [],
                        }
                    elif "parent-b" in url:
                        resp.json.return_value = {
                            "children": [{"id": "shared-child", "type": "Collection"}],
                            "links": [],
                        }
                    else:
                        resp.json.return_value = {"children": [], "links": []}
                else:  # /collections
                    resp.json.return_value = {"collections": [], "links": []}
                return resp

            mock_session.get.side_effect = [
                catalogs_response,
                response_factory("parent-a/children"),
                response_factory("parent-a/collections"),
                response_factory("parent-b/children"),
                response_factory("parent-b/collections"),
                response_factory("shared-child/children"),
                response_factory("shared-child/collections"),
            ]

            result = runner.invoke(
                cli, ["crawl-graph", "--stac-url", "http://localhost:8080"]
            )

            assert result.exit_code == 0
            assert "Discovered 3 entities" in result.output
            assert "Poly-Linked" in result.output


class TestVisualizeGraph:
    """Tests for the visualize-graph command."""

    def test_visualize_graph_networkx_not_installed(self):
        """Test that visualize-graph fails gracefully when networkx is not installed."""
        runner = CliRunner()

        with patch("sfeos_tools.cli.nx", None):
            result = runner.invoke(cli, ["visualize-graph"])

            assert result.exit_code == 1
            assert "networkx is not installed" in result.output

    def test_visualize_graph_pyvis_not_installed(self):
        """Test that visualize-graph fails gracefully when pyvis is not installed."""
        runner = CliRunner()

        with patch("sfeos_tools.cli.Network", None):
            result = runner.invoke(cli, ["visualize-graph"])

            assert result.exit_code == 1
            assert "pyvis is not installed" in result.output

    @patch("sfeos_tools.cli.webbrowser.open")
    @patch("sfeos_tools.cli.requests.Session")
    def test_visualize_graph_creates_html_file(self, mock_session_class, mock_browser):
        """Test that visualize-graph creates an HTML file and opens it."""
        runner = CliRunner()
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        catalogs_response = MagicMock()
        catalogs_response.json.return_value = {
            "catalogs": [
                {"id": "cat-1", "title": "Catalog 1"},
            ],
            "links": [],
        }

        children_response = MagicMock()
        children_response.json.return_value = {"children": [], "links": []}

        collections_response = MagicMock()
        collections_response.json.return_value = {"collections": [], "links": []}

        mock_session.get.side_effect = [
            catalogs_response,
            children_response,
            collections_response,
        ]

        result = runner.invoke(
            cli, ["visualize-graph", "--stac-url", "http://localhost:8080"]
        )

        assert result.exit_code == 0
        assert "Crawling" in result.output
        assert "Rendering the visualization" in result.output
        assert "Dashboard opened" in result.output
        mock_browser.assert_called_once()

    @patch("sfeos_tools.cli.webbrowser.open")
    @patch("sfeos_tools.cli.requests.Session")
    def test_visualize_graph_detects_poly_hierarchy(
        self, mock_session_class, mock_browser
    ):
        """Test that visualize-graph highlights poly-hierarchical nodes."""
        runner = CliRunner()
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        catalogs_response = MagicMock()
        catalogs_response.json.return_value = {
            "catalogs": [
                {"id": "parent-a", "title": "Parent A"},
                {"id": "parent-b", "title": "Parent B"},
                {"id": "shared-child", "title": "Shared Child"},
            ],
            "links": [],
        }

        def response_factory(url):
            resp = MagicMock()
            if "/children" in url:
                resp.json.return_value = {"children": [], "links": []}
                if "parent-a" in url:
                    resp.json.return_value = {
                        "children": [
                            {
                                "id": "shared-child",
                                "title": "Shared Child",
                                "type": "Catalog",
                            }
                        ],
                        "links": [],
                    }
                elif "parent-b" in url:
                    resp.json.return_value = {
                        "children": [
                            {
                                "id": "shared-child",
                                "title": "Shared Child",
                                "type": "Catalog",
                            }
                        ],
                        "links": [],
                    }
            elif "/collections" in url:
                resp.json.return_value = {"collections": [], "links": []}
            return resp

        mock_session.get.side_effect = [
            catalogs_response,
            response_factory("parent-a/children"),
            response_factory("parent-a/collections"),
            response_factory("parent-b/children"),
            response_factory("parent-b/collections"),
            response_factory("shared-child/children"),
            response_factory("shared-child/collections"),
        ]

        result = runner.invoke(
            cli, ["visualize-graph", "--stac-url", "http://localhost:8080"]
        )

        assert result.exit_code == 0
        assert "Dashboard opened" in result.output
        mock_browser.assert_called_once()
