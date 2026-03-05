"""Tests for catalog_ingestion module."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from sfeos_tools.catalog_ingestion import ingest_from_xml, slugify
from sfeos_tools.cli import cli


class TestSlugify:
    """Tests for slugify function."""

    def test_slugify_basic(self):
        """Test basic slugification of text."""
        assert slugify("Atmospheric Temperature") == "atmospheric-temperature"

    def test_slugify_lowercase(self):
        """Test that output is lowercase."""
        assert slugify("UPPERCASE TEXT") == "uppercase-text"

    def test_slugify_special_chars(self):
        """Test removal of special characters."""
        assert slugify("Road Networks!") == "road-networks"

    def test_slugify_multiple_spaces(self):
        """Test handling of multiple spaces."""
        assert slugify("Urban  Infrastructure") == "urban-infrastructure"

    def test_slugify_leading_trailing_spaces(self):
        """Test stripping of leading/trailing spaces."""
        assert slugify("  River Systems  ") == "river-systems"


class TestIngestFromXml:
    """Tests for ingest_from_xml function."""

    @pytest.fixture
    def test_xml_file(self):
        """Return path to test RDF/XML file."""
        return Path(__file__).parent / "skos-test-topics.rdf"

    def test_ingest_from_xml_file_not_found(self):
        """Test that FileNotFoundError is raised for non-existent file."""
        with pytest.raises(FileNotFoundError):
            ingest_from_xml("/nonexistent/file.rdf", "http://localhost:8080")

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_from_xml_creates_catalogs(self, mock_post, test_xml_file):
        """Test that catalogs are created from SKOS concepts."""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        ingest_from_xml(str(test_xml_file), "http://localhost:8080")

        # Should create catalogs for each concept
        assert mock_post.call_count > 0

        # Check that POST calls were made to /catalogs endpoint (all calls now go to /catalogs)
        catalog_calls = [
            call
            for call in mock_post.call_args_list
            if call[0][0].endswith("/catalogs")
        ]
        assert len(catalog_calls) > 0

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_from_xml_establishes_hierarchy(self, mock_post, test_xml_file):
        """Test that parent-child relationships are established through sub-catalog creation."""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        ingest_from_xml(str(test_xml_file), "http://localhost:8080")

        # Check that sub-catalogs were created under parent catalogs
        # Look for calls to /catalogs/{parent_id}/catalogs endpoints
        subcatalog_calls = [
            call
            for call in mock_post.call_args_list
            if "/catalogs" in call[0][0] and call[0][0].endswith("/catalogs")
            and not call[0][0].endswith("/catalogs") or "/catalogs/" in call[0][0]
        ]
        
        # Filter more precisely: calls that have /catalogs/ in the middle (parent_id)
        subcatalog_calls = [
            call
            for call in mock_post.call_args_list
            if call[0][0].count("/catalogs") > 1  # More than one /catalogs in URL
        ]
        
        assert len(subcatalog_calls) > 0

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_from_xml_handles_409_conflict(self, mock_post, test_xml_file):
        """Test that 409 Conflict responses are handled gracefully."""
        mock_response = Mock()
        mock_response.status_code = 409
        mock_post.return_value = mock_response

        # Should not raise an exception
        ingest_from_xml(str(test_xml_file), "http://localhost:8080")

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_from_xml_catalog_payload_structure(self, mock_post, test_xml_file):
        """Test that catalog payloads have correct structure."""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        ingest_from_xml(str(test_xml_file), "http://localhost:8080")

        # Get the first catalog creation call
        catalog_calls = [
            call
            for call in mock_post.call_args_list
            if call[0][0].endswith("/catalogs")
        ]
        assert len(catalog_calls) > 0

        first_call = catalog_calls[0]
        payload = first_call[1]["json"]

        # Verify required fields
        assert "type" in payload
        assert payload["type"] == "Catalog"
        assert "id" in payload
        assert "title" in payload
        assert "description" in payload
        assert "stac_version" in payload
        assert payload["stac_version"] == "1.0.0"
        assert "links" in payload

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_from_xml_preserves_metadata(self, mock_post, test_xml_file):
        """Test that definitions and modification dates are preserved."""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        ingest_from_xml(str(test_xml_file), "http://localhost:8080")

        # Find a call with a definition
        catalog_calls = [
            call
            for call in mock_post.call_args_list
            if call[0][0].endswith("/catalogs")
        ]

        # At least one catalog should have a definition or description
        has_description = False
        for call in catalog_calls:
            payload = call[1]["json"]
            if payload.get("description"):
                has_description = True
                break

        assert has_description

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_from_xml_creates_semantic_links(self, mock_post, test_xml_file):
        """Test that semantic links (exactMatch, closeMatch, etc.) are created."""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        ingest_from_xml(str(test_xml_file), "http://localhost:8080")

        # Find calls with semantic links
        catalog_calls = [
            call
            for call in mock_post.call_args_list
            if call[0][0].endswith("/catalogs")
        ]

        # At least one catalog should have related links
        has_related_links = False
        for call in catalog_calls:
            payload = call[1]["json"]
            if payload.get("links"):
                for link in payload["links"]:
                    if link.get("rel") == "related":
                        has_related_links = True
                        break

        assert has_related_links


class TestIngestCatalogCLI:
    """Tests for ingest-catalog CLI command."""

    def test_ingest_catalog_command_requires_xml_file(self):
        """Test that --xml-file option is required."""
        runner = CliRunner()
        result = runner.invoke(cli, ["ingest-catalog"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "Error" in result.output

    def test_ingest_catalog_command_file_not_found(self):
        """Test error handling for non-existent file."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["ingest-catalog", "--xml-file", "/nonexistent/file.rdf"]
        )
        assert result.exit_code != 0

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_catalog_command_success(self, mock_post):
        """Test successful execution of ingest-catalog command."""
        runner = CliRunner()
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        test_xml_file = Path(__file__).parent / "skos-test-topics.rdf"

        result = runner.invoke(
            cli, ["ingest-catalog", "--xml-file", str(test_xml_file)]
        )

        assert result.exit_code == 0
        assert "completed successfully" in result.output

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_catalog_command_custom_stac_url(self, mock_post):
        """Test ingest-catalog command with custom STAC URL."""
        runner = CliRunner()
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        test_xml_file = Path(__file__).parent / "skos-test-topics.rdf"
        custom_url = "http://custom-stac-api.example.com"

        result = runner.invoke(
            cli,
            [
                "ingest-catalog",
                "--xml-file",
                str(test_xml_file),
                "--stac-url",
                custom_url,
            ],
        )

        assert result.exit_code == 0

        # Verify that the custom URL was used
        for call in mock_post.call_args_list:
            url = call[0][0]
            assert custom_url in url

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_catalog_command_connection_error(self, mock_post):
        """Test error handling for connection failures."""
        runner = CliRunner()
        mock_post.side_effect = ConnectionError("Connection refused")

        test_xml_file = Path(__file__).parent / "skos-test-topics.rdf"

        result = runner.invoke(
            cli, ["ingest-catalog", "--xml-file", str(test_xml_file)]
        )

        assert result.exit_code != 0
        assert "failed" in result.output.lower()

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_catalog_command_with_auth(self, mock_post):
        """Test ingest-catalog command with username and password."""
        runner = CliRunner()
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        test_xml_file = Path(__file__).parent / "skos-test-topics.rdf"

        result = runner.invoke(
            cli,
            [
                "ingest-catalog",
                "--xml-file",
                str(test_xml_file),
                "--user",
                "testuser",
                "--password",
                "testpass",
            ],
        )

        assert result.exit_code == 0

        # Verify that auth was passed to requests.post
        for call in mock_post.call_args_list:
            assert call[1]["auth"] == ("testuser", "testpass")

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_catalog_command_with_ssl_disabled(self, mock_post):
        """Test ingest-catalog command with SSL verification disabled."""
        runner = CliRunner()
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        test_xml_file = Path(__file__).parent / "skos-test-topics.rdf"

        result = runner.invoke(
            cli,
            [
                "ingest-catalog",
                "--xml-file",
                str(test_xml_file),
                "--no-ssl",
            ],
        )

        assert result.exit_code == 0

        # Verify that verify=False was passed to requests.post
        for call in mock_post.call_args_list:
            assert call[1]["verify"] is False

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_catalog_command_with_auth_and_ssl(self, mock_post):
        """Test ingest-catalog command with both auth and SSL options."""
        runner = CliRunner()
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        test_xml_file = Path(__file__).parent / "skos-test-topics.rdf"

        result = runner.invoke(
            cli,
            [
                "ingest-catalog",
                "--xml-file",
                str(test_xml_file),
                "--user",
                "admin",
                "--password",
                "secret",
                "--no-ssl",
            ],
        )

        assert result.exit_code == 0

        # Verify that both auth and verify were passed correctly
        for call in mock_post.call_args_list:
            assert call[1]["auth"] == ("admin", "secret")
            assert call[1]["verify"] is False


class TestIngestWithTestData:
    """Integration tests using actual test RDF files."""

    @pytest.fixture
    def test_xml_file_1(self):
        """Return path to first test RDF/XML file."""
        return Path(__file__).parent / "skos-test-topics.rdf"

    @pytest.fixture
    def test_xml_file_2(self):
        """Return path to second test RDF/XML file."""
        return Path(__file__).parent / "skos-test-topics-2.rdf"

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_observation_domains(self, mock_post, test_xml_file_1):
        """Test ingestion of observation domains thesaurus."""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        ingest_from_xml(str(test_xml_file_1), "http://localhost:8080")

        # Verify that expected concepts were created
        # Filter for calls to /catalogs endpoint (not /catalogs/{id}/catalogs)
        catalog_calls = [
            call
            for call in mock_post.call_args_list
            if call[0][0].endswith("/catalogs")
        ]

        created_ids = []
        for call in catalog_calls:
            payload = call[1]["json"]
            created_ids.append(payload["id"])

        # Check for expected concepts from skos-test-topics.rdf
        assert "observation-domain" in created_ids
        assert "urban-infrastructure" in created_ids
        assert "hydrological-features" in created_ids
        assert "road-networks" in created_ids
        assert "river-systems" in created_ids

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_culinary_topics(self, mock_post, test_xml_file_2):
        """Test ingestion of culinary topics thesaurus."""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        ingest_from_xml(str(test_xml_file_2), "http://localhost:8080")

        # Verify that expected concepts were created
        # Filter for calls to /catalogs endpoint (not /catalogs/{id}/catalogs)
        catalog_calls = [
            call
            for call in mock_post.call_args_list
            if call[0][0].endswith("/catalogs")
        ]

        created_ids = []
        for call in catalog_calls:
            payload = call[1]["json"]
            created_ids.append(payload["id"])

        # Check for expected concepts from skos-test-topics-2.rdf
        assert "food-topic" in created_ids
        assert "desserts" in created_ids
        assert "beverages" in created_ids
        assert "cakes" in created_ids
        assert "coffee" in created_ids
