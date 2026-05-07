"""Tests for language filtering feature in catalog_ingestion module."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import SKOS

from sfeos_tools.catalog_ingestion import get_lang_literal, ingest_from_xml
from sfeos_tools.cli import cli


class TestGetLangLiteral:
    """Tests for get_lang_literal function."""

    def test_get_lang_literal_explicit_english(self):
        """Test explicitly requesting English language."""
        g = Graph()
        subject = URIRef("http://example.com/concept")

        # Add English, French, and un-tagged labels
        g.add((subject, SKOS.prefLabel, Literal("Apple", lang="en")))
        g.add((subject, SKOS.prefLabel, Literal("Pomme", lang="fr")))
        g.add((subject, SKOS.altLabel, Literal("Generic Fruit")))

        result = get_lang_literal(g, subject, SKOS.prefLabel, "en")
        assert result == "Apple"

    def test_get_lang_literal_explicit_french(self):
        """Test explicitly requesting French language."""
        g = Graph()
        subject = URIRef("http://example.com/concept")

        g.add((subject, SKOS.prefLabel, Literal("Apple", lang="en")))
        g.add((subject, SKOS.prefLabel, Literal("Pomme", lang="fr")))

        result = get_lang_literal(g, subject, SKOS.prefLabel, "fr")
        assert result == "Pomme"

    def test_get_lang_literal_missing_language(self):
        """Test requesting a language that doesn't exist."""
        g = Graph()
        subject = URIRef("http://example.com/concept")

        g.add((subject, SKOS.prefLabel, Literal("Apple", lang="en")))
        g.add((subject, SKOS.prefLabel, Literal("Pomme", lang="fr")))

        result = get_lang_literal(g, subject, SKOS.prefLabel, "es")
        assert result is None

    def test_get_lang_literal_fallback_when_lang_none(self):
        """Test fallback logic when lang=None (should return first available)."""
        g = Graph()
        subject = URIRef("http://example.com/concept")

        g.add((subject, SKOS.prefLabel, Literal("Apple", lang="en")))
        g.add((subject, SKOS.prefLabel, Literal("Pomme", lang="fr")))

        result = get_lang_literal(g, subject, SKOS.prefLabel, None)
        # Should return one of the available values, not None
        assert result in ["Apple", "Pomme"]

    def test_get_lang_literal_untagged_fallback(self):
        """Test fallback to untagged literal when lang=None."""
        g = Graph()
        subject = URIRef("http://example.com/concept")

        # Add only an untagged literal
        g.add((subject, SKOS.prefLabel, Literal("Generic Fruit")))

        result = get_lang_literal(g, subject, SKOS.prefLabel, None)
        assert result == "Generic Fruit"

    def test_get_lang_literal_missing_predicate(self):
        """Test when predicate doesn't exist."""
        g = Graph()
        subject = URIRef("http://example.com/concept")

        g.add((subject, SKOS.prefLabel, Literal("Apple", lang="en")))

        result = get_lang_literal(g, subject, SKOS.definition, None)
        assert result is None


class TestIngestWithLanguageFilter:
    """Tests for language filtering in ingest_from_xml."""

    @pytest.fixture
    def test_xml_file(self):
        """Return path to test RDF/XML file."""
        return Path(__file__).parent / "skos-test-topics.rdf"

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_without_lang_flag_uses_fallback(self, mock_post, test_xml_file):
        """Test that ingestion without --lang flag uses fallback (first available)."""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        # Ingest without specifying a language
        ingest_from_xml(str(test_xml_file), "http://localhost:8080", lang=None)

        # Verify that concepts were created with proper titles (not "Unnamed Concept")
        catalog_calls = [
            call
            for call in mock_post.call_args_list
            if call[0][0].endswith("/catalogs")
        ]

        created_catalogs = []
        for call in catalog_calls:
            payload = call[1]["json"]
            created_catalogs.append(payload)

        # Verify we have actual titles, not "Unnamed Concept"
        titles = [cat.get("title") for cat in created_catalogs]
        assert len(titles) > 0
        # None of the titles should be "Unnamed Concept" (the bug we fixed)
        assert "Unnamed Concept" not in titles

    @patch("sfeos_tools.catalog_ingestion.requests.post")
    def test_ingest_preserves_exact_match_links(self, mock_post, test_xml_file):
        """Test that SKOS Exact Match links are correctly set for each catalog."""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        ingest_from_xml(str(test_xml_file), "http://localhost:8080", lang=None)

        # Get all payloads sent to the STAC API
        all_payloads = [call[1]["json"] for call in mock_post.call_args_list]

        for payload in all_payloads:
            # Extract the SKOS Exact Match link
            exact_match_links = [
                link
                for link in payload.get("links", [])
                if link.get("title") == "SKOS Exact Match"
                and link.get("rel") == "alternate"
            ]

            # Verify exactly one Exact Match link exists per catalog
            assert (
                len(exact_match_links) == 1
            ), f"Missing exact match link for {payload['id']}"

            # Verify the link points to a valid URI (not empty or malformed)
            href = exact_match_links[0]["href"]
            assert href.startswith("http"), f"Invalid href for {payload['id']}: {href}"


class TestIngestCatalogCLIWithLang:
    """Tests for ingest-catalog CLI with language flag."""

    @patch("sfeos_tools.cli.ingest_from_xml")
    def test_cli_ingest_with_lang_flag(self, mock_ingest):
        """Test that the --lang flag correctly passes to the ingestion module."""
        from click.testing import CliRunner

        runner = CliRunner()

        with runner.isolated_filesystem():
            # Create a dummy test file
            with open("test.rdf", "w") as f:
                f.write("<rdf></rdf>")

            result = runner.invoke(
                cli,
                [
                    "ingest-catalog",
                    "--xml-file",
                    "test.rdf",
                    "--stac-url",
                    "http://api",
                    "--lang",
                    "fr",
                ],
            )

            # Verify the command succeeded
            assert result.exit_code == 0

            # Verify the language was passed to ingest_from_xml
            mock_ingest.assert_called_once()
            call_kwargs = mock_ingest.call_args[1]
            assert call_kwargs.get("lang") == "fr"

    @patch("sfeos_tools.cli.ingest_from_xml")
    def test_cli_ingest_without_lang_flag_defaults_to_none(self, mock_ingest):
        """Test that omitting --lang flag defaults to None (fallback behavior)."""
        from click.testing import CliRunner

        runner = CliRunner()

        with runner.isolated_filesystem():
            # Create a dummy test file
            with open("test.rdf", "w") as f:
                f.write("<rdf></rdf>")

            result = runner.invoke(
                cli,
                [
                    "ingest-catalog",
                    "--xml-file",
                    "test.rdf",
                    "--stac-url",
                    "http://api",
                ],
            )

            # Verify the command succeeded
            assert result.exit_code == 0

            # Verify lang defaults to None
            mock_ingest.assert_called_once()
            call_kwargs = mock_ingest.call_args[1]
            assert call_kwargs.get("lang") is None
