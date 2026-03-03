"""Tests for data_loader module."""

import json
from unittest.mock import MagicMock, Mock, patch

import click
import pytest
from click.testing import CliRunner
from httpx import Client, Response

from sfeos_tools.cli import cli
from sfeos_tools.data_loader import (
    load_collection,
    load_data,
    load_items,
    load_items_bulk_insert,
    load_items_one_by_one,
)


class TestLoadData:
    """Tests for load_data function."""

    def test_load_data_success(self, tmp_path):
        """Test loading valid JSON data from a file."""
        test_data = {"id": "test", "type": "Feature"}
        test_file = tmp_path / "test.json"
        test_file.write_text(json.dumps(test_data))

        result = load_data(str(test_file))

        assert result == test_data

    def test_load_data_file_not_found(self):
        """Test loading data from non-existent file raises error."""
        with pytest.raises(click.exceptions.Abort):
            load_data("/nonexistent/path/file.json")


class TestLoadCollection:
    """Tests for load_collection function."""

    def test_load_collection_success(self, tmp_path):
        """Test successfully loading a collection."""
        collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "description": "Test collection",
        }
        collection_file = tmp_path / "collection.json"
        collection_file.write_text(json.dumps(collection_data))

        mock_client = Mock(spec=Client)
        mock_response = Mock(spec=Response)
        mock_response.status_code = 201
        mock_client.post.return_value = mock_response

        load_collection(mock_client, "test-collection", str(tmp_path))

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert call_args[0][0] == "/collections"
        assert call_args[1]["json"]["id"] == "test-collection"

    def test_load_collection_already_exists(self, tmp_path):
        """Test loading a collection that already exists."""
        collection_data = {"type": "Collection"}
        collection_file = tmp_path / "collection.json"
        collection_file.write_text(json.dumps(collection_data))

        mock_client = Mock(spec=Client)
        mock_response = Mock(spec=Response)
        mock_response.status_code = 409
        mock_client.post.return_value = mock_response

        load_collection(mock_client, "existing-collection", str(tmp_path))

        mock_client.post.assert_called_once()

    def test_load_collection_error(self, tmp_path):
        """Test handling error when loading collection."""
        collection_data = {"type": "Collection"}
        collection_file = tmp_path / "collection.json"
        collection_file.write_text(json.dumps(collection_data))

        mock_client = Mock(spec=Client)
        mock_response = Mock(spec=Response)
        mock_response.status_code = 500
        mock_response.text = "Internal server error"
        mock_client.post.return_value = mock_response

        load_collection(mock_client, "test-collection", str(tmp_path))

        mock_client.post.assert_called_once()


class TestLoadItemsOneByOne:
    """Tests for load_items_one_by_one function."""

    def test_load_items_one_by_one_success(self):
        """Test loading items one by one successfully."""
        feature_collection = {
            "type": "FeatureCollection",
            "features": [
                {"id": "item-1", "type": "Feature"},
                {"id": "item-2", "type": "Feature"},
            ],
        }

        mock_client = Mock(spec=Client)
        mock_response = Mock(spec=Response)
        mock_response.status_code = 200
        mock_client.post.return_value = mock_response

        load_items_one_by_one(mock_client, "test-collection", feature_collection)

        assert mock_client.post.call_count == 2
        for call in mock_client.post.call_args_list:
            assert call[0][0] == "/collections/test-collection/items"
            assert call[1]["json"]["collection"] == "test-collection"

    def test_load_items_one_by_one_with_conflicts(self):
        """Test loading items when some already exist."""
        feature_collection = {
            "type": "FeatureCollection",
            "features": [
                {"id": "item-1", "type": "Feature"},
                {"id": "item-2", "type": "Feature"},
            ],
        }

        mock_client = Mock(spec=Client)
        mock_response_success = Mock(spec=Response)
        mock_response_success.status_code = 200
        mock_response_conflict = Mock(spec=Response)
        mock_response_conflict.status_code = 409
        mock_client.post.side_effect = [mock_response_success, mock_response_conflict]

        load_items_one_by_one(mock_client, "test-collection", feature_collection)

        assert mock_client.post.call_count == 2


class TestLoadItemsBulkInsert:
    """Tests for load_items_bulk_insert function."""

    def test_load_items_bulk_insert_success(self):
        """Test bulk loading items successfully."""
        feature_collection = {
            "type": "FeatureCollection",
            "features": [
                {"id": "item-1", "type": "Feature"},
                {"id": "item-2", "type": "Feature"},
            ],
        }

        mock_client = Mock(spec=Client)
        mock_response = Mock(spec=Response)
        mock_response.status_code = 200
        mock_client.post.return_value = mock_response

        load_items_bulk_insert(mock_client, "test-collection", feature_collection)

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert call_args[0][0] == "/collections/test-collection/items"
        # Check that all features have collection set
        for feature in call_args[1]["json"]["features"]:
            assert feature["collection"] == "test-collection"

    def test_load_items_bulk_insert_no_content(self):
        """Test bulk loading with 204 response."""
        feature_collection = {
            "type": "FeatureCollection",
            "features": [{"id": "item-1", "type": "Feature"}],
        }

        mock_client = Mock(spec=Client)
        mock_response = Mock(spec=Response)
        mock_response.status_code = 204
        mock_client.post.return_value = mock_response

        load_items_bulk_insert(mock_client, "test-collection", feature_collection)

        mock_client.post.assert_called_once()

    def test_load_items_bulk_insert_conflict(self):
        """Test bulk loading with conflict response."""
        feature_collection = {
            "type": "FeatureCollection",
            "features": [{"id": "item-1", "type": "Feature"}],
        }

        mock_client = Mock(spec=Client)
        mock_response = Mock(spec=Response)
        mock_response.status_code = 409
        mock_client.post.return_value = mock_response

        load_items_bulk_insert(mock_client, "test-collection", feature_collection)

        mock_client.post.assert_called_once()


class TestLoadItems:
    """Tests for load_items function."""

    def test_load_items_finds_feature_file(self, tmp_path):
        """Test that load_items finds and loads feature collection file."""
        collection_data = {"type": "Collection"}
        collection_file = tmp_path / "collection.json"
        collection_file.write_text(json.dumps(collection_data))

        feature_data = {
            "type": "FeatureCollection",
            "features": [{"id": "item-1", "type": "Feature"}],
        }
        feature_file = tmp_path / "features.json"
        feature_file.write_text(json.dumps(feature_data))

        mock_client = Mock(spec=Client)
        mock_response = Mock(spec=Response)
        mock_response.status_code = 200
        mock_client.post.return_value = mock_response

        load_items(mock_client, "test-collection", False, str(tmp_path))

        # Should be called twice: once for collection, once for item
        assert mock_client.post.call_count == 2

    def test_load_items_no_feature_file_found(self, tmp_path):
        """Test error when no feature collection file is found."""
        collection_data = {"type": "Collection"}
        collection_file = tmp_path / "collection.json"
        collection_file.write_text(json.dumps(collection_data))

        mock_client = Mock(spec=Client)

        with pytest.raises(click.exceptions.Abort):
            load_items(mock_client, "test-collection", False, str(tmp_path))

    def test_load_items_uses_bulk_insert(self, tmp_path):
        """Test that load_items uses bulk insert when flag is set."""
        collection_data = {"type": "Collection"}
        collection_file = tmp_path / "collection.json"
        collection_file.write_text(json.dumps(collection_data))

        feature_data = {
            "type": "FeatureCollection",
            "features": [
                {"id": "item-1", "type": "Feature"},
                {"id": "item-2", "type": "Feature"},
            ],
        }
        feature_file = tmp_path / "features.json"
        feature_file.write_text(json.dumps(feature_data))

        mock_client = Mock(spec=Client)
        mock_response = Mock(spec=Response)
        mock_response.status_code = 200
        mock_client.post.return_value = mock_response

        load_items(mock_client, "test-collection", True, str(tmp_path))

        # Should be called twice: once for collection, once for bulk items
        assert mock_client.post.call_count == 2


class TestCLILoadData:
    """Tests for CLI load-data command."""

    def test_cli_load_data_success(self, tmp_path):
        """Test CLI load-data command with successful execution."""
        collection_data = {"type": "Collection"}
        collection_file = tmp_path / "collection.json"
        collection_file.write_text(json.dumps(collection_data))

        feature_data = {
            "type": "FeatureCollection",
            "features": [{"id": "item-1", "type": "Feature"}],
        }
        feature_file = tmp_path / "features.json"
        feature_file.write_text(json.dumps(feature_data))

        runner = CliRunner()
        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value.__enter__.return_value = mock_client
            mock_response = Mock(spec=Response)
            mock_response.status_code = 200
            mock_client.post.return_value = mock_response

            result = runner.invoke(
                cli,
                [
                    "load-data",
                    "--stac-url",
                    "http://localhost:8080",
                    "--collection-id",
                    "test-collection",
                    "--data-dir",
                    str(tmp_path),
                ],
            )

        assert result.exit_code == 0
        assert "✓ Data loading completed successfully" in result.output

    def test_cli_load_data_with_bulk_flag(self, tmp_path):
        """Test CLI load-data command with bulk insert flag."""
        collection_data = {"type": "Collection"}
        collection_file = tmp_path / "collection.json"
        collection_file.write_text(json.dumps(collection_data))

        feature_data = {
            "type": "FeatureCollection",
            "features": [{"id": "item-1", "type": "Feature"}],
        }
        feature_file = tmp_path / "features.json"
        feature_file.write_text(json.dumps(feature_data))

        runner = CliRunner()
        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value.__enter__.return_value = mock_client
            mock_response = Mock(spec=Response)
            mock_response.status_code = 200
            mock_client.post.return_value = mock_response

            result = runner.invoke(
                cli,
                [
                    "load-data",
                    "--stac-url",
                    "http://localhost:8080",
                    "--collection-id",
                    "test-collection",
                    "--use-bulk",
                    "--data-dir",
                    str(tmp_path),
                ],
            )

        assert result.exit_code == 0
        assert "✓ Data loading completed successfully" in result.output

    def test_cli_load_data_missing_base_url(self):
        """Test CLI load-data command fails without data directory."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["load-data", "--stac-url", "http://localhost:8080"]
        )

        assert result.exit_code != 0
        assert (
            "No feature collection files found" in result.output
            or "failed" in result.output.lower()
        )

    def test_cli_load_data_handles_error(self, tmp_path):
        """Test CLI load-data command handles errors gracefully."""
        collection_data = {"type": "Collection"}
        collection_file = tmp_path / "collection.json"
        collection_file.write_text(json.dumps(collection_data))

        feature_data = {
            "type": "FeatureCollection",
            "features": [{"id": "item-1", "type": "Feature"}],
        }
        feature_file = tmp_path / "features.json"
        feature_file.write_text(json.dumps(feature_data))

        runner = CliRunner()
        with patch("httpx.Client") as mock_client_class:
            mock_client_class.return_value.__enter__.side_effect = Exception(
                "Connection failed"
            )

            result = runner.invoke(
                cli,
                [
                    "load-data",
                    "--stac-url",
                    "http://localhost:8080",
                    "--data-dir",
                    str(tmp_path),
                ],
            )

        assert result.exit_code == 1
        assert "✗ Data loading failed" in result.output
        assert "Connection failed" in result.output
