"""Tests for AirflowConfig."""

import pytest

from airflow_mcp_server.config import AirflowConfig


def test_config_valid():
    """Test valid configuration."""
    config = AirflowConfig(base_url="http://localhost:8080", auth_token="test-token")

    assert config.base_url == "http://localhost:8080"
    assert config.auth_token == "test-token"


def test_config_missing_base_url():
    """Test configuration with missing base_url."""
    with pytest.raises(ValueError, match="Missing required configuration: base_url"):
        AirflowConfig(base_url=None, auth_token="test-token")


def test_config_empty_base_url():
    """Test configuration with empty base_url."""
    with pytest.raises(ValueError, match="Missing required configuration: base_url"):
        AirflowConfig(base_url="", auth_token="test-token")


def test_config_missing_auth_token():
    """Test configuration with missing auth_token."""
    with pytest.raises(ValueError, match="Missing required configuration: auth_token"):
        AirflowConfig(base_url="http://localhost:8080", auth_token=None)


def test_config_empty_auth_token():
    """Test configuration with empty auth_token."""
    with pytest.raises(ValueError, match="Missing required configuration: auth_token"):
        AirflowConfig(base_url="http://localhost:8080", auth_token="")


def test_config_both_missing():
    """Test configuration with both values missing."""
    with pytest.raises(ValueError, match="Missing required configuration: base_url"):
        AirflowConfig(base_url=None, auth_token=None)


def test_config_valid_with_username_password():
    """Username/password alone should satisfy the auth requirement."""
    config = AirflowConfig(base_url="http://localhost:8080", username="airflow", password="airflow")

    assert config.auth_token is None
    assert config.username == "airflow"
    assert config.password == "airflow"


def test_config_missing_auth_token_and_credentials():
    """Neither auth_token nor a full username/password pair should fail clearly."""
    with pytest.raises(ValueError, match="auth_token .* or both username and password"):
        AirflowConfig(base_url="http://localhost:8080")


def test_config_partial_credentials_not_enough():
    """A username without a password (or vice versa) should not satisfy the requirement."""
    with pytest.raises(ValueError, match="auth_token .* or both username and password"):
        AirflowConfig(base_url="http://localhost:8080", username="airflow")
