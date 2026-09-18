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


def test_config_missing_auth_token_allowed():
    """No credential is required at construction time - stdio/sse enforce it via the CLI,
    and streamable-http intentionally allows it (per-connection auth mode)."""
    config = AirflowConfig(base_url="http://localhost:8080", auth_token=None)

    assert config.auth_token is None
    assert config.username is None
    assert config.password is None


def test_config_empty_auth_token_allowed():
    """An empty auth_token is treated the same as no auth_token - not an error here."""
    config = AirflowConfig(base_url="http://localhost:8080", auth_token="")

    assert config.auth_token == ""


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


def test_config_no_credentials_allowed():
    """Neither auth_token nor username/password is required here - the CLI decides whether
    that's acceptable based on transport (stdio/sse require it, streamable-http doesn't)."""
    config = AirflowConfig(base_url="http://localhost:8080")

    assert config.auth_token is None
    assert config.username is None
    assert config.password is None


def test_config_partial_credentials_stored_as_is():
    """A username without a password (or vice versa) is stored as-is; the CLI/runtime layer
    is responsible for deciding that this doesn't satisfy the credential requirement."""
    config = AirflowConfig(base_url="http://localhost:8080", username="airflow")

    assert config.username == "airflow"
    assert config.password is None
