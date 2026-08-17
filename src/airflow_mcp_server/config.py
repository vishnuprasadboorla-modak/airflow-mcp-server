class AirflowConfig:
    """Centralized configuration for Airflow MCP server."""

    def __init__(
        self,
        base_url: str | None = None,
        auth_token: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        """Initialize configuration with provided values.

        Args:
            base_url: Airflow API base URL
            auth_token: Static authentication token (JWT). Never refreshed once the
                server starts - prefer username/password for long-running deployments.
            username: Airflow username. Combined with password, lets the server fetch
                and automatically refresh its own JWT for the life of the process.
            password: Airflow password.

        Raises:
            ValueError: If required configuration is missing
        """
        self.base_url = base_url
        if not self.base_url:
            raise ValueError("Missing required configuration: base_url")

        self.auth_token = auth_token
        self.username = username
        self.password = password

        if not self.auth_token and not (self.username and self.password):
            raise ValueError("Missing required configuration: auth_token (JWT), or both username and password")
