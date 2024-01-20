from typing import Any, Dict

FUGUE_TRINO_CONF_TEMP_SCHEMA = "fugue.trino.temp_schema"
FUGUE_TRINO_CONF_TEMP_SCHEMA_DEFAULT_NAME = "fugue_temp_schema"
FUGUE_TRINO_CONF_CATALOG = "fugue.trino.catalog"
FUGUE_TRINO_CONF_HOST = "fugue.trino.host"
FUGUE_TRINO_CONF_PORT = "fugue.trino.port"
FUGUE_TRINO_CONF_USER = "fugue.trino.user"
FUGUE_TRINO_CONF_PASSWORD = "fugue.trino.password"
FUGUE_TRINO_CONF_AUTH = "fugue.trino.auth"

FUGUE_TRINO_ENV_PASSWORD = "FUGUE_TRINO_PASSWORD"

FUGUE_TRINO_DEFAULT_CONF: Dict[str, Any] = {
    FUGUE_TRINO_CONF_TEMP_SCHEMA: FUGUE_TRINO_CONF_TEMP_SCHEMA_DEFAULT_NAME,
}
