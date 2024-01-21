import os
from typing import Any, Dict

from triad import ParamDict

_FUGUE_SF_ENV_PREFIX = "FUGUE_SF_"
_FUGUE_SF_CONF_PREFIX = "fugue.sf."


def get_client_init_params(conf: Any) -> Dict[str, Any]:
    _conf = ParamDict(conf)
    return dict(  # noqa: C408
        account=_get_value(_conf, "account"),
        user=_get_value(_conf, "user"),
        password=_get_value(_conf, "password"),
        warehouse=_get_value(_conf, "warehouse"),
        database=_get_value(_conf, "database"),
        schema=_get_value(_conf, "schema"),
    )


def _get_value(conf: ParamDict, name: str) -> Any:
    if _FUGUE_SF_CONF_PREFIX + name in conf:
        return conf.get_or_throw(_FUGUE_SF_CONF_PREFIX + name, str)
    if _FUGUE_SF_ENV_PREFIX + name.upper() in os.environ:
        return os.environ[_FUGUE_SF_ENV_PREFIX + name.upper()]
    return None
