"""Helpers for constructing and caching Databricks SDK configuration objects."""

import functools
import json
import logging
import os
import subprocess
import threading
from builtins import Exception, ValueError, hasattr
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from databricks.sdk.core import Config
from databricks.sdk.credentials_provider import OAuthCredentialsProvider
from pyspark.sql import SparkSession
from reggie_core import inputs

from reggie_tools import catalogs, clients, runtimes

_config_default_lock = threading.Lock()
_config_default: Optional[Config] = None


class ConfigValueSource(Enum):
    """Enumerates supported config sources in order of discovery precedence."""

    WIDGETS = 1
    SPARK_CONF = 2
    OS_ENVIRON = 3
    SECRETS = 4

    @classmethod
    def without(cls, *excluded):
        """Return members excluding any provided in ``excluded`` while preserving order."""
        return [member for member in cls if member not in excluded]


def get(profile: Optional[str] = None) -> Config:
    """Return a cached or freshly created Databricks ``Config`` for the given profile."""
    global _config_default
    if not profile:
        profile = None
    if not profile:
        if _config_default:
            return _config_default
        elif not _config_default_lock.locked():
            with _config_default_lock:
                return get(profile)

    def _default_profile() -> Optional[str]:
        if not _cli_version():
            return None
        auth_profiles = _cli_auth_profiles()
        profiles = auth_profiles.get("profiles", [])
        if profiles:
            if False and len(profiles) == 1:
                return profiles[0].get("name")
            else:
                for profile in profiles:
                    profile_name = profile.get("name")
                    if "DEFAULT" == profile_name:
                        return profile_name
                return inputs.select_choice(
                    "Select a profile", [p["name"] for p in profiles]
                )
        return None

    def _config(profile, auth_login=True) -> Config:
        try:
            cfg = Config(profile=profile)
            if not cfg.cluster_id:
                cfg.serverless_compute_id = "auto"
            return cfg
        except Exception as e:
            if not profile:
                profile = _default_profile()
                if profile:
                    return _config(profile, auth_login)
            if auth_login and profile:
                _cli_auth_login(profile)
                return _config(profile, False)
            raise e

    cfg = _config(profile)
    logging.getlogger().debug("config created - profile:%s config:%s", profile, cfg)
    if not profile:
        _config_default = cfg
    return cfg


def token(config: Config = None) -> str:
    """Extract an API token from the provided or default configuration."""
    if not config:
        config = globals().get("config")()
    if isinstance(config._header_factory, OAuthCredentialsProvider):
        return config.oauth_token().access_token
    else:
        if config.token:
            return config.token
        else:
            raise ValueError(f"config token not found - config:{config}")


def config_value(
    name: str,
    spark: SparkSession = None,
    config_value_sources: List[ConfigValueSource] = None,
) -> Any:
    """Fetch a configuration value by checking the configured sources in order."""
    if not name:
        raise ValueError("name cannot be empty")
    if not config_value_sources:
        config_value_sources = tuple(ConfigValueSource)

    dbutils = (
        runtimes.dbutils(spark)
        if (
            ConfigValueSource.WIDGETS in config_value_sources
            or ConfigValueSource.SECRETS in config_value_sources
        )
        else None
    )
    for config_value_source in config_value_sources:
        loader = None
        if config_value_source is ConfigValueSource.WIDGETS:
            loader = (
                dbutils.widgets.get if dbutils and hasattr(dbutils, "widgets") else None
            )
        elif config_value_source is ConfigValueSource.SPARK_CONF:
            loader = (spark or clients.spark()).conf.get
        elif config_value_source is ConfigValueSource.OS_ENVIRON:
            loader = os.environ.get
        elif config_value_source is ConfigValueSource.SECRETS:
            if dbutils and hasattr(dbutils, "secrets"):
                catalog_schema = catalogs.catalog_schema(spark)
                if catalog_schema:
                    loader = lambda n: dbutils.secrets.get(
                        scope=str(catalog_schema), key=n
                    )
        else:
            raise ValueError(
                f"unknown ConfigValueSource - config_value_source:{config_value_source}"
            )
        if loader:
            try:
                value = loader(name)
                if value:
                    return value
            except Exception:
                pass
    return None


def _cli_run(
    *popenargs,
    profile=None,
    stdout=subprocess.PIPE,
    stderr=None,
    check=False,
    timeout=None,
) -> Tuple[Dict[str, Any], subprocess.CompletedProcess]:
    """Execute the Databricks CLI and return the parsed JSON payload and process."""
    version = runtimes.version()
    if version:
        raise ValueError("cli unsupported in databricks runtime - version:{version}")
    args = ["databricks", "--output", "json"]
    args.extend(popenargs)
    if profile:
        args.extend(["--profile", profile])
    logging.getlogger().debug(
        "cli run - args:%s stdout:%s stderr:%s check:%s", args, stdout, stderr, check
    )
    completed_process = subprocess.run(
        args, stdout=stdout, stderr=stderr, check=check, timeout=timeout
    )
    return json.loads(
        completed_process.stdout
    ) if completed_process.stdout else None, completed_process


@functools.cache
def _cli_version() -> Dict[str, Any]:
    """Return cached CLI version metadata, if available outside a runtime cluster."""
    version = (
        None
        if runtimes.version()
        else _cli_run("version", check=False, stderr=subprocess.DEVNULL)[0]
    )
    logging.getlogger().debug(f"version:{version}")
    return version


@functools.cache
def _cli_auth_profiles() -> Optional[Dict[str, Any]]:
    """Return cached authentication profiles discovered via the Databricks CLI."""
    auth_profiles = _cli_run("auth", "profiles")[0]
    logging.getlogger().debug(f"auth profiles:{auth_profiles}")
    return auth_profiles


def _cli_auth_login(profile: str):
    """Execute ``databricks auth login`` for the provided CLI profile."""
    _cli_run("auth", "login", profile=profile)


if __name__ == "__main__":
    print(config_value("PATH"))
    clients.spark().sql("select 'hello there' as msg").show()
