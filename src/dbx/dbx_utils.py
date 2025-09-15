import functools
import json
import os
import subprocess
import threading
from typing import Optional, Tuple, Dict, Any

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.credentials_provider import OAuthCredentialsProvider
from pyspark.sql import SparkSession

from common import log_utils
from common import console_utils


def runtime_version() -> Optional[str]:
    return os.environ.get("DATABRICKS_RUNTIME_VERSION") or None


_config_default_lock = threading.Lock()
_config_default: Optional[Config] = None


def config(profile: Optional[str] = None) -> Config:
    global _config_default
    if not profile:
        profile = None
    if not profile:
        if _config_default:
            return _config_default
        elif not _config_default_lock.locked():
            with _config_default_lock:
                return config(profile)

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
                return console_utils.select_choice("Select a profile", [p["name"] for p in profiles])
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
    log_utils.logger().info("config created - profile:%s config:%s", profile, cfg)
    if not profile:
        _config_default = cfg
    return cfg


def token(config: Config = None) -> str:
    if not config:
        config = globals().get("config")()
    if isinstance(config._header_factory, OAuthCredentialsProvider):
        return config.oauth_token().access_token
    else:
        if config.token:
            return config.token
        else:
            raise ValueError(f"config token not found - config:{config}")


def workspace_client(config: Config = None) -> WorkspaceClient:
    if not config:
        config = globals().get("config")()
    return WorkspaceClient(config=config)


def spark(config: Config = None) -> SparkSession:
    if not config:
        config = globals().get("config")()
    if runtime_version():
        return SparkSession.builder.getOrCreate()
    spark_builder = DatabricksSession.builder.sdkConfig(config)
    return spark_builder.getOrCreate()


# noinspection PyUnresolvedReferences
@functools.lru_cache(maxsize=1)
def context() -> Optional[Any]:
    if runtime_version():
        from dbruntime.databricks_repl_context import get_context
        return get_context()
    return None


def _cli_run(*popenargs,
             profile=None, stdout=subprocess.PIPE, stderr=None, check=False) -> Tuple[
    Dict[str, Any], subprocess.CompletedProcess]:
    version = runtime_version()
    if version:
        raise ValueError("cli unsupported in databricks runtime - version:{version}")
    args = ["databricks", "--output", "json"]
    args.extend(popenargs)
    if profile:
        args.extend(["--profile", profile])
    log_utils.logger().debug("cli run - args:%s stdout:%s stderr:%s check:%s", args, stdout, stderr, check)
    completed_process = subprocess.run(args, stdout=stdout, stderr=stderr, check=check)
    return json.loads(completed_process.stdout) if completed_process.stdout else None, completed_process


@functools.lru_cache(maxsize=1)
def _cli_version() -> Dict[str, Any]:
    version = None if runtime_version() else _cli_run("version", check=False, stderr=subprocess.DEVNULL)[0]
    log_utils.logger().debug(f"version:{version}")
    return version


@functools.lru_cache(maxsize=1)
def _cli_auth_profiles() -> Optional[Dict[str, Any]]:
    auth_profiles = _cli_run("auth", "profiles")[0]
    log_utils.logger().debug(f"auth profiles:{auth_profiles}")
    return auth_profiles


def _cli_auth_login(profile: str):
    _cli_run("auth", "login", profile=profile)


if __name__ == "__main__":
    # os.environ["DATABRICKS_RUNTIME_VERSION"]="123"
    print(config(""))
    print(token())
    print(context())
    print(context())
    df = spark().sql("select * from reggie_pierce.document_hub_dev.file_parse")
    df.show()
    df = spark().sql("select * from reggie_pierce.document_hub_dev.file_parse")
    df.show()
    spark().sql("select 'hello there' as msg").show()
    w = workspace_client()
    for v in w.jobs.list(name="dbdemos_job_bike_init_dilan_patel"):
        log_utils.logger().info(v)
