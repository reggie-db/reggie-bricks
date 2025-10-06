import functools
from datetime import datetime
from typing import Optional

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from pyspark.sql import SparkSession

from reggie_tools import configs, logs, runtimes


def workspace_client(config: Config = None) -> WorkspaceClient:
    if not config:
        config = configs.get()
    return WorkspaceClient(config=config)


def spark(config: Optional[Config] = None) -> SparkSession:
    # Fast paths when no explicit config is provided
    if config is None:
        # Existing session injected in IPython user namespace
        sess = runtimes.ipython_user_ns("spark")
        if sess:
            return sess

        # Any active local Spark session
        sess = SparkSession.getActiveSession()
        if sess:
            return sess

        # Local Spark available via runtime
        if runtimes.version():
            return SparkSession.builder.getOrCreate()

        # Databricks Connect default session
        return _databricks_session_default()

    # Config provided or resolved above
    return DatabricksSession.builder.sdkConfig(config).getOrCreate()


@functools.cache
def _databricks_session_default() -> SparkSession:
    config = configs.get()
    log = logs.logger(__name__, __file__)
    log.info("databricks connect session initializing")
    start_time = datetime.now()
    sess = DatabricksSession.builder.sdkConfig(config).getOrCreate()
    elapsed = (datetime.now() - start_time).total_seconds()
    log.info(f"databricks connect session created in {elapsed:.2f}s")
    return sess


if __name__ == "__main__":
    import os

    os.environ["DATABRICKS_CONFIG_PROFILE"] = "FIELD-ENG-EAST"
    print(spark())
    print(spark())
