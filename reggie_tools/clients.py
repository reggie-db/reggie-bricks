from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from reggie_tools import configs, runtimes
from pyspark.sql import SparkSession


def workspace_client(config: Config = None) -> WorkspaceClient:
    if not config:
        config = configs.get()
    return WorkspaceClient(config=config)


def spark(config: Config = None) -> SparkSession:
    if not config:
        config = configs.get()
    if runtimes.version():
        return SparkSession.builder.getOrCreate()
    spark_builder = DatabricksSession.builder.sdkConfig(config)
    return spark_builder.getOrCreate()
