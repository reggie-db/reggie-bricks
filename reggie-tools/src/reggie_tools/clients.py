from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from pyspark.sql import SparkSession

from reggie_tools import configs, logs, runtimes

LOG = logs.logger(__name__)


def spark(config: Config = None) -> SparkSession:
    if not config:
        spark_session = runtimes.ipython_user_ns("spark")
        if spark_session:
            return spark_session
        spark_session = SparkSession.getActiveSession()
        if spark_session:
            return spark_session
        elif runtimes.version():
            return SparkSession.builder.getOrCreate()
        config = configs.get()
    spark_builder = DatabricksSession.builder.sdkConfig(config)
    return spark_builder.getOrCreate()


def workspace_client(config: Config = None) -> WorkspaceClient:
    if not config:
        config = configs.get()
    return WorkspaceClient(config=config)


if __name__ == "__main__":
    print(spark().conf.get("spark.master"))
