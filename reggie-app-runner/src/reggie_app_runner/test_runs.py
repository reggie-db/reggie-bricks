import os

from reggie_app_runner import runs

if __name__ == "__main__":
    os.environ["DATABRICKS_APP_CONFIG_0_SOURCE"] = (
        "git@github.com:reggie-db/test-app.git"
    )
    os.environ["DATABRICKS_APP_CONFIG_0_COMMAND"] = "uv run --script app.py"
    os.environ["DATABRICKS_APP_CONFIG_0_POLL_INTERVAL"] = "15s"
    # os.environ["DATABRICKS_APP_CONFIG_0_ENV_PYTHONUNBUFFERED"] = "1"

    os.environ["DATABRICKS_APP_CONFIG_1_SOURCE"] = (
        "git@github.com:reggie-db/document-hub.git"
    )
    os.environ["DATABRICKS_APP_CONFIG_1_COMMAND"] = "./gradlew app:run"
    os.environ["DATABRICKS_APP_CONFIG_1_POLL_INTERVAL"] = "15s"
    runs.run()
