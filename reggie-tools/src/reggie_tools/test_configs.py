"""Manual check entrypoint for verifying Databricks config discovery."""

from reggie_tools import configs

print(configs.get())
