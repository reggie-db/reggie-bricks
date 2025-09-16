import functools
import json
import os
from pyspark.sql import SparkSession
from reggie_tools import clients
from typing import Optional, Any, Dict


@functools.cache
def version() -> Optional[Dict[str, Any]]:
    runtime_version = os.environ.get("DATABRICKS_RUNTIME_VERSION")
    return json.loads(runtime_version) if runtime_version else None


def context(spark: SparkSession = None) -> Optional[Any]:
    context: Dict[str, Any] = None
    get_context_function = _get_context_function()
    if get_context_function:
        context= get_context_function()
    context_dbutils = dbutils(spark)
    if context_dbutils and hasattr(context_dbutils, "entry_point"):
        context_json = context_dbutils.entry_point.getDbutils().notebook().getContext().safeToJson()
        context = json.loads(context_json).get("attributes", {})
    if not context:
        return None
    def _snake_to_camel(s: str) -> str:
        parts = s.split("_")
        return parts[0] + "".join(p.title() for p in parts[1:])
    def _convert_keys(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {_snake_to_camel(k): _convert_keys(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [_convert_keys(i) for i in obj]
        else:
            return obj
    return _convert_keys(context)


def dbutils(spark: SparkSession = None):
    if not spark:
        spark = clients.spark()
    dbutils = None
    dbutils_class = _dbutils_class()
    if dbutils_class:
        dbutils = dbutils_class(spark)
    if not dbutils:
        ipython_class = _ipython_class()
        if ipython_class:
            dbutils = ipython_class().get_ipython().user_ns["dbutils"]
    return dbutils


@functools.cache
def _dbutils_class():
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils
    except ImportError:
        return False


@functools.cache
def _ipython_class():
    try:
        import IPython
        return IPython
    except ImportError:
        pass


@functools.cache
def _get_context_function():
    try:
        from dbruntime.databricks_repl_context import get_context
        return get_context
    except ImportError:
        pass


if __name__ == "__main__":
    print(context())
