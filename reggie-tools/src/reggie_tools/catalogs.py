import functools
import re
import uuid
from builtins import hasattr
from dataclasses import dataclass
from typing import Optional, Set

from pyspark.sql import SparkSession

from reggie_tools import clients, configs, runtimes


@dataclass(frozen=True)
class CatalogSchema:
    catalog: str
    schema: str

    def __str__(self) -> str:
        return f"{self.catalog}.{self.schema}"


@dataclass(frozen=True)
class CatalogSchemaTable(CatalogSchema):
    table: str

    def __str__(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


@functools.cache
def _catalog_schema_config() -> Optional[CatalogSchema]:
    config_value_sources = configs.ConfigValueSource.without(
        configs.ConfigValueSource.SECRETS
    )
    catalog_name = configs.config_value(
        "catalog_name", config_value_sources=config_value_sources
    )
    if catalog_name:
        schema_name = configs.config_value(
            "schema_name", config_value_sources=config_value_sources
        )
        if schema_name:
            return CatalogSchema(catalog_name, schema_name)
    if runtimes.is_pipeline():
        catalog_schemas: Set[CatalogSchema] = set()
        try:
            # Intentionally reference a non existent table to surface fully qualified path in error
            clients.spark().sql(
                f"SELECT * FROM table_{uuid.uuid4().hex} LIMIT 1"
            ).count()
        except Exception as e:
            msg = str(e)
            matches = re.findall(r"`([^`]+)`\.`([^`]+)`\.`([^`]+)`", msg)
            for c, s, _ in matches:
                if c and s:
                    catalog_schemas.add(CatalogSchema(c, s))
        if len(catalog_schemas) == 1:
            return next(iter(catalog_schemas))
    return None


def catalog_schema(spark: SparkSession = None) -> Optional[CatalogSchema]:
    catalog_schema_config = _catalog_schema_config()
    if catalog_schema_config:
        return catalog_schema_config
    if not spark:
        spark = clients.spark()
    if hasattr(spark, "catalog"):
        spark_catalog = spark.catalog
        if (
            spark_catalog
            and hasattr(spark_catalog, "currentCatalog")
            and hasattr(spark_catalog, "currentDatabase")
        ):
            current_catalog = spark_catalog.currentCatalog()
            current_schema = spark_catalog.currentDatabase()
            if current_catalog and current_schema:
                return CatalogSchema(current_catalog, current_schema)
    catalog_schema_row = (
        (spark or clients.spark())
        .sql("SELECT current_catalog() AS catalog, current_schema() AS schema")
        .first()
    )
    if catalog_schema_row.catalog and catalog_schema_row.schema:
        return CatalogSchema(catalog_schema_row.catalog, catalog_schema_row.schema)
    return None


def catalog_schema_table(
    table: str, spark: SparkSession = None
) -> Optional[CatalogSchemaTable]:
    if table:
        cs = catalog_schema(spark)
        if cs:
            return CatalogSchemaTable(table, catalog_schema)
    return None


def catalog(spark: SparkSession = None) -> Optional[str]:
    cs = catalog_schema(spark)
    return cs.catalog if catalog_schema else None


def schema(spark: SparkSession = None) -> Optional[str]:
    cs = catalog_schema(spark)
    return cs.schema if catalog_schema else None


if __name__ == "__main__":
    print(catalog_schema_table("reggie_tools.test.test"))
    print(catalog_schema())
