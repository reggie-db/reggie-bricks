import re
import uuid
from builtins import RuntimeError, ValueError
from dataclasses import dataclass
from pyspark.sql import SparkSession
from pyspark.sql.connect.functions import current_catalog
from reggie_tools import clients, configs, logs, runtimes
from typing import Set


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


def catalog_schema(spark: SparkSession = None) -> CatalogSchema:
    catalog_name = configs.config_value("catalog_name", spark=spark, secrets=False)
    if catalog_name:
        schema_name = configs.config_value("schema_name", spark=spark, secrets=False)
        if schema_name:
            return CatalogSchema(catalog_name, schema_name)
    if runtimes.is_pipeline(spark):
        catalog_schemas: Set[CatalogSchema] = set()
        try:
            # Intentionally reference a non existent table to surface fully qualified path in error
            (spark or clients.spark()).sql(f"SELECT * FROM table_{uuid.uuid4().hex} LIMIT 1").count()
        except Exception as e:
            msg = str(e)
            matches = re.findall(r"`([^`]+)`\.`([^`]+)`\.`([^`]+)`", msg)
            for c, s, _ in matches:
                if c and s:
                    catalog_schemas.add(CatalogSchema(c, s))
        if len(catalog_schemas) == 1:
            return next(iter(catalog_schemas))
    catalog_schema_row = (spark or clients.spark()).sql(
        "SELECT current_catalog() AS catalog, current_schema() AS schema").first()
    if catalog_schema_row.catalog and catalog_schema_row.schema:
        return CatalogSchema(catalog_schema_row.catalog, catalog_schema_row.schema)
    raise ValueError(
        f"catalog/schema not found - current_catalog:{current_catalog} current_schema:{current_schema}")


def catalog(spark: SparkSession = None):
    catalog_schema = catalog_schema(spark)
    return catalog_schema.catalog if catalog_schema else None


def schema(spark: SparkSession = None):
    catalog_schema = catalog_schema(spark)
    return catalog_schema.schema if catalog_schema else None


if __name__ == "__main__":
    print(catalog_schema())
