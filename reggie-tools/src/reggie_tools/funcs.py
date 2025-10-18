from pyspark.sql import functions as F
from pyspark.sql.column import Column


def infer_json_schema(col: Column) -> Column:
    """Infer a schema string (array<struct<...>>, struct<...>, variant, or null)."""

    return (
        F.when(col.isNull(), F.lit(None))
        .when(
            col.rlike(r"^\s*\["),
            F.concat(
                F.lit("array<struct<"),
                F.array_join(
                    F.transform(
                        F.array_distinct(
                            F.flatten(
                                F.transform(
                                    F.from_json(col, "array<string>"),
                                    lambda x: F.json_object_keys(x),
                                )
                            )
                        ),
                        lambda x: F.concat(x, F.lit(" variant")),
                    ),
                    F.lit(","),
                ),
                F.lit(">>"),
            ),
        )
        .when(
            col.rlike(r"^\s*\{"),
            F.concat(
                F.lit("struct<"),
                F.array_join(
                    F.transform(
                        F.json_object_keys(col),
                        lambda x: F.concat(x, F.lit(" variant")),
                    ),
                    F.lit(","),
                ),
                F.lit(">"),
            ),
        )
        .otherwise(F.lit("variant"))
    )


def infer_json_type(col: Column) -> Column:
    """
    Quick JSON type inference using only the first non whitespace character.
    Returns: array, object, string, number, boolean, null, or NULL when undetected.
    """

    return (
        F.when(col.isNull(), F.lit("null"))
        .when(col.rlike(r"^\s*\["), F.lit("array"))
        .when(col.rlike(r"^\s*\{"), F.lit("object"))
        .when(col.rlike(r'^\s*["\']'), F.lit("string"))
        .when(col.rlike(r"^\s*[+-]?[0-9]"), F.lit("number"))
        .when(col.rlike(r"^\s*[tT]"), F.lit("boolean"))
        .when(col.rlike(r"^\s*[fF]"), F.lit("boolean"))
        .when(col.rlike(r"^\s*[nN]"), F.lit("null"))
        .otherwise(F.lit("null"))  # cannot detect
    )


def infer_json(
    col: Column,
    *,
    infer_type: bool = False,
) -> Column:
    """
    Return a JSON string containing any combination of:
      {"value":...,"schema":"...","type":...}

    - schema includes a top level value field: struct<value ...>
    - type is quoted when known, or unquoted null when undetected- if all flags are False, returns NULL
    """

    inner_schema = infer_json_schema(col)
    if infer_type:
        type_expr = F.concat(F.lit(',"type":"'), infer_json_type(col), F.lit('"'))
    else:
        type_expr = F.lit("")
    return F.concat(
        F.lit('{"value":'),
        col,
        F.lit(',"schema":"struct<value '),
        inner_schema,
        F.lit('>"'),
        type_expr,
        F.lit("}"),
    )


if __name__ == "__main__":
    import os

    from reggie_tools import clients

    os.environ["DATABRICKS_CONFIG_PROFILE"] = "FIELD-ENG-EAST"
    df = clients.spark().createDataFrame(
        [
            ('{"a":1,"b":2}',),
            ('[{"x":1},{"y":2}]',),
            ('"hello"',),
            ("42",),
            ("true",),
            ("null",),
            ("???",),  # undetected -> "type": null
            (None,),
        ],
        ["json_col"],
    )

    print("=== schema + type + value ===")
    df.withColumn("wrapped", infer_json(F.col("json_col"), infer_type=True)).show(
        truncate=False
    )

    print("=== schema only ===")
    df.withColumn("wrapped", infer_json(F.col("json_col"), infer_type=False)).show(
        truncate=False
    )

    print("=== value only ===")

    df.withColumn(
        "wrapped",
        infer_json(F.col("json_col"), infer_type=False),
    ).show(truncate=False)

    df.withColumn("wrapped", infer_json(F.col("json_col"))).show(truncate=False)
