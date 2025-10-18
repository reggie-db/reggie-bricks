from pyspark.sql import functions as F
from pyspark.sql.column import Column


def infer_json_schema(col: Column | str) -> Column:
    """Infer a schema string (array<struct<...>>, struct<...>, variant, or null)."""
    if isinstance(col, str):
        col = F.col(col)

    # Build `struct<...>` from keys
    def keys_to_struct_fields(keys_col: Column) -> Column:
        return F.array_join(
            F.transform(
                keys_col, lambda k: F.concat(F.lit("`"), k, F.lit("` variant"))
            ),
            F.lit(","),
        )

    # ARRAY: flatten keys from all elements
    keys_for_array = F.array_sort(
        F.array_distinct(
            F.flatten(
                F.transform(
                    F.from_json(col, "array<string>"), lambda x: F.json_object_keys(x)
                )
            )
        )
    )

    # OBJECT: directly extract keys
    keys_for_object = F.array_sort(F.json_object_keys(col))

    struct_str = F.concat(
        F.lit("struct<"), keys_to_struct_fields(keys_for_object), F.lit(">")
    )
    array_struct_str = F.concat(
        F.lit("array<struct<"), keys_to_struct_fields(keys_for_array), F.lit(">>")
    )

    return (
        F.when(col.isNull(), F.lit(None))
        .when(col.rlike(r"^\s*\["), array_struct_str)
        .when(col.rlike(r"^\s*\{"), struct_str)
        .otherwise(F.lit("variant"))
    )


def infer_json_type(col: Column | str) -> Column:
    """
    Quick JSON type inference using only the first non whitespace character.
    Returns: array, object, string, number, boolean, null, or NULL when undetected.
    """
    if isinstance(col, str):
        col = F.col(col)

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
    col: Column | str,
    *,
    infer_type: bool = False,
) -> Column:
    """
    Return a JSON string containing any combination of:
      {"value":...,"schema":"...","type":...}

    - schema includes a top level `value` field: struct<`value` ...>
    - type is quoted when known, or unquoted null when undetected- if all flags are False, returns NULL
    """
    if isinstance(col, str):
        col = F.col(col)

    inner_schema = infer_json_schema(col)
    schema_with_value = F.concat(F.lit("struct<`value` "), inner_schema, F.lit(">"))

    # build field fragments
    fields = [
        F.concat(F.lit('"value":'), col),
        F.concat(F.lit('"schema":"'), schema_with_value, F.lit('"')),
    ]

    if infer_type:
        t = infer_json_type(col)
        t_json = F.when(t.isNull(), F.lit("null")).otherwise(
            F.concat(F.lit('"'), t, F.lit('"'))
        )
        fields.append(F.concat(F.lit('"type":'), t_json))

    # join with commas and wrap with braces
    body = F.array_join(F.array(*fields), F.lit(","))
    expr = F.concat(F.lit("{"), body, F.lit("}"))

    return F.when(col.isNull(), F.lit(None)).otherwise(expr)


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
    df.withColumn("wrapped", infer_json("json_col", infer_type=True)).show(
        truncate=False
    )

    print("=== schema only ===")
    df.withColumn("wrapped", infer_json("json_col", infer_type=False)).show(
        truncate=False
    )

    print("=== value only ===")

    df.withColumn(
        "wrapped",
        infer_json("json_col", infer_type=False),
    ).show(truncate=False)

    df.withColumn("wrapped", infer_json("json_col")).show(truncate=False)
