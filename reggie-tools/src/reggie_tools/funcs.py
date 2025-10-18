from pyspark.sql import functions as F
from pyspark.sql.column import Column


def col(col: Column | str) -> Column:
    if isinstance(col, str):
        return F.col(col)
    return col


def infer_json_schema(col: Column | str) -> Column:
    """Infer a schema string (array<struct<...>>, struct<...>, variant, or null)."""
    col = col(col)

    array_schemas = F.when(
        col.rlike(r"^\s*\["),
        F.transform(
            F.array_sort(
                F.array_distinct(
                    F.flatten(
                        F.transform(
                            F.from_json(col, "array<string>"),
                            lambda x: F.json_object_keys(x),
                        )
                    )
                )
            ),
            lambda x: F.concat(x, F.lit(" variant")),
        ),
    )

    array_schema = F.when(
        F.coalesce(F.array_size(array_schemas), F.lit(0)) > 0,
        F.concat(
            F.lit("array<struct<"),
            F.concat_ws(", ", array_schemas),
            F.lit(">>"),
        ),
    ).otherwise(F.lit(None))

    object_schemas = F.when(
        col.rlike(r"^\s*\{"),
        F.transform(
            F.array_sort(F.json_object_keys(col)),
            lambda x: F.concat(x, F.lit(" variant")),
        ),
    )

    object_schema = F.when(
        F.coalesce(F.array_size(object_schemas), F.lit(0)) > 0,
        F.concat(F.lit("struct<"), F.concat_ws(", ", object_schemas), F.lit(">")),
    ).otherwise(F.lit(None))

    return F.when(col.isNull(), F.lit(None)).otherwise(
        F.coalesce(array_schema, object_schema, F.lit("variant"))
    )


def infer_json_type(col: Column | str) -> Column:
    """
    Quick JSON type inference using only the first non whitespace character.
    Returns: array, object, string, number, boolean, null, or NULL when undetected.
    """
    col = col(col)
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
    include_value: bool = True,
    include_schema: bool = True,
    include_type: bool = False,
) -> Column:
    """
    Return a JSON string containing any combination of:
      {"value":...,"schema":"...","type":...}

    - schema includes a top level value field: struct<value ...>
    - type is quoted when known, or unquoted null when undetected- if all flags are False, returns NULL
    """
    col = col(col)
    if not any([include_value, include_schema, include_type]):
        return F.lit(None)
    exprs = [F.lit("{")]

    def _append_expr(*append_exprs):
        if len(exprs) > 1:
            exprs.append(F.lit(","))
        exprs.append(F.concat(*append_exprs))

    if include_value:
        _append_expr(F.lit('"value":'), col)
    if include_schema:
        _append_expr(
            F.lit('"schema":"struct<value '), infer_json_schema(col), F.lit('>"')
        )
    if include_type:
        _append_expr(F.lit('"type":"'), infer_json_type(col), F.lit('"'))
    exprs.append(F.lit("}"))

    return F.when(col.isNull(), F.lit(None)).otherwise(F.concat(*exprs))


def infer_json_parsed(col: Column | str) -> Column:
    col = col(col)
    infer_json_col = infer_json(col)
    return F.when(infer_json_col.isNull(), F.lit(None)).otherwise(
        F.from_json(infer_json_col, None, {"schemaLocationKey": "schema"})
    )


if __name__ == "__main__":
    import os

    from reggie_tools import clients

    TRUNCATE = 60
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
            (
                '[{"ID": "9417574000083", "Description": "FIJI WATER 500 ML", "Department": "271-BEVERAGES", "Sub_Department": "5-Water", "Account": "Inventory", "Size": "500 ML", "Brand": "FIJI", "Manufacturer": "FIJI", "Type": "Inventory", "UPC": "12345678901", "Test": "test_9417574000083"}, {"ID": "8936190136217", "Description": "HORCHA CINAMON", "Department": "271-BEVERAGES", "Sub_Department": "7-Other Beverage", "Account": "Inventory", "Size": "16 OZ", "Brand": "Generic", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "12345678902", "Test": "test_8936190136217"}, {"ID": "8936190136194", "Description": "HORCHATA", "Department": "271-BEVERAGES", "Sub_Department": "7-Other Beverage", "Account": "Inventory", "Size": "16 OZ", "Brand": "Generic", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "12345678903", "Test": "test_8936190136194"}, {"ID": "8936190135856", "Description": "WATERMELLON JUICE WITH PULP", "Department": "271-BEVERAGES", "Sub_Department": "2-Juice", "Account": "Inventory", "Size": "20 OZ", "Brand": "Generic", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "12345678904", "Test": "test_8936190135856"}, {"ID": "8936190135759", "Description": "DE MI PAISO 16 OZ", "Department": "271-BEVERAGES", "Sub_Department": "7-Other Beverage", "Account": "Inventory", "Size": "16 OZ", "Brand": "Generic", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "12345678905", "Test": "test_8936190135759"}, {"ID": "75B1C34F592FE29419D73F9D0FFAB4", "Description": "GATORADE LEMON LIME 20 OZ", "Department": "271-BEVERAGES", "Sub_Department": "Unknown", "Account": "Inventory", "Size": "Unknown", "Brand": "GATORADE", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "5847C2701245404072B4836BCB49ED", "Test": "test_75B1C34F592FE29419D73F9D0FFAB4"}, {"ID": "964BFECA158CB0658F93DA73A5B1CC", "Description": "COCA COLA CLASSIC 12 OZ CAN", "Department": "271-BEVERAGES", "Sub_Department": "Unknown", "Account": "Inventory", "Size": "Unknown", "Brand": "COCA COLA", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "1067A2AEEEE267711FD8A40C12B76D", "Test": "test_964BFECA158CB0658F93DA73A5B1CC"}, {"ID": "7CDAF43997A0D811A363C5FEEFF769", "Description": "LAYS CLASSIC POTATO CHIPS 2.75 OZ", "Department": "Uncategorized", "Sub_Department": "Unknown", "Account": "Inventory", "Size": "Unknown", "Brand": "LAYS", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "38C1A4A62C6C60EA67741D8D895A85", "Test": "test_7CDAF43997A0D811A363C5FEEFF769"}, {"ID": "BC55FA92B434DB92B25387A3CDB10C", "Description": "DORITOS NACHO CHEESE 2.75 OZ", "Department": "123-SNACKS", "Sub_Department": "Unknown", "Account": "Inventory", "Size": "Unknown", "Brand": "DORITOS", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "1A79194A09F89DC4EAB9205A551157", "Test": "test_BC55FA92B434DB92B25387A3CDB10C"}, {"ID": "45174CB2C7B8191197065C995D1E55", "Description": "MARLBORO RED BOX 20 CT", "Department": "456-TOBACCO", "Sub_Department": "Unknown", "Account": "Inventory", "Size": "Unknown", "Brand": "MARLBORO", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "395948C742151D73B29E6CD1B1E49B", "Test": "test_45174CB2C7B8191197065C995D1E55"}, {"ID": "E1D7296225F902160D45C0B95B2C69", "Description": "NEWPORT MENTHOL 100s 20 CT", "Department": "Uncategorized", "Sub_Department": "Unknown", "Account": "Inventory", "Size": "Unknown", "Brand": "NEWPORT", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "B4666E7BC1B3EB2B48F93C0614A213", "Test": "test_E1D7296225F902160D45C0B95B2C69"}, {"ID": "868B207A7B10850E4F0A51FD63A5EB", "Description": "OREO CHOCOLATE SANDWICH COOKIES 14 OZ", "Department": "123-SNACKS", "Sub_Department": "Unknown", "Account": "Inventory", "Size": "Unknown", "Brand": "OREO", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "2216D47BD83FCD7E1E973A970B8EB8", "Test": "test_868B207A7B10850E4F0A51FD63A5EB"}, {"ID": "926C0D758FF83E6FA84A141CCE5F70", "Description": "HERSHEY MILK CHOCOLATE BAR 1.55 OZ", "Department": "123-SNACKS", "Sub_Department": "Unknown", "Account": "Inventory", "Size": "Unknown", "Brand": "HERSHEY", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "C67DBEE7E15D529954ABA0EC7C66DB", "Test": "test_926C0D758FF83E6FA84A141CCE5F70"}, {"ID": "C35E15E275BF3C3F5F792CF31005BC", "Description": "STARBUCKS FRAPPUCCINO MOCHA 13.7 OZ", "Department": "Uncategorized", "Sub_Department": "Unknown", "Account": "Inventory", "Size": "Unknown", "Brand": "STARBUCKS", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "695E1195E39CA9295C8A60C3851FAC", "Test": "test_C35E15E275BF3C3F5F792CF31005BC"}, {"ID": "D07E12DFEDA88D3F4E1C28E80ADB64", "Description": "SWISHER SWEETS GRAPE CIGARILLOS 2 PACK", "Department": "456-TOBACCO", "Sub_Department": "Unknown", "Account": "Inventory", "Size": "Unknown", "Brand": "SWISHER", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "02937E7D132D063BF0F59FBA30F3B5", "Test": "test_D07E12DFEDA88D3F4E1C28E80ADB64"}, {"ID": "2960C4400278033C151657EE2F4264", "Description": "HOSTESS CUPCAKES 2 COUNT", "Department": "123-SNACKS", "Sub_Department": "Unknown", "Account": "Inventory", "Size": "Unknown", "Brand": "HOSTESS", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "CDC2D0E3990A8B4C2570C802BA43A8", "Test": "test_2960C4400278033C151657EE2F4264"}, {"ID": "332EEE6CCB36D718E3658AD7BEE816", "Description": "MONSTER ENERGY ORIGINAL 16 OZ", "Department": "271-BEVERAGES", "Sub_Department": "Unknown", "Account": "Inventory", "Size": "Unknown", "Brand": "MONSTER", "Manufacturer": "Generic", "Type": "Inventory", "UPC": "A0EB9233882FA7BB2068945B26BED3", "Test": "test_332EEE6CCB36D718E3658AD7BEE816"}]',
            ),
        ],
        ["json_col"],
    )

    print("=== schema + type + value ===")
    df.withColumn(
        "wrapped", infer_json(F.col("json_col"), include_value=False, include_type=True)
    ).drop("json_col").show(truncate=False)

    print("=== schema only ===")
    df.withColumn("wrapped", infer_json(F.col("json_col"))).show(truncate=TRUNCATE)

    print("=== value only ===")

    df.withColumn(
        "wrapped",
        infer_json(F.col("json_col")),
    ).show(truncate=TRUNCATE)

    df.withColumn("wrapped", infer_json(F.col("json_col"))).show(truncate=TRUNCATE)
