from pyspark.sql.functions import regexp_replace, col
from pyspark.sql import DataFrame


def remove_space(df: DataFrame, column_name: str) -> DataFrame:
    result = df.withColumn(column_name, regexp_replace(col(column_name), r"\s{2,}", " "))
    return result


def filter_senior(df: DataFrame, column_name: str) -> DataFrame:
    result = df.filter(col(column_name) < 60)
    return result
