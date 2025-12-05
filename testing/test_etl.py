import pytest
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession

from etl import remove_extra_spaces


@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark


# Define unit test
def test_single_space(spark_fixture):
    sample_data = [
        {"name": "John    D.", "age": 30},
        {"name": "Alice   G.", "age": 25},
        {"name": "Bob  T.", "age": 35},
        {"name": "Eve   A.", "age": 28},
    ]

    # Create a Spark DataFrame
    original_df = spark_fixture.createDataFrame(sample_data)

    # Apply the transformation function from before
    transformed_df = remove_extra_spaces(original_df, "name")

    expected_data = [
        {"name": "John D.", "age": 30},
        {"name": "Alice G.", "age": 25},
        {"name": "Bob T.", "age": 35},
        {"name": "Eve A.", "age": 28},
    ]

    expected_df = spark_fixture.createDataFrame(expected_data)

    assertDataFrameEqual(transformed_df, expected_df)
