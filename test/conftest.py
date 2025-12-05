import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    """Create a Spark session and return it."""
    spark = (
        SparkSession.builder.appName("Testing").getOrCreate()
    )
    return spark