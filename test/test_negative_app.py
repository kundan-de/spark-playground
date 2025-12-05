from common import filter_senior


def test_negative_filter_senior(spark_session):
    data = [
        {"name": "Kundan", "age": 80},
        {"name": "Shubhank", "age": 60},
        {"name": "Kumarika Sau", "age": 28},
        {"name": "Pankaj Kumar Rai", "age": 35},
    ]

    df = spark_session.createDataFrame(data)
    prepared_df = filter_senior(df, "age")

    expected_data = [
        {"name": "Kundan", "age": 80},
        {"name": "Shubhank", "age": 60},
    ]
    expected_df = spark_session.createDataFrame(expected_data)

    assert expected_df.collect() == prepared_df.collect()
