from spark_sql_migrations import custom_logging

from src.cms_pipeline import manipulator

logger = custom_logging.setup_logging().getLogger(__name__)


def test_main_runs_all_benchmarks_and_saves_metrics(migrated_spark, request):
    spark = migrated_spark[0]
    schema = migrated_spark[1]
    logger.info(f"TEST: {request.node.name}; will be using schema {schema};")
    rows_before = spark.sql(
        f"select count(*) as cnt from spark_catalog.{schema}.test_table;"
    ).collect()[0]["cnt"]

    manipulator.main(cat="spark_catalog", schema=schema)

    # main() doesn't return or expose the batch id it generates internally, so scope the
    # assertions to the most recently written batch rather than assuming a pristine table —
    # other tests sharing this session may also have written rows to spark_catalog.{schema}.metrics.
    latest_batch_id = spark.sql(
        f"""
        select metric_batch_id
        from spark_catalog.{schema}.metrics
        group by metric_batch_id
        order by max(last_updated) desc
        limit 1
        """
    ).collect()[0]["metric_batch_id"]
    metric_names = sorted(
        r["metric_name"]
        for r in spark.sql(
            f"select metric_name from spark_catalog.{schema}.metrics where metric_batch_id = '{latest_batch_id}';"
        ).toLocalIterator()
    )
    assert metric_names == sorted(
        [
            "SingleRowInsert",
            "ClusterRoundtripLatency",
            "RangeAggregation",
            "ShuffleGroupBy",
            "CollectBandwidth",
            "PythonUdfOverhead",
        ]
    )

    test_table_rows = spark.sql(
        f"select stuff from spark_catalog.{schema}.test_table order by int_id desc limit 1;"
    ).collect()
    rows_after = spark.sql(
        f"select count(*) as cnt from spark_catalog.{schema}.test_table;"
    ).collect()[0]["cnt"]
    assert rows_after == rows_before + 1
    assert test_table_rows[0]["stuff"].startswith("QQPP")
