from unittest import mock

import pytest
from spark_sql_migrations import custom_logging

from src.cms_pipeline import manipulator
from src.cms_pipeline.manipulator import (
    AbstractBenchmark,
    ClusterRoundtripLatency,
    CollectBandwidth,
    PythonUdfOverhead,
    RangeAggregation,
    ShuffleGroupBy,
    SingleRowInsert,
)

logger = custom_logging.setup_logging().getLogger(__name__)


@mock.patch.multiple(AbstractBenchmark, __abstractmethods__=set())
def test_abstract_benchmark_save_metric(migrated_spark, request):
    spark = migrated_spark[0]
    schema = migrated_spark[1]
    logger.info(f"TEST: {request.node.name}; will be using schema {schema};")
    bmrk = AbstractBenchmark(
        spark,
        cat="spark_catalog",
        schema=schema,
        the_batch_id="7",
    )
    bmrk.save_metric({"testing": "testing"})
    sql_result = spark.sql(f"select * from spark_catalog.{schema}.metrics;")
    results = [x.asDict() for x in sql_result.toLocalIterator()]
    assert len(results) == 1
    assert results[0]["metric_name"] == "AbstractBenchmark"


@mock.patch.multiple(AbstractBenchmark, __abstractmethods__=set())
@mock.patch("src.cms_pipeline.manipulator.AbstractBenchmark.save_metric")
@mock.patch("src.cms_pipeline.manipulator.AbstractBenchmark.benchmark")
def test_abstract_benchmark_execute(benchmark, save_metric):
    bmrk = AbstractBenchmark(None, cat=None, schema=None, the_batch_id=None)
    bmrk.execute()
    benchmark.assert_called_once()
    save_metric.assert_called_once()


def test_single_row_insert(migrated_spark, request):
    spark = migrated_spark[0]
    schema = migrated_spark[1]
    logger.info(f"TEST: {request.node.name}; will be using schema {schema};")
    SingleRowInsert(
        spark,
        cat="spark_catalog",
        schema=schema,
        the_batch_id="7",
    ).benchmark()
    sql_result = spark.sql(
        f"select * from spark_catalog.{schema}.test_table order by int_id desc;"
    )
    results = [x.asDict() for x in sql_result.toLocalIterator()]
    assert len(results) == 1
    assert results[0]["stuff"].startswith("QQPP")
    logger.info("end of test")


def test_cluster_roundtrip_latency(test_spark):
    result = ClusterRoundtripLatency(
        test_spark,
        cat="spark_catalog",
        schema="default",
        the_batch_id="7",
        iterations=1,
    ).benchmark()
    assert result["iterations"] == 1
    assert result["total_seconds"] > 0


def test_range_aggregation(test_spark):
    result = RangeAggregation(
        test_spark, cat="spark_catalog", schema="default", the_batch_id="7", num_rows=1
    ).benchmark()
    assert result["num_rows"] == 1
    assert result["total_seconds"] >= 0


def test_shuffle_group_by(test_spark):
    result = ShuffleGroupBy(
        test_spark,
        cat="spark_catalog",
        schema="default",
        the_batch_id="7",
        num_rows=1,
        num_groups=1,
    ).benchmark()
    assert result["num_rows"] == 1
    assert result["num_groups"] == 1
    assert result["total_seconds"] >= 0


def test_collect_bandwidth(test_spark):
    result = CollectBandwidth(
        test_spark, cat="spark_catalog", schema="default", the_batch_id="7", num_rows=1
    ).benchmark()
    assert result["num_rows"] == 1
    assert result["rows_collected"] == 1
    assert result["total_seconds"] >= 0


def test_python_udf_overhead(test_spark):
    result = PythonUdfOverhead(
        test_spark, cat="spark_catalog", schema="default", the_batch_id="7", num_rows=1
    ).benchmark()
    assert result["num_rows"] == 1
    assert result["total_seconds"] >= 0
    assert result["python_udf_seconds"] >= 0


# Decorators apply bottom-up, so the mocks arrive as arguments in the reverse
@mock.patch("src.cms_pipeline.manipulator.get_spark")
@mock.patch("src.cms_pipeline.manipulator.SingleRowInsert")
@mock.patch("src.cms_pipeline.manipulator.ClusterRoundtripLatency")
@mock.patch("src.cms_pipeline.manipulator.RangeAggregation")
@mock.patch("src.cms_pipeline.manipulator.ShuffleGroupBy")
@mock.patch("src.cms_pipeline.manipulator.CollectBandwidth")
@mock.patch("src.cms_pipeline.manipulator.PythonUdfOverhead")
def test_main_calls_execute_on_benchmarks(
    mock_python_udf_overhead,
    mock_collect_bandwidth,
    mock_shuffle_group_by,
    mock_range_aggregation,
    mock_cluster_roundtrip_latency,
    mock_single_row_insert,
    mock_get_spark,
):
    """main() should invoke .execute() on every benchmark class."""
    manipulator.main(cat="c", schema="s")

    for benchmark_class in (
        mock_single_row_insert,
        mock_cluster_roundtrip_latency,
        mock_range_aggregation,
        mock_shuffle_group_by,
        mock_collect_bandwidth,
        mock_python_udf_overhead,
    ):
        benchmark_class.return_value.execute.assert_called_once()
    mock_get_spark.assert_called_once()


@mock.patch(
    "src.cms_pipeline.manipulator.sys.argv",
    ["manipulator.py", "", "argv_schema"],
)
def test_main_raises_when_no_cat_or_schema():
    """main() should raise if neither kwargs nor sys.argv supply both cat and schema."""
    with pytest.raises(ValueError, match="Expecting both cat and schema"):
        manipulator.main()
