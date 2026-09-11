import argparse
import datetime
import json
import os
import shutil
import sys
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from spark_sql_migrations import custom_logging
from spark_sql_migrations.spark_sql.spark_sql import get_ascending_letters_within_minute
from spark_sql_migrations.spark_utils import get_spark

logger = custom_logging.setup_logging().getLogger(__name__)


class AbstractBenchmark(ABC):
    def __init__(self, spark: SparkSession, cat: str, schema: str, the_batch_id: str):
        self.spark = spark
        self.cat = cat
        self.schema = schema
        self.metric_name = self.__class__.__name__
        self.the_batch_id = the_batch_id

    @abstractmethod
    def benchmark(self) -> Dict[str, Any]:
        pass

    def save_metric(self, metric):
        # Inline literals rather than :named params: pyspark's local session does
        # not bind spark.sql(..., args=...) parameters. Single quotes are escaped
        # to keep the SQL well-formed; current_timestamp() avoids a datetime literal.
        metric_id = str(uuid.uuid4())
        metric_name = self.metric_name.replace("'", "''")
        payload = json.dumps(metric).replace("'", "''")
        self.spark.sql(
            f"""
            insert into {self.cat}.{self.schema}.metrics (id, metric_name, payload, metric_batch_id, last_updated)
            values ('{metric_id}', '{metric_name}', parse_json('{payload}'), '{self.the_batch_id}', current_timestamp())
            """
        )

    def execute(self):
        metric = self.benchmark()
        self.save_metric(metric)
        logger.info(f"{self.metric_name} saved metric: {metric}")


class SingleRowInsert(AbstractBenchmark):

    def benchmark(self) -> Dict[str, Any]:
        df = self.spark.table(f"{self.cat}.{self.schema}.test_table")
        print(df.columns)
        stuff = f"QQPP{datetime.datetime.today().strftime('%Y%m%d_%H%M')}_{get_ascending_letters_within_minute()}"
        start = time.perf_counter()
        self.spark.sql(
            f"""
            insert into {self.cat}.{self.schema}.test_table(int_id, stuff) values ({time.time_ns()}, '{stuff}');
            """
        )
        duration = time.perf_counter() - start
        return {
            "total_seconds": duration,
        }


class ClusterRoundtripLatency(AbstractBenchmark):
    def __init__(
        self,
        spark: SparkSession,
        cat: str,
        schema: str,
        the_batch_id: str,
        iterations: int = 50,
    ):
        super().__init__(spark, cat, schema, the_batch_id)
        self.iterations = iterations

    def benchmark(self) -> Dict[str, Any]:
        """Benchmark driver<->cluster round-trip latency using trivial no-op queries.

        Runs many minimal ``select 1`` queries so the measured time is dominated by
        request/response overhead rather than computation, exposing how "chatty" the
        connection to this particular spark instance is.
        """
        durations = []
        for _ in range(self.iterations):
            start = time.perf_counter()
            self.spark.sql("select 1").collect()
            durations.append(time.perf_counter() - start)
        durations.sort()
        total = sum(durations)
        return {
            "iterations": self.iterations,
            "total_seconds": total,
            "avg_ms": (total / self.iterations) * 1000,
            "min_ms": durations[0] * 1000,
            "p50_ms": durations[len(durations) // 2] * 1000,
            "p95_ms": durations[min(len(durations) - 1, int(len(durations) * 0.95))]
            * 1000,
            "max_ms": durations[-1] * 1000,
        }


class RangeAggregation(AbstractBenchmark):
    def __init__(
        self,
        spark: SparkSession,
        cat: str,
        schema: str,
        the_batch_id: str,
        num_rows: int = 50_000_000,
    ):
        super().__init__(spark, cat, schema, the_batch_id)
        self.num_rows = num_rows

    def benchmark(self) -> Dict[str, Any]:
        """Benchmark raw compute throughput via a full-scan aggregation.

        A single-pass ``sum`` over ``spark.range`` is CPU/scan bound with no shuffle
        or data transfer to the driver, so it isolates the cluster's per-row compute
        rate. The checksum is returned to prevent the optimizer eliding the work.
        """
        start = time.perf_counter()
        result = (
            self.spark.range(0, self.num_rows)
            .select(F.sum(F.col("id")).alias("total"))
            .collect()
        )
        elapsed = time.perf_counter() - start
        return {
            "num_rows": self.num_rows,
            "total_seconds": elapsed,
            "rows_per_second": self.num_rows / elapsed if elapsed else float("inf"),
            "checksum": result[0]["total"] if result else None,
        }


class ShuffleGroupBy(AbstractBenchmark):
    def __init__(
        self,
        spark: SparkSession,
        cat: str,
        schema: str,
        the_batch_id: str,
        num_rows: int = 20_000_000,
        num_groups: int = 1000,
    ):
        super().__init__(spark, cat, schema, the_batch_id)
        self.num_rows = num_rows
        self.num_groups = num_groups

    def benchmark(self) -> Dict[str, Any]:
        """Benchmark shuffle/exchange performance via a wide group-by.

        Grouping many rows into ``num_groups`` buckets forces a shuffle across the
        cluster, stressing network and disk between executors rather than scan
        speed, which is where distributed spark instances differ most.
        """
        start = time.perf_counter()
        grouped = (
            self.spark.range(0, self.num_rows)
            .withColumn("grp", F.col("id") % self.num_groups)
            .groupBy("grp")
            .agg(
                F.count(F.lit(1)).alias("cnt"),
                F.sum(F.col("id")).alias("s"),
            )
        )
        collected = grouped.collect()
        elapsed = time.perf_counter() - start
        return {
            "num_rows": self.num_rows,
            "num_groups": self.num_groups,
            "groups_returned": len(collected),
            "total_seconds": elapsed,
            "rows_per_second": self.num_rows / elapsed if elapsed else float("inf"),
        }


class CollectBandwidth(AbstractBenchmark):
    def __init__(
        self,
        spark: SparkSession,
        cat: str,
        schema: str,
        the_batch_id: str,
        num_rows: int = 1_000_000,
    ):
        super().__init__(spark, cat, schema, the_batch_id)
        self.num_rows = num_rows

    def benchmark(self) -> Dict[str, Any]:
        """Benchmark result-transfer bandwidth from executors back to the driver.

        Materializing a payload column and pulling every row to the driver with
        ``collect`` measures serialization + network cost of moving data out of
        the cluster, the opposite bottleneck from the compute-bound benchmarks.
        """
        # md5 yields a stable 32-char hex string per row for a predictable size.
        df = self.spark.range(0, self.num_rows).withColumn(
            "payload", F.md5(F.col("id").cast("string"))
        )
        start = time.perf_counter()
        rows = df.collect()
        elapsed = time.perf_counter() - start
        # 8-byte long + 32-char payload, rough estimate.
        approx_bytes = len(rows) * (8 + 32)
        return {
            "num_rows": self.num_rows,
            "rows_collected": len(rows),
            "total_seconds": elapsed,
            "rows_per_second": len(rows) / elapsed if elapsed else float("inf"),
            "approx_mb_per_second": (
                (approx_bytes / 1e6) / elapsed if elapsed else float("inf")
            ),
        }


class PythonUdfOverhead(AbstractBenchmark):
    def __init__(
        self,
        spark: SparkSession,
        cat: str,
        schema: str,
        the_batch_id: str,
        num_rows: int = 5_000_000,
    ):
        super().__init__(spark, cat, schema, the_batch_id)
        self.num_rows = num_rows

    def benchmark(self) -> Dict[str, Any]:
        """Benchmark the cost of Python UDF serialization vs. native expressions.

        The same squaring computation is run once with a built-in column
        expression and once with a Python UDF; the ratio quantifies the per-row
        JVM<->Python round-trip overhead for this spark instance's workers.
        """
        start = time.perf_counter()
        self.spark.range(0, self.num_rows).select(
            (F.col("id") * F.col("id")).alias("v")
        ).agg(F.sum(F.col("v"))).collect()
        native_elapsed = time.perf_counter() - start

        square_udf = F.udf(lambda x: x * x, LongType())
        start = time.perf_counter()
        self.spark.range(0, self.num_rows).select(
            square_udf(F.col("id")).alias("v")
        ).agg(F.sum(F.col("v"))).collect()
        udf_elapsed = time.perf_counter() - start

        return {
            "num_rows": self.num_rows,
            "total_seconds": native_elapsed,
            "python_udf_seconds": udf_elapsed,
            "udf_overhead_ratio": (
                udf_elapsed / native_elapsed if native_elapsed else float("inf")
            ),
        }


def main(*args, **kwargs):
    logger.info("manipulator main begins")
    cat = kwargs.get("cat", None)
    schema = kwargs.get("schema", None)
    if not cat or not schema:
        cat = sys.argv[1]
        schema = sys.argv[2]
    if not cat or not schema:
        raise ValueError(
            f"Expecting both cat and schema but got {args}, {kwargs}, {sys.argv};"
        )
    logger.info(f"will be using cat:{cat}; schema:{schema};")
    spark = get_spark()
    the_batch_id = f"{datetime.datetime.today().strftime('%Y%m%d_%H%M')}_{get_ascending_letters_within_minute()}_{uuid4()}"  # noqa: E501
    SingleRowInsert(spark, cat, schema, the_batch_id).execute()
    ClusterRoundtripLatency(spark, cat, schema, the_batch_id, iterations=5).execute()
    RangeAggregation(spark, cat, schema, the_batch_id, num_rows=5000).execute()
    ShuffleGroupBy(
        spark, cat, schema, the_batch_id, num_rows=5000, num_groups=2
    ).execute()
    CollectBandwidth(spark, cat, schema, the_batch_id, num_rows=5000).execute()
    PythonUdfOverhead(spark, cat, schema, the_batch_id, num_rows=5000).execute()
    logger.info("main manipulator end")


if __name__ == "__main__":  # pragma: no cover
    shutil.rmtree(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "spark-warehouse"),
        ignore_errors=True,
    )
    parser = argparse.ArgumentParser(description="manipulator params")
    parser.add_argument(
        "--cat",
        help="catalog name to use",
        default="spark_catalog",
    )
    parser.add_argument(
        "--schema",
        help="schema name to use",
        default="default",
    )
    args = parser.parse_args()
    main(cat=args.cat, schema=args.schema)
