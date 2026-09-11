"""This project's own migration chain, applied end to end.

The migration *engine* is spark_sql_migrations's and is tested there. What is still ours,
and what this covers, is the content of all_spark_migrations/ -- that it forms a
walkable chain and that every statement in it runs on real Spark.
"""

import datetime
import os
import random
import string

from spark_sql_migrations import custom_logging
from spark_sql_migrations.spark_sql.spark_sql import ALL_SPARK, run_migrations

from src.crutch_migrations.run_crutch_migrations import get_migrations_dir

logger = custom_logging.setup_logging().getLogger(__name__)

HEAD_REVISION = "949db4199eab"

EXPECTED_PRIMED = [
    "260831_01_create_test_table_e5ce0039b32b_primed.sql",
    "260831_02_create_metrics_table_07990e2a101e_primed.sql",
    "260831_03_batch_id_for_metrics_table_57037b6c19b4_primed.sql",
    "260831_04_create_open_cms_data_kvp_table_949db4199eab_primed.sql",
]


def _fresh_schema():
    stamp = datetime.datetime.today().strftime("%Y%m%d_%H%M%S")
    suffix = "".join(random.choices(string.ascii_lowercase, k=6))
    return f"crutch_it_{stamp}_{suffix}"


def test_run_migrations(test_spark, tmp_path, request):
    logger.info(
        f"TEST: {request.node.name}; will be writing migrations under {tmp_path};"
    )
    schema = _fresh_schema()
    output_folder = str(tmp_path / "migrations_out")

    run_migrations(
        test_spark, "spark_catalog", schema, output_folder, get_migrations_dir()
    )

    results = [
        x.asDict()
        for x in test_spark.sql(
            f"select version_num from spark_catalog.{schema}._spark_migrations_version "
            f"where migration_type = '{ALL_SPARK}'"
        ).toLocalIterator()
    ]
    assert results[0]["version_num"] == HEAD_REVISION

    all_spark_out = os.path.join(output_folder, f"{ALL_SPARK}_migrations")
    assert sorted(os.listdir(all_spark_out)) == EXPECTED_PRIMED

    metrics_columns = [
        f.name
        for f in test_spark.table(f"spark_catalog.{schema}.metrics").schema.fields
    ]
    assert "metric_batch_id" in metrics_columns
