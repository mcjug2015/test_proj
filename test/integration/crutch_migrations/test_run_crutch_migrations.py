import os
from unittest import mock

from freezegun import freeze_time

from src import custom_logging
from src.crutch_migrations import run_crutch_migrations
from src.crutch_migrations.run_crutch_migrations import ALL_SPARK, Migration

logger = custom_logging.setup_logging().getLogger(__name__)


@freeze_time("2007-07-07")
def test_create_new_migration(tmp_path, request):
    logger.info(
        f"TEST: {request.node.name}; will be writing migrations under {tmp_path};"
    )
    output_path = tmp_path / f"{run_crutch_migrations.ALL_SPARK}_migrations"
    os.makedirs(output_path)
    template_path = os.path.join(
        os.path.dirname(run_crutch_migrations.__file__),
        "migration_templates",
        "default_migration_template.sql",
    )

    run_crutch_migrations.create_new_migration(
        "integration test migration", template_path, str(output_path)
    )

    migrations = run_crutch_migrations.get_migrations_list(ALL_SPARK, str(output_path))
    assert (len(migrations)) == 1
    assert migrations[0].prev_revision_id is None
    assert migrations[0].revision_id != ""


def test_run_migrations(test_spark, tmp_path):
    first_out = tmp_path / "migrations_out_first"
    with mock.patch.object(
        run_crutch_migrations,
        "get_unapplied_migrations_list",
        return_value=[
            Migration(
                revision_id="e5ce0039b32b",
                prev_revision_id=None,
                template_name="260831_01_create_test_table_e5ce0039b32b.sql",
            )
        ],
    ):
        # run migrations w. all_spark capped to a single one
        run_crutch_migrations.run_migrations(
            test_spark, "spark_catalog", "default", first_out
        )

    results = [
        x.asDict()
        for x in test_spark.sql(
            "select version_num from spark_catalog.default._spark_migrations_version "
            f"where migration_type = '{ALL_SPARK}'"
        ).toLocalIterator()
    ]
    assert results[0]["version_num"] == "e5ce0039b32b"

    first_all_spark_out = first_out / f"{ALL_SPARK}_migrations"
    assert sorted(os.listdir(first_all_spark_out)) == [
        "260831_01_create_test_table_e5ce0039b32b_primed.sql"
    ]

    second_out = tmp_path / "migrations_out_second"
    with mock.patch.object(
        run_crutch_migrations,
        "get_unapplied_migrations_list",
        return_value=[
            Migration(
                revision_id="07990e2a101e",
                prev_revision_id="e5ce0039b32b",
                template_name="260831_02_create_metrics_table_07990e2a101e.sql",
            ),
            Migration(
                revision_id="57037b6c19b4",
                prev_revision_id="07990e2a101e",
                template_name="260831_03_batch_id_for_metrics_table_57037b6c19b4.sql",
            ),
            Migration(
                revision_id="949db4199eab",
                prev_revision_id="57037b6c19b4",
                template_name="260831_04_create_open_cms_data_kvp_table_949db4199eab.sql",
            ),
        ],
    ):
        # run migrations w. more all spark let through
        run_crutch_migrations.run_migrations(
            test_spark, "spark_catalog", "default", second_out
        )

    results = [
        x.asDict()
        for x in test_spark.sql(
            "select version_num from spark_catalog.default._spark_migrations_version "
            f"where migration_type = '{ALL_SPARK}'"
        ).toLocalIterator()
    ]
    assert results[0]["version_num"] == "949db4199eab"

    second_all_spark_out = second_out / f"{ALL_SPARK}_migrations"
    assert sorted(os.listdir(second_all_spark_out)) == [
        "260831_02_create_metrics_table_07990e2a101e_primed.sql",
        "260831_03_batch_id_for_metrics_table_57037b6c19b4_primed.sql",
        "260831_04_create_open_cms_data_kvp_table_949db4199eab_primed.sql",
    ]
