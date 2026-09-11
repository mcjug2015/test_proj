from unittest import mock

from spark_sql_migrations import custom_logging

from src.crutch_migrations.run_crutch_migrations import (
    create_migration,
    get_migrations_dir,
    run,
)

logger = custom_logging.setup_logging().getLogger(__name__)


def test_get_migrations_dir():
    """the chains sit beside the module, so spark_sql_migrations can be told where they are."""
    assert get_migrations_dir().endswith("crutch_migrations")


@mock.patch("src.crutch_migrations.run_crutch_migrations.main")
@mock.patch(
    "src.crutch_migrations.run_crutch_migrations.get_migrations_dir",
    return_value="/i/am/a/fake/migrations/dir",
)
def test_run(get_migrations_dir_mock, main_mock):
    run("FAKETESTCAT", "FAKETESTSCHEMA")

    main_mock.assert_called_once_with(
        "FAKETESTCAT", "FAKETESTSCHEMA", "/i/am/a/fake/migrations/dir"
    )
    get_migrations_dir_mock.assert_called()


@mock.patch("src.crutch_migrations.run_crutch_migrations.create_new_migration")
@mock.patch(
    "src.crutch_migrations.run_crutch_migrations.get_default_template_path",
    return_value="/i/am/a/fake/template.sql",
)
@mock.patch(
    "src.crutch_migrations.run_crutch_migrations.get_migrations_dir",
    return_value="/i/am/a/fake/migrations/dir",
)
def test_create_migration(
    get_migrations_dir_mock, get_default_template_path_mock, create_new_migration_mock
):
    create_migration("a test migration")

    create_new_migration_mock.assert_called_once_with(
        "a test migration",
        "/i/am/a/fake/template.sql",
        "/i/am/a/fake/migrations/dir/all_spark_migrations",
    )
    get_default_template_path_mock.assert_called_once()
    get_migrations_dir_mock.assert_called_once()
