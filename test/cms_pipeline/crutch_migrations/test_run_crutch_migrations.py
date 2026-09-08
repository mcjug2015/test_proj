import os
from unittest import mock
from uuid import UUID

import pytest
from freezegun import freeze_time
from jinja2.environment import Environment
from jinja2.loaders import FileSystemLoader
from jinja2.utils import select_autoescape

from src import custom_logging
from src.crutch_migrations import run_crutch_migrations
from src.crutch_migrations.run_crutch_migrations import (
    ALL_SPARK,
    DBR_ONLY,
    Migration,
    _get_migrations_dir,
    _parse_migration,
    apply_template,
    create_new_migration,
    get_ascending_letters_within_minute,
    get_migrations_list,
    get_ordered_migration_objs,
    get_output_folder,
    get_unapplied_migrations_list,
    main,
    migrate_initial,
    migrate_w_rev,
    record_revision,
    run_migrations,
    use_migration_file,
)

logger = custom_logging.setup_logging().getLogger(__name__)


def _write_dummy_sql(migrations_dir, migrations):
    for migration in migrations:
        with open(os.path.join(migrations_dir, migration.template_name), "w") as handle:
            handle.write("-- dummy;\n")


def _patch_parse_migration(migrations):
    by_template_name = {migration.template_name: migration for migration in migrations}
    return mock.patch.object(
        run_crutch_migrations,
        "_parse_migration",
        side_effect=lambda migrations_dir, template_name: by_template_name[
            template_name
        ],
    )


def _prime_migrations_tree(tmp_path, monkeypatch, migration_type):
    os.makedirs(tmp_path / f"{migration_type}_migrations")
    os.makedirs(tmp_path / "migration_templates")
    with open(
        tmp_path / "migration_templates" / "default_migration_template.sql", "w"
    ) as handle:
        handle.write("-- revision_id:{{revision_id}};\n-- prev_revision_id:;\n")
    monkeypatch.setattr(
        run_crutch_migrations, "__file__", str(tmp_path / "run_crutch_migrations.py")
    )


def test_get_migrations_dir_w_dir():
    assert _get_migrations_dir("qqq", "ppp") == "ppp"


def test_get_migrations_dir_w_type():
    assert _get_migrations_dir("qqq").endswith("qqq_migrations")


def test_parse_migration_reads_headers(tmp_path):
    with open(tmp_path / "01_headed.sql", "w") as handle:
        handle.write("-- revision_id:aaa;\n-- prev_revision_id:;\nselect 1;\n")

    assert _parse_migration(str(tmp_path), "01_headed.sql") == Migration(
        "aaa", None, "01_headed.sql"
    )


def test_parse_migration_no_rev_id(tmp_path):
    with open(tmp_path / "01_headerless.sql", "w") as handle:
        handle.write("-- a comment, not a header\nbegin\nselect 1;\nend;\n")

    with pytest.raises(ValueError, match="has no revision_id header"):
        _parse_migration(str(tmp_path), "01_headerless.sql")


def test_parse_migration_empty_file(tmp_path):
    (tmp_path / "01_empty.sql").touch()

    with pytest.raises(ValueError, match="is empty"):
        _parse_migration(str(tmp_path), "01_empty.sql")


def test_parse_migration_happy(tmp_path):
    with open(tmp_path / "01_w_revision.sql", "w") as handle:
        handle.write(
            "-- revision_id:TESTREV;\n-- prev_revision_id:TESTPREVREV;\nbegin\nselect 1;\nend;\n"
        )
    result = _parse_migration(str(tmp_path), "01_w_revision.sql")
    assert result.revision_id == "TESTREV"
    assert result.prev_revision_id == "TESTPREVREV"
    assert result.template_name == "01_w_revision.sql"


def test_get_ordered_migration_objs_clash():
    with pytest.raises(ValueError, match="is claimed by both"):
        get_ordered_migration_objs(
            [
                Migration(
                    revision_id="q", prev_revision_id="test10", template_name="test1"
                ),
                Migration(
                    revision_id="q", prev_revision_id="test20", template_name="test2"
                ),
            ]
        )


def test_get_ordered_migration_objs_too_many_roots():
    with pytest.raises(
        ValueError, match="expected exactly one migration with an empty"
    ):
        get_ordered_migration_objs(
            [
                Migration(revision_id="q", prev_revision_id="", template_name="test1"),
                Migration(revision_id="p", prev_revision_id="", template_name="test2"),
            ]
        )


def test_get_ordered_migration_objs_unknown_prev():
    with pytest.raises(ValueError, match="points at unknown prev_revision_id"):
        get_ordered_migration_objs(
            [
                Migration(
                    revision_id="q", prev_revision_id="unknown", template_name="test1"
                ),
                Migration(revision_id="p", prev_revision_id="", template_name="test2"),
            ]
        )


def test_get_ordered_migration_objs_parent_of_both():
    with pytest.raises(ValueError, match="is the parent of both"):
        get_ordered_migration_objs(
            [
                Migration(
                    revision_id="head", prev_revision_id="q", template_name="test500"
                ),
                Migration(
                    revision_id="another head",
                    prev_revision_id="q",
                    template_name="test700",
                ),
                Migration(revision_id="q", prev_revision_id="", template_name="test1"),
            ]
        )


def test_get_ordered_migration_objs_not_reachable_from_root():
    with pytest.raises(ValueError, match="Migrations are not reachable from the root"):
        get_ordered_migration_objs(
            [
                Migration(
                    revision_id="aaa",
                    prev_revision_id=None,
                    template_name="test500",
                ),
                Migration(revision_id="q", prev_revision_id="p", template_name="test1"),
                Migration(revision_id="p", prev_revision_id="q", template_name="test2"),
            ]
        )


def test_get_ordered_migration_objs_happy():
    result = get_ordered_migration_objs(
        [
            Migration(
                revision_id="aaa",
                prev_revision_id=None,
                template_name="test500",
            ),
            Migration(revision_id="q", prev_revision_id="aaa", template_name="test1"),
            Migration(revision_id="p", prev_revision_id="q", template_name="test2"),
        ]
    )
    assert len(result) == 3


@mock.patch(
    "src.crutch_migrations.run_crutch_migrations._get_migrations_dir",
    return_value="/i/am/a/fake/migrations/dir",
)
def test_get_migrations_list_not_isdir(get_migrations_dir):
    with pytest.raises(ValueError, match="has no migrations dir"):
        get_migrations_list(ALL_SPARK)
    get_migrations_dir.assert_called_once()


@mock.patch(
    "src.crutch_migrations.run_crutch_migrations._get_migrations_dir",
)
def test_get_migrations_list_no_migrations(get_migrations_dir, tmp_path):
    get_migrations_dir.return_value = tmp_path
    result = get_migrations_list(ALL_SPARK)
    assert result == []
    get_migrations_dir.assert_called_once()


@mock.patch(
    "src.crutch_migrations.run_crutch_migrations._get_migrations_dir",
)
@mock.patch(
    "src.crutch_migrations.run_crutch_migrations._parse_migration",
    return_value="testing",
)
@mock.patch(
    "src.crutch_migrations.run_crutch_migrations.get_ordered_migration_objs",
    return_value=["testing"],
)
def test_get_migrations_list_ignores_non_sql_files(
    get_ordered_migration_objs, parse_migration, get_migrations_dir, tmp_path
):
    get_migrations_dir.return_value = tmp_path
    (tmp_path / "01_i_am_test.sql").touch()
    (tmp_path / "02_i_am_non_sql_test.txt").touch()

    assert get_migrations_list(ALL_SPARK, migrations_dir=str(tmp_path)) == ["testing"]
    get_migrations_dir.assert_called_once()
    parse_migration.assert_called_once()
    get_ordered_migration_objs.assert_called_once()


@freeze_time("2007-07-07")
@mock.patch(
    "src.crutch_migrations.run_crutch_migrations.uuid.uuid4",
    return_value=UUID(bytes=b"1111222233334444", version=4),
)
def test_create_new_migration_long_slug(_uuid, tmp_path):
    output_path = tmp_path / f"{run_crutch_migrations.ALL_SPARK}_migrations"
    os.makedirs(output_path)
    create_new_migration(
        "a very long message that certainly runs past the slug truncation limit",
        template_path=os.path.join(
            os.path.dirname(run_crutch_migrations.__file__),
            "migration_templates",
            "default_migration_template.sql",
        ),
        output_path=output_path,
    )
    with open(
        os.path.join(
            output_path,
            "070707_a_very_long_message_that_certainly_runs_333334343434.sql",
        )
    ) as result_file:
        result_txt = result_file.read()
        assert "-- revision_id:" in result_txt


def test_apply_template(tmp_path):
    with open(tmp_path / "01_template.sql", "w") as handle:
        handle.write("\nbegin\nselect x from {{cat}}.{{schema}}.fake_table;\nend;\n")

    env = Environment(
        loader=FileSystemLoader(str(tmp_path)),
        autoescape=select_autoescape(),
    )

    apply_template(
        tmp_path, env.get_template("01_template.sql"), "FAKETESTCAT", "FAKETESTSCHEMA"
    )
    with open(tmp_path / "01_template_primed.sql") as result_file:
        result_text = result_file.read()
        assert "FAKETESTCAT.FAKETESTSCHEMA" in result_text


@freeze_time("2007-07-07 01:02:03.123456")
def test_get_ascending_letters_within_minute():
    assert get_ascending_letters_within_minute() == "BCDEFG"


@freeze_time("2007-07-07 01:02:03")
@mock.patch(
    "src.crutch_migrations.run_crutch_migrations.get_ascending_letters_within_minute",
    return_value="QQPP",
)
def test_get_output_folder(get_ascending_letters):
    assert (
        get_output_folder("/i/am/a/fake/parent")
        == "/i/am/a/fake/parent/20070707_0102_QQPP"
    )
    get_ascending_letters.assert_called_once()


def test_use_migration_name_all():
    assert use_migration_file("TESTING_all.sql")


@mock.patch("src.crutch_migrations.run_crutch_migrations.is_dbr", return_value=True)
def test_use_migration_name_dbr(is_dbr):
    assert use_migration_file("TESTING_dbr_only.sql")
    is_dbr.assert_called_once()


@mock.patch("src.crutch_migrations.run_crutch_migrations.is_dbr", return_value=False)
def test_use_migration_name_false(is_dbr):
    assert not use_migration_file("TESTING.sql")
    is_dbr.assert_called_once()


@mock.patch("src.crutch_migrations.run_crutch_migrations.migrate_w_rev")
@mock.patch("src.crutch_migrations.run_crutch_migrations.migrate_initial")
@mock.patch("src.crutch_migrations.run_crutch_migrations.is_dbr", return_value=True)
def test_run_migrations(is_dbr, migrate_initial, migrate_w_rev, tmp_path):
    output_folder = str(tmp_path / "28818989_8182_BCDEQQ")

    spark = mock.MagicMock()
    run_migrations(spark, "spark_catalog", "default", output_folder)

    assert os.path.isdir(output_folder)

    migrate_initial.assert_called_once()
    is_dbr.assert_called_once()
    assert migrate_w_rev.call_args_list == [
        mock.call(spark, mock.ANY, "spark_catalog", "default", DBR_ONLY),
        mock.call(spark, mock.ANY, "spark_catalog", "default", ALL_SPARK),
    ]


@mock.patch("src.crutch_migrations.run_crutch_migrations.run_migrations")
@mock.patch("src.crutch_migrations.run_crutch_migrations.get_spark")
def test_main(get_spark, run_migrations, tmp_path, monkeypatch):
    warehouse_dir = tmp_path / "spark-warehouse"
    os.makedirs(warehouse_dir)
    os.makedirs(tmp_path / "src" / "crutch_migrations")
    monkeypatch.setattr(
        run_crutch_migrations,
        "__file__",
        str(tmp_path / "src" / "crutch_migrations" / "run_crutch_migrations.py"),
    )

    main("spark_catalog", "default")

    assert not os.path.exists(warehouse_dir)
    get_spark.assert_called_once()
    run_migrations.assert_called_once()


@mock.patch(
    "src.crutch_migrations.run_crutch_migrations.get_migrations_list",
    return_value=["testing"],
)
def test_get_unapplied_migrations_list_no_table(get_migrations_list, test_spark):
    result = get_unapplied_migrations_list(
        test_spark, "fake", "spark_catalog", "default"
    )
    assert result == ["testing"]
    get_migrations_list.assert_called_once()


@mock.patch(
    "src.crutch_migrations.run_crutch_migrations.get_migrations_list",
    return_value=["testing"],
)
def test_get_unapplied_migrations_list_no_rows(get_migrations_list, test_spark):
    test_spark.sql(
        """
        begin
        drop table if exists spark_catalog.default._spark_migrations_version;
        create table spark_catalog.default._spark_migrations_version (
            migration_type string not null,
            version_num string not null
        );
        end;
    """
    )
    result = get_unapplied_migrations_list(
        test_spark, "fake", "spark_catalog", "default"
    )
    assert result == ["testing"]
    get_migrations_list.assert_called_once()


@mock.patch(
    "src.crutch_migrations.run_crutch_migrations.get_migrations_list",
    return_value=[
        Migration(
            revision_id="wont match", prev_revision_id=None, template_name="test1"
        )
    ],
)
def test_get_unapplied_migrations_list_no_match(get_migrations_list, test_spark):
    test_spark.sql(
        """
        begin
        drop table if exists spark_catalog.default._spark_migrations_version;
        create table spark_catalog.default._spark_migrations_version (
            migration_type string not null,
            version_num string not null
        );
        insert into spark_catalog.default._spark_migrations_version(migration_type,version_num)
        values("fake", "non matching");
        end;
    """
    )
    with pytest.raises(ValueError, match="which matches no migration in"):
        get_unapplied_migrations_list(test_spark, "fake", "spark_catalog", "default")
    get_migrations_list.assert_called_once()


@mock.patch(
    "src.crutch_migrations.run_crutch_migrations.get_migrations_list",
    return_value=[
        Migration(revision_id="55", prev_revision_id=None, template_name="test1"),
        Migration(
            revision_id="unapplied", prev_revision_id="55", template_name="test2"
        ),
    ],
)
def test_get_unapplied_migrations_list_happy(get_migrations_list, test_spark):
    test_spark.sql(
        """
        begin
        drop table if exists spark_catalog.default._spark_migrations_version;
        create table spark_catalog.default._spark_migrations_version (
            migration_type string not null,
            version_num string not null
        );
        insert into spark_catalog.default._spark_migrations_version(migration_type,version_num)
        values("fake", "55");
        end;
    """
    )
    result = get_unapplied_migrations_list(
        test_spark, "fake", "spark_catalog", "default"
    )
    assert result[0].template_name == "test2"
    get_migrations_list.assert_called_once()


@mock.patch(
    "src.crutch_migrations.run_crutch_migrations.apply_template",
    return_value="select 1",
)
@mock.patch("src.crutch_migrations.run_crutch_migrations.Environment")
def test_migrate_initial_happy(environment, apply_template, test_spark):
    environment.return_value.list_templates.return_value = ["20010909_1_all.sql"]

    migrate_initial(
        test_spark,
        "/i/am/a/fake/output/folder",
        "spark_catalog",
        "default",
        "all_spark_migrations",
    )

    environment.return_value.list_templates.assert_called_once_with(
        filter_func=use_migration_file
    )
    environment.return_value.get_template.assert_called_once_with("20010909_1_all.sql")
    apply_template.assert_called_once_with(
        "/i/am/a/fake/output/folder",
        environment.return_value.get_template.return_value,
        cat="spark_catalog",
        schema="default",
    )


def test_record_revision(test_spark):
    test_spark.sql(
        """
        begin
        drop table if exists spark_catalog.default._spark_migrations_version;
        create table spark_catalog.default._spark_migrations_version (
            migration_type string not null,
            version_num string not null
        );
        insert into spark_catalog.default._spark_migrations_version(migration_type,version_num)
        values("testing", "000111");
        end;
    """
    )
    record_revision(test_spark, "spark_catalog", "default", "testing", "revIdTesting")

    rows = test_spark.sql(
        "select migration_type, version_num from spark_catalog.default._spark_migrations_version"
    )
    dict_rows = [x.asDict() for x in rows.toLocalIterator()]
    assert dict_rows[0] == {"migration_type": "testing", "version_num": "revIdTesting"}


@mock.patch("src.crutch_migrations.run_crutch_migrations.record_revision")
@mock.patch(
    "src.crutch_migrations.run_crutch_migrations.apply_template",
    return_value="select 1",
)
@mock.patch("src.crutch_migrations.run_crutch_migrations.Environment")
@mock.patch(
    "src.crutch_migrations.run_crutch_migrations.get_unapplied_migrations_list",
    return_value=[
        Migration(revision_id="55", prev_revision_id=None, template_name="test1")
    ],
)
def test_migrate_w_rev(
    get_unapplied_migrations_list,
    environment,
    apply_template,
    record_revision,
    test_spark,
):
    migrate_w_rev(
        test_spark,
        "/i/am/a/fake/output/folder",
        "spark_catalog",
        "default",
        ALL_SPARK,
    )

    get_unapplied_migrations_list.assert_called_once()
    environment.return_value.get_template.assert_called_once()
    apply_template.assert_called_once()
    record_revision.assert_called_once()
