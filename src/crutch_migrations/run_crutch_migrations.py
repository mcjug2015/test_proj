"""
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYTHONPATH"] = ":".join(sys.path)
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64/"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
"""

import argparse
import datetime
import os
import re
import shutil
import uuid
from dataclasses import dataclass

from jinja2 import Environment, PackageLoader, select_autoescape
from pyspark.sql.functions import col

from src import custom_logging
from src.spark_utils import get_spark, is_dbr

logger = custom_logging.setup_logging().getLogger(__name__)


ALL_SPARK = "all_spark"
DBR_ONLY = "dbr_only"

VERSION_TABLE = "_spark_migrations_version"

_HEADER_RE = re.compile(r"^--\s*(revision_id|prev_revision_id)\s*:\s*(\w*)\s*;$")


@dataclass(frozen=True)
class Migration:
    revision_id: str
    prev_revision_id: str | None
    template_name: str


def _get_migrations_dir(migration_type, migrations_dir=None):
    if migrations_dir:
        return migrations_dir
    return os.path.join(os.path.dirname(__file__), f"{migration_type}_migrations")


def _parse_migration(migrations_dir, template_name):
    headers = {}
    migration_file_path = os.path.join(migrations_dir, template_name)
    if os.path.getsize(migration_file_path) == 0:
        raise ValueError(f"{template_name} is empty;")
    with open(migration_file_path) as migration_file:
        for line in migration_file:
            line = line.strip()
            match = _HEADER_RE.match(line)
            if match:
                headers[match.group(1)] = match.group(2) or None
            elif line and not line.startswith("--"):
                break

    if not headers.get("revision_id"):
        raise ValueError(f"{template_name} has no revision_id header;")

    return Migration(
        revision_id=headers["revision_id"],
        prev_revision_id=headers.get("prev_revision_id"),
        template_name=template_name,
    )


def get_ordered_migration_objs(migration_objs):
    by_revision = {}
    for migration in migration_objs:
        clash = by_revision.get(migration.revision_id)
        if clash:
            raise ValueError(
                f"revision_id {migration.revision_id} is claimed by both "
                f"{clash.template_name} and {migration.template_name};"
            )
        by_revision[migration.revision_id] = migration

    roots = [
        migration for migration in migration_objs if not migration.prev_revision_id
    ]
    if len(roots) != 1:
        raise ValueError(
            f"expected exactly one migration with an empty "
            f"prev_revision_id, found {[m.template_name for m in roots]};"
        )

    next_by_revision = {}
    for migration in migration_objs:
        if not migration.prev_revision_id:
            continue
        if migration.prev_revision_id not in by_revision:
            raise ValueError(
                f"{migration.template_name} points at unknown prev_revision_id "
                f"{migration.prev_revision_id};"
            )
        sibling = next_by_revision.get(migration.prev_revision_id)
        if sibling:
            raise ValueError(
                f"revision_id {migration.prev_revision_id} is the parent of both "
                f"{sibling.template_name} and {migration.template_name};"
            )
        next_by_revision[migration.prev_revision_id] = migration

    ordered = []
    current = roots[0]
    while current:
        ordered.append(current)
        current = next_by_revision.get(current.revision_id)

    if len(ordered) != len(migration_objs):
        walked = {migration.template_name for migration in ordered}
        raise ValueError(
            f"Migrations are not reachable from the root: "
            f"{sorted(m.template_name for m in migration_objs if m.template_name not in walked)};"
        )

    return ordered


def get_migrations_list(migration_type, migrations_dir=None):
    """every migration of one type, walked from its root to its head."""
    migrations_dir = _get_migrations_dir(migration_type, migrations_dir)
    if not os.path.isdir(migrations_dir):
        raise ValueError(f"{migration_type} has no migrations dir at {migrations_dir};")

    migrations = [
        _parse_migration(migrations_dir, template_name)
        for template_name in sorted(os.listdir(migrations_dir))
        if template_name.endswith(".sql")
    ]
    if not migrations:
        return []

    return get_ordered_migration_objs(migrations)


def get_unapplied_migrations_list(
    spark, migration_type, cat: str, schema: str, migrations_dir=None
):
    """the tail of one chain after the revision VERSION_TABLE records for it.

    the whole chain when the table is missing or holds no row for this type.
    """
    ordered = get_migrations_list(migration_type, migrations_dir=migrations_dir)

    version_table = f"{cat}.{schema}.{VERSION_TABLE}"
    if not spark.catalog.tableExists(version_table):
        return ordered

    current = (
        spark.read.table(version_table)
        .where(col("migration_type") == migration_type)
        .select("version_num")
        .head()
    )
    if not current:
        return ordered

    current_revision_id = current["version_num"]

    for index, migration in enumerate(ordered):
        if migration.revision_id == current_revision_id:
            return ordered[index + 1 :]

    raise ValueError(
        f"{VERSION_TABLE} has {migration_type} at {current_revision_id}, which matches "
        f"no migration in {_get_migrations_dir(migration_type, migrations_dir)};"
    )


def create_new_migration(
    message: str,
    template_path: str,
    output_path: str,
    truncate_slug_length: int = 40,
):
    """
    TODO XXX start looking up prev revision id from db or files and including it
    """
    rev_id = uuid.uuid4().hex[-12:]

    date_prefix = datetime.datetime.today().strftime("%y%m%d")

    slug = "_".join(re.split(r"\W+", message.lower())).strip("_")
    if len(slug) > truncate_slug_length:
        slug = slug[:truncate_slug_length].rsplit("_", 1)[0]

    migration_filename = f"{date_prefix}_{slug}_{rev_id}.sql"

    migration_path = os.path.join(output_path, migration_filename)

    default_migration_content = open(template_path).read()
    default_migration_content = default_migration_content.replace(
        "{{revision_id}}", rev_id
    )

    with open(migration_path, "w") as migration_file:
        migration_file.write(default_migration_content)


def apply_template(output_dir, template, cat: str, schema: str):
    result_sql = template.render(cat=cat, schema=schema)
    with open(
        os.path.join(output_dir, template.name.replace(".sql", "_primed.sql")), "w"
    ) as file_handle:
        file_handle.write(result_sql)
    return result_sql


def get_ascending_letters_within_minute():
    micros_since_minute = datetime.datetime.now() - datetime.datetime.now().replace(
        second=0, microsecond=0
    )
    result = str(micros_since_minute.microseconds).translate(
        str.maketrans("0123456789", "ABCDEFGHIJ")
    )
    return result


def get_output_folder(output_parent_path):
    folder_name = f"{datetime.datetime.today().strftime('%Y%m%d_%H%M')}_{get_ascending_letters_within_minute()}"
    return os.path.join(output_parent_path, folder_name)


def use_migration_file(fname):
    if fname.endswith("all.sql"):
        return True
    elif is_dbr() and fname.endswith("dbr_only.sql"):
        return True
    return False


def migrate_initial(
    spark,
    output_folder,
    cat: str,
    schema: str,
    package_path: str,
):
    env = Environment(
        loader=PackageLoader(
            package_name="src.crutch_migrations", package_path=package_path
        ),
        autoescape=select_autoescape(),
    )
    all_templates = env.list_templates(filter_func=use_migration_file)

    logger.info(
        f"found {len(all_templates)} migrations; first five are {all_templates[:5]};"
    )
    for template_name in all_templates:
        result_sql = apply_template(
            output_folder,
            env.get_template(template_name),
            cat=cat,
            schema=schema,
        )
        spark.sql(result_sql)
    logger.info(f"invoked spark on {len(all_templates)} migrations;")


def record_revision(
    spark, cat: str, schema: str, migration_type: str, revision_id: str
):
    """point VERSION_TABLE's row for one migration type at revision_id.

    delete + insert rather than merge: there is one row per type, and a replay
    after a crash between the two statements is harmless because migrations are
    idempotent. the values are inlined rather than passed as `args` because
    delta's DELETE does not bind named parameters.
    """
    version_table = f"{cat}.{schema}.{VERSION_TABLE}"
    type_literal = migration_type.replace("'", "''")
    revision_literal = revision_id.replace("'", "''")
    spark.sql(f"delete from {version_table} where migration_type = '{type_literal}'")
    spark.sql(
        f"insert into {version_table} (migration_type, version_num) "
        f"values ('{type_literal}', '{revision_literal}')"
    )


def migrate_w_rev(
    spark,
    output_folder,
    cat: str,
    schema: str,
    migration_type: str,
):
    """apply the migrations of one chain that VERSION_TABLE hasn't recorded yet."""
    unapplied = get_unapplied_migrations_list(
        spark,
        migration_type,
        cat=cat,
        schema=schema,
    )

    logger.info(
        f"{migration_type} has {len(unapplied)} unapplied migrations; "
        f"first five are {[migration.template_name for migration in unapplied[:5]]};"
    )

    env = Environment(
        loader=PackageLoader(
            package_name="src.crutch_migrations",
            package_path=f"{migration_type}_migrations",
        ),
        autoescape=select_autoescape(),
    )
    for migration in unapplied:
        result_sql = apply_template(
            output_folder,
            env.get_template(migration.template_name),
            cat=cat,
            schema=schema,
        )
        spark.sql(result_sql)
        record_revision(spark, cat, schema, migration_type, migration.revision_id)
        logger.info(
            f"applied {migration.template_name}; {migration_type} is now at "
            f"{migration.revision_id};"
        )


def run_migrations(spark, cat, schema, output_folder):
    initial_output_folder = os.path.join(output_folder, "migrations_initial")
    dbr_ouput_folder = os.path.join(output_folder, f"{DBR_ONLY}_migrations")
    all_spark_ouput_folder = os.path.join(output_folder, f"{ALL_SPARK}_migrations")
    os.makedirs(initial_output_folder, exist_ok=True)
    os.makedirs(dbr_ouput_folder, exist_ok=True)
    os.makedirs(all_spark_ouput_folder, exist_ok=True)
    migrate_initial(
        spark,
        initial_output_folder,
        cat,
        schema,
        "migrations_initial",
    )
    if is_dbr():
        migrate_w_rev(spark, dbr_ouput_folder, cat, schema, DBR_ONLY)
    migrate_w_rev(spark, all_spark_ouput_folder, cat, schema, ALL_SPARK)


def main(cat, schema):
    shutil.rmtree(
        os.path.join(os.path.dirname(__file__), "..", "..", "spark-warehouse"),
        ignore_errors=True,
    )
    output_folder = get_output_folder(
        os.path.join(
            os.path.dirname(__file__),
            f"migrations_out_{datetime.datetime.today().strftime('%Y%m%d_%H%M')}"
            f"_{get_ascending_letters_within_minute()}",
        )
    )
    os.makedirs(output_folder)
    run_migrations(get_spark(), cat, schema, output_folder)


def _cli_main(cat, schema):  # pragma: no cover
    main(cat, schema)


def _cli_create_new_migration(message):  # pragma: no cover
    create_new_migration(
        message,
        template_path=os.path.join(
            os.path.dirname(__file__),
            "migration_templates",
            "default_migration_template.sql",
        ),
        output_path=os.path.join(
            os.path.dirname(__file__),
            "all_spark_migrations",
        ),
    )


def build_parser():  # pragma: no cover
    parser = argparse.ArgumentParser(description="crutch migration utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_run = subparsers.add_parser("run", help="apply all pending migrations")
    p_run.add_argument("--cat", default="spark_catalog")
    p_run.add_argument("--schema", default="default")
    p_run.set_defaults(func=_cli_main)

    p_create = subparsers.add_parser(
        "create_new_migration", help="create new crutch migration"
    )
    p_create.add_argument("--message", required=True)
    p_create.set_defaults(func=_cli_create_new_migration)

    return parser


if __name__ == "__main__":  # pragma: no cover
    args = build_parser().parse_args()
    kwargs = vars(args)
    func = kwargs.pop("func")
    kwargs.pop("command")
    func(**kwargs)
