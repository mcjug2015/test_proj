"""cms_pipeline's entry point into spark_sql_migrations's migration engine.

spark_sql_migrations owns the engine, the initial chain and the default migration
template. This module owns the one thing the library cannot know: where
cms_pipeline keeps its own all_spark_migrations/ and dbr_only_migrations/
chains, which is right here beside this file.
"""

import argparse
import os

from spark_sql_migrations import custom_logging
from spark_sql_migrations.spark_sql.spark_sql import (
    ALL_SPARK,
    create_new_migration,
    get_default_template_path,
    main,
)

logger = custom_logging.setup_logging().getLogger(__name__)


def get_migrations_dir():
    """the parent of this project's own migration chains."""
    return os.path.dirname(__file__)


def run(cat, schema):
    logger.info(f"applying migrations from {get_migrations_dir()} to {cat}.{schema};")
    main(cat, schema, get_migrations_dir())


def create_migration(message):
    create_new_migration(
        message,
        get_default_template_path(),
        os.path.join(get_migrations_dir(), f"{ALL_SPARK}_migrations"),
    )


def build_parser():  # pragma: no cover
    parser = argparse.ArgumentParser(description="crutch migration utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_run = subparsers.add_parser("run", help="apply all pending migrations")
    p_run.add_argument("--cat", default="spark_catalog")
    p_run.add_argument("--schema", default="default")
    p_run.set_defaults(func=run)

    p_create = subparsers.add_parser(
        "create_new_migration", help="create new crutch migration"
    )
    p_create.add_argument("--message", required=True)
    p_create.set_defaults(func=create_migration)

    return parser


if __name__ == "__main__":  # pragma: no cover
    args = build_parser().parse_args()
    kwargs = vars(args)
    func = kwargs.pop("func")
    kwargs.pop("command")
    func(**kwargs)
