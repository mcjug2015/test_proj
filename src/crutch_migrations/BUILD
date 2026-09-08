python_sources(
    name="lib",
    dependencies=[
        ":all_spark_migrations",
        ":dbr_only_migrations",
        ":migration_templates",
        ":migrations_initial",
    ],
    # create_new_migration() writes a new .sql file relative to this file's own
    # location; sandboxed `pants run` would write it into a throwaway chroot copy
    # instead of the real migrations/ dir (same reason Django's makemigrations
    # needs this off).
    run_goal_use_sandbox=False,
)

python_sources(
    name="lib_test",
    dependencies=[
        ":all_spark_migrations",
        ":dbr_only_migrations",
        ":migration_templates",
        ":migrations_initial",
    ],
    resolve="py-reqs-dev",
)

resources(
    name="all_spark_migrations",
    sources=["all_spark_migrations/*.sql"],
)

resources(
    name="dbr_only_migrations",
    sources=["dbr_only_migrations/*.sql"],
)

resources(
    name="migration_templates",
    sources=["migration_templates/*.sql"],
)

resources(
    name="migrations_initial",
    sources=["migrations_initial/*.sql"],
)
