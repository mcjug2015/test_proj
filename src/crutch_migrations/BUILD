python_sources(
    name="lib",
    dependencies=[
        ":all_spark_migrations",
        ":dbr_only_migrations",
    ],
    # create_new_migration() writes a new .sql file into all_spark_migrations/
    # beside this package; sandboxed `pants run` would write it into a throwaway
    # chroot copy instead of the real migrations dir (same reason Django's
    # makemigrations needs this off).
    run_goal_use_sandbox=False,
)

python_sources(
    name="lib_test",
    dependencies=[
        ":all_spark_migrations",
        ":dbr_only_migrations",
    ],
    resolve="py-reqs-dev",
)

# Only the project-owned chains live here now. spark_sql_migrations ships migrations_initial/
# and migration_templates/ inside its own wheel.
resources(
    name="all_spark_migrations",
    sources=["all_spark_migrations/*.sql"],
)

resources(
    name="dbr_only_migrations",
    sources=["dbr_only_migrations/*.sql"],
)
