# CLAUDE.md

Guidance for working in this repo. **Focus: code quality.** Every change must pass the
same gates CI enforces before it is considered done.

## What this project is

A PySpark / Delta Lake harness that ingests and profiles data, plus SQL "crutch"
migrations and a Databricks Asset Bundle (DAB) deployment. Runs both locally
(open-source Spark + Delta) and on Databricks (`databricks-connect`).


## Laws of unit testing
- The unit under test should always be a single python function or method from src/.
- A unit test may not invoke its unit more than once.
- With the exception of simple getters and setters a test may not invoke any part of src/ other than its unit.
- Do not create abstract class implementations that only serve tests, use `__abstractmethods__=set()` instead.
- Always mock/patch other methods from our source that are invoked from the unit under test.
- Whenever mocking/patching is done, assert_called must verify that the expected call took place, or did not if that is what should have happenned.
- Use `test_spark` or `migrated_spark` whenever spark is is invoked in the unit under test.
- File, zip and worksheet operations in the unit under test should be tested with trivial dummy files instead of being mocked.
- The line and branch coverage of one test may not be fully overlapped by the coverage of another test. Keep the test that covers more, delete the other one.
- Every test must add unique line or branch coverage that isn't supplied by any other test.
- If the `__init__` method of the class whose method is under test does anything besides trivial assignment and call to super, it should be mocked, or the functionality moved into its own (private)method.
- If the `__init__` method and its chain of `super().__init__()` only do simple variable assignment they should not be mocked.
- Whenever feasible src/ should not be changed in order enable testing.
- Avoid negative assertions, esp `assert_not_called..`. It's hard to keep them up to date as them become irrelevant.
- Don't test raises(exceptions) that are never mentioned in the unit under test. If they would happen in the code below and pass through to the code above, they are not a concern for the test.
- Strive to cover all src with tests when possible to do so while obeying the rules above.


## Laws of integration testing
- Integration tests live under `test/integration/`, mirroring the `src/` package path below
  that, same as unit tests do under `test/`.
- Integration tests should not contribute to coverage, that should only come from unit tests.
- Integration tests should test individual tasks the src/ code might be expected to perform.
- Integration tests are free to mock or patch if using real src/ code would detract from focus.
- Integration tests always run after unit tests, as a separate `pants test` invocation, never
  in parallel with them or with each other's invocation — see the quality gates below.


## The quality gates — ask before running them

**Always ask before running `pants fmt`, `pants lint`, `pants check`, or `pants test`.**
These are slow (a full unit run plus integration is minutes of wall clock, and Spark
fixtures dominate it), so they are the user's call, not an automatic reflex after every
edit. Make the change, say what you would run to verify it, and wait for a yes. If the
user has already said to run them — in this session or in the request itself — go ahead
without asking again. Same for a scoped run the user asked for.

When you have not run them, say plainly that the change is unverified rather than
implying it passed.

Always go through Pants, never bare `python`/`pytest` (Delta needs the JVM classpath
Pants assembles; bare `pytest` fails with Delta classpath errors).

```bash
# Format + lint + typecheck (black, isort, flake8, mypy) — must be clean
pants fmt lint check src/ test/

# Run unit tests with coverage (everything under test/ except test/integration)
pants test --test-force --use-coverage test/:: -test/integration::

# Then, only after unit tests are green, run integration tests as their own
# invocation — never combined or fanned out alongside the unit run
pants test --test-force test/integration::

# Scope to one file/selector while iterating
pants test --test-force test/cms_pipeline/test_manipulator.py
pants test --test-force test/cms_pipeline/test_manipulator.py -- -k <expr>

# Build the wheel (CI does this; do it if you touched packaging)
pants package src/
```

Non-negotiable gates (from `ci.yml`):
1. `pants lint check src/ test/` is clean — this is `black`, `isort`, `flake8`, `mypy`.
2. All tests pass. Unit tests (`test/::` minus `test/integration::`) run first, with
   coverage; integration tests run afterward, as a separate `pants test` invocation, never
   in parallel with the unit run — see `ci.yml`'s "Run unit tests" / "Run integration tests"
   steps.
3. Branch coverage over `src/` stays **≥ 94%** (`fail_under = 94` under `[coverage-py]` in
   `pants.toml`). It belongs there, not in `pyproject.toml`: coverage.py's own `fail_under`
   is applied to each test partition separately, so a single test file gets failed for not
   covering the rest of `src/` on its own.
   New code needs tests; don't lower the threshold to make a change pass.
   Integration tests must not be run with `--use-coverage`; per the laws below, coverage
   should come only from unit tests.

If you can't run these, say so explicitly rather than claiming the change is verified.

## Style — match the tooling, not your preferences

- **Line length is 120.** Let `black` and `isort` do the formatting; never hand-format
  to fight them.
- Imports: sorted by `isort` with the `black` profile. Prefer top-of-file imports;
  defer an import into a function only to gate an optional/env-specific dependency
  (see the Databricks-only imports inside `get_spark`/`is_dbr`).
- **Type hints + mypy.** `mypy` runs in `check`. Third-party imports without stubs get a
  narrow `# type: ignore` (e.g. `delta`, `pyspark.dbutils`) — keep it on the specific
  import line, not blanket-ignored, and add `# noqa: F401` only when the import exists
  purely as a capability probe.
- **Logging, not prints.** Configure via `spark_sql_migrations.custom_logging`; get a module logger
  and log at appropriate levels.
- Keep functions small and single-purpose; prefer pure helpers that are unit-testable
  without a Spark session where possible.

## Spark / Delta conventions

- **Get a session only through `get_spark()`** (`spark_sql_migrations.spark_utils`). It transparently
  returns a `DatabricksSession` on DBR and a Delta-configured local `SparkSession`
  otherwise. Don't build `SparkSession.builder` ad hoc elsewhere.
- Local storage locations come from `SPARK_WAREHOUSE_DIR` / `SPARK_METASTORE_DIR` env
  vars. Tests isolate these per-session (see `test/conftest.py`); never hardcode a
  warehouse/metastore path in code or tests.
- Code must work in **both** modes (local OSS Spark and Databricks). Gate DBR-only
  behaviour behind `is_dbr()`, and gate DBR-only imports inside the function that uses them.

## SQL migrations (`src/crutch_migrations/`)

The migration **engine** lives in the `spark_sql_migrations` library, not here. This directory holds
only what this project owns: the `all_spark_migrations/` and `dbr_only_migrations/`
chains, plus `run_crutch_migrations.py`, a thin wrapper that tells spark_sql_migrations where they
are. The initial bootstrap chain and the new-migration template ship inside the spark_sql_migrations
wheel — don't recreate them here.

- Migration files are named `YYYYMMDD_N_<slug>_<revision_id>.sql`, and the chain each one
  belongs to is its directory:
  - `all_spark_migrations/` — runs everywhere (local + Databricks),
  - `dbr_only_migrations/` — Databricks-only
  Pick the chain deliberately; SQL that only one engine supports must not be in
  `all_spark_migrations/`. Each file carries `revision_id` / `prev_revision_id` headers
  forming a single chain; `pants run src/crutch_migrations/run_crutch_migrations.py:lib --
  create_new_migration --message="..."` writes a correctly-headed one.
- **Migrations must be idempotent.** The test harness runs them **twice** on purpose
  (`migrated_spark` in `conftest.py`). Note that the version table gates the second pass,
  so that double-run proves the *initial* chain is idempotent but not these ones — a
  replay only happens if the version row is cleared. Databricks does **not** support
  `IF NOT EXISTS` on `ALTER TABLE ... ADD COLUMN` — guard idempotent column adds with a
  SQLSTATE 42710 exit handler (see `260831_03_batch_id_for_metrics_table_*.sql`).

## Pants / BUILD discipline

- Every new source directory needs a `BUILD` file. Library code uses
  `python_sources(name="lib")`; the parallel `python_sources(name="lib_test", resolve="py-reqs-dev")`
  target exposes the same code to the test resolve.
- Two resolves exist: `python-default` (src) and `py-reqs-dev` (test). Add runtime deps to
  `src/requirements.txt`, test-only deps to `test/requirements-dev.txt`, then regenerate:
  `pants generate-lockfiles`. Don't edit lockfiles by hand.
- `spark_sql_migrations` declares **no** Spark of its own, so the extra is what pulls one in:
  `spark_sql_migrations[databricks]` in `src/requirements.txt`, `spark_sql_migrations[local]` in
  `test/requirements-dev.txt`. databricks-connect ships its own top-level `pyspark/` and
  `delta/`, so the two flavours can never be installed together. The explicit
  `databricks-connect` / `pyspark` / `delta-spark` pins stay alongside them so this repo,
  not the extra's version range, decides which Spark is used — the Spark Connect test
  image is pinned to a matching version.
- Console entry points and the wheel are defined in `src/BUILD` (`python_distribution`).

## Testing standards

- Tests live under `test/`, mirroring the `src/` package path, with a `BUILD` per dir.
- Use the shared fixtures in `test/conftest.py` (`test_spark`, `migrated_spark`)
  rather than spinning up Spark yourself.
- A change to `src/` without a corresponding test is incomplete — coverage is gated and
  CI also enforces a minimum test count.

## Before you finish a change

Make sure new/changed behaviour has tests, and that nothing hardcodes environment-specific
paths, credentials, warehouse ids, or catalogs. Then offer to run the quality gates, in
order — don't run them unasked.
