#!/usr/bin/env bash
set -e

USE_DOCKER_SPARK=0
for arg in "$@"; do
  case "$arg" in
    --docker-spark)
      USE_DOCKER_SPARK=1
      ;;
    *)
      echo "Usage: $0 [--docker-spark]" >&2
      exit 1
      ;;
  esac
done

pants generate-lockfiles
pants lint check src/:: test/::

mkdir -p "$HOME/.cache/pytest-tmp"
export PYTEST_TMP_BASE="$HOME/.cache/pytest-tmp"

source "$(dirname "${BASH_SOURCE[0]}")/detect_spark_connect.sh"

# --docker-spark: if detect_spark_connect.sh didn't find one already running,
# build and start one ourselves (see start_spark_connect.sh) instead of
# falling back to local embedded Spark.
if [ "$WHICH_SPARK" = "local" ] && [ "$USE_DOCKER_SPARK" -eq 1 ]; then
  source "$(dirname "${BASH_SOURCE[0]}")/start_spark_connect.sh"
fi

# Unit tests run first, with coverage. Integration tests run afterward, as their own
# invocation, never in parallel with the unit run, and are excluded from --use-coverage —
# coverage should come only from unit tests (see CLAUDE.md "Laws of integration testing").
pants test --output=all --test-force --use-coverage --report test/:: -test/integration::
pants test --output=all --test-force --report test/integration::