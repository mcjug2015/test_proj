import os
from unittest import mock

import pytest  # type: ignore
from spark_sql_migrations import custom_logging

from src.cms_pipeline.unwrapper import Unwrapper

logger = custom_logging.setup_logging().getLogger(__name__)
RES_DIR = os.path.join(os.path.dirname(__file__), "res")


@mock.patch("src.cms_pipeline.unwrapper.Unwrapper.find_target", return_value="test123")
def test_unwrap_success(find_target):
    unwrapper = Unwrapper()
    with unwrapper.unwrap(None) as fake_target:
        assert fake_target == "test123"
        assert os.path.exists(find_target.call_args.args[1])
    find_target.assert_called_once()


@mock.patch(
    "src.cms_pipeline.unwrapper.Unwrapper.find_target",
)
def test_unwrap_except(find_target):
    find_target.side_effect = ValueError("test error 123")
    unwrapper = Unwrapper()
    with pytest.raises(ValueError, match="test error 123"):
        with unwrapper.unwrap(None):
            pass
    find_target.assert_called_once()
    # make sure the finally clause cleaned up the temp dir
    assert not os.path.exists(find_target.call_args.args[1])


def test_find_target_recurses_into_nested_zip_and_subdirs(tmp_path):
    # nested.zip holds wrap/inner.zip, which holds deep/sub/TARGET.xlsx; matching is by
    # bare filename, so neither the nesting nor the enclosing subdirs may matter.
    unwrapper = Unwrapper()

    found = unwrapper.find_target(os.path.join(RES_DIR, "nested.zip"), str(tmp_path))

    assert os.path.basename(found) == "TARGET.xlsx"
    with open(found, "rb") as fh:
        assert fh.read() == b"payload-nested"


def test_find_target_raises_when_no_entry_matches(tmp_path):
    # no_target.zip holds a single readme.txt: neither the target nor a nested zip.
    unwrapper = Unwrapper()

    with pytest.raises(FileNotFoundError, match="target file not found within"):
        unwrapper.find_target(os.path.join(RES_DIR, "no_target.zip"), str(tmp_path))
