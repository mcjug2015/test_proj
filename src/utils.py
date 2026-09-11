import os
import re
from urllib.parse import urlparse

from pyspark.sql.session import SparkSession
from spark_sql_migrations import custom_logging

logger = custom_logging.setup_logging().getLogger(__name__)


def convert_to_key(value: str) -> str:
    key = value.lower()
    key = re.sub(r"medicare\s+advantage", "ma", key)
    key = key.replace("medicare", "me")
    key = key.replace("year", "yr")
    key = key.replace("total", "tot")
    key = key.replace("enrollment", "enroll")
    key = key.replace("original", "orig")
    key = key.replace("percentage", "pct")
    key = key.replace("without", "wo")
    key = key.replace("count", "ct")
    key = key.replace("/", "_")
    return re.sub(r"\s+", "_", key)


def download_s3_zip(spark: SparkSession, s3_uri: str, dest_dir: str) -> str:
    parsed = urlparse(s3_uri)
    dest_path = os.path.join(dest_dir, os.path.basename(parsed.path))
    logger.info(f"downloading {s3_uri} -> {dest_path}")
    row = spark.read.format("binaryFile").load(s3_uri).select("content").first()
    assert row is not None
    with open(dest_path, "wb") as f:
        f.write(row["content"])
    return dest_path
