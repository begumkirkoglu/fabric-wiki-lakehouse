import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "producer"))
sys.path.insert(0, str(ROOT / "fabric" / "lib" / "src"))
FIXTURES = ROOT / "tests" / "fixtures"


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # workers use the same interpreter
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
