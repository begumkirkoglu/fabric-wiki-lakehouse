from datetime import datetime

import pytest

pytest.importorskip("pyspark")
pd = pytest.importorskip("pandas")

from wikilake import nyc_taxi  # noqa: E402


def test_months():
    assert nyc_taxi.months("2023-11", "2024-02") == ["2023-11", "2023-12", "2024-01", "2024-02"]
    assert nyc_taxi.months("2024-05", "2024-05") == ["2024-05"]


def _file(tmp_path, name, df):
    p = tmp_path / name
    df.to_parquet(p, index=False)
    return str(p)


def test_normalize_handles_schema_drift(spark, tmp_path):
    # older file style: capitalized names, passenger_count as float, no airport fee
    old = pd.DataFrame({
        "VendorID": [1, 2, 2],
        "tpep_pickup_datetime": pd.to_datetime(["2019-03-01 10:00", "2019-03-31 23:59", "2019-02-28 23:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2019-03-01 10:20", "2019-04-01 00:10", "2019-02-28 23:30"]),
        "passenger_count": [1.0, 2.0, None], "trip_distance": [1.2, 5.0, 0.4], "RatecodeID": [1.0, 1.0, 1.0],
        "store_and_fwd_flag": ["N", "N", "Y"], "PULocationID": [132, 48, 1], "DOLocationID": [48, 132, 2],
        "payment_type": [1, 2, 1], "fare_amount": [10.0, 20.0, 3.0], "extra": [0.5] * 3, "mta_tax": [0.5] * 3,
        "tip_amount": [2.0, 0.0, 0.0], "tolls_amount": [0.0] * 3, "improvement_surcharge": [0.3] * 3,
        "total_amount": [13.3, 21.3, 4.3], "congestion_surcharge": [2.5, 2.5, None],
    })
    new = old.rename(columns={"VendorID": "vendorid"}).assign(passenger_count=[1, 2, 1], Airport_fee=[0.0, 1.75, 0.0])
    a = nyc_taxi.normalize(spark.read.parquet(_file(tmp_path, "a.parquet", old)), "2019-03")
    b = nyc_taxi.normalize(spark.read.parquet(_file(tmp_path, "b.parquet", new)), "2019-03")
    assert a.schema == b.schema                       # one canonical schema for both eras
    assert dict(a.dtypes)["passenger_count"] == "int"
    assert a.count() == 2                             # the February stray row is dropped
    assert a.where("airport_fee IS NOT NULL").count() == 0 and b.where("airport_fee = 1.75").count() == 1
    assert {r.pickup_month for r in a.collect()} == {"2019-03"}


class FakeResp:
    def __init__(self, chunks): self.chunks = chunks
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def raise_for_status(self): pass
    def iter_content(self, chunk_size): yield from self.chunks


class FakeHttp:
    def __init__(self): self.urls = []
    def get(self, url, stream=True, timeout=None):
        self.urls.append(url)
        return FakeResp([b"abc", b"def"])


def test_download_streams_to_file(tmp_path):
    http = FakeHttp()
    path, size = nyc_taxi.download("2024-01", str(tmp_path), session=http)
    assert http.urls == ["https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"]
    assert size == 6 and open(path, "rb").read() == b"abcdef"
    assert not (tmp_path / "yellow_tripdata_2024-01.parquet.part").exists()
