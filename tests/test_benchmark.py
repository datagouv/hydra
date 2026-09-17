"""Slow end-to-end performance benchmarks for Hydra.

Excluded from default CI via ``pytest -m "not slow"``. Run manually or via the
CircleCI ``benchmark-workflow`` (pipeline parameter ``run-benchmarks=true``).
"""

import csv
import gzip
import hashlib
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from unittest.mock import patch

import pytest

from udata_hydra.analysis.helpers import download_from_check
from udata_hydra.data_formats import Csv, Geojson
from udata_hydra.db.resource import Resource

pytestmark = pytest.mark.asyncio

BENCHMARK_DIR = Path(".benchmarks")
BENCHMARK_HISTORY = BENCHMARK_DIR / "benchmarks.history.csv"
BENCHMARK_CSV = BENCHMARK_DIR / "benchmarks.csv"
CSV_HEADERS = [
    "datetime",
    "test_name",
    "input_file",
    "ci",
    "execution_time_seconds",
    "commit_author",
    "commit_id",
    "runner_class",
    "runner_cpu",
    "runner_memory",
    "python_version",
]

ANNUAIRE_CSV = "20190618-annuaire-diagnostiqueurs.csv"
GEO_CSV_GZ = Path("tests/data/H_01_1990-1999.csv.gz")
GEO_CSV = Path("tests/data/H_01_1990-1999.csv")
GEO_INPUT_LABEL = "H_01_1990-1999.csv.gz"

_benchmark_state: dict[str, Geojson] = {}


def _runner_cpu() -> str:
    if cpu := os.environ.get("BENCHMARK_RUNNER_CPU"):
        return cpu
    return str(os.cpu_count() or "")


def _runner_memory_mb() -> str:
    if memory := os.environ.get("BENCHMARK_RUNNER_MEMORY_MB"):
        return memory
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        if pages > 0 and page_size > 0:
            return str((pages * page_size) // (1024 * 1024))
    except (AttributeError, OSError, ValueError):
        pass
    return ""


def _ensure_benchmark_csv() -> None:
    BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)
    if not BENCHMARK_CSV.exists() and BENCHMARK_HISTORY.exists():
        shutil.copy(BENCHMARK_HISTORY, BENCHMARK_CSV)
    elif not BENCHMARK_CSV.exists():
        with BENCHMARK_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()


def _append_benchmark_row(
    *,
    test_name: str,
    input_file: str,
    execution_time_seconds: float,
) -> None:
    _ensure_benchmark_csv()
    with BENCHMARK_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writerow(
            {
                "datetime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "test_name": test_name,
                "input_file": input_file,
                "ci": os.environ.get("BENCHMARK_CI", "local"),
                "execution_time_seconds": round(execution_time_seconds, 2),
                "commit_author": os.environ.get("CIRCLE_USERNAME", ""),
                "commit_id": os.environ.get("CIRCLE_SHA1", "")[:7],
                "runner_class": os.environ.get("BENCHMARK_RUNNER_CLASS", ""),
                "runner_cpu": _runner_cpu(),
                "runner_memory": _runner_memory_mb(),
                "python_version": os.environ.get(
                    "BENCHMARK_PYTHON_VERSION",
                    f"{sys.version_info.major}.{sys.version_info.minor}",
                ),
            }
        )


@pytest.fixture(scope="module")
def geo_big_csv_path() -> Path:
    if not GEO_CSV.exists():
        with gzip.open(GEO_CSV_GZ, "rb") as src, GEO_CSV.open("wb") as dst:
            shutil.copyfileobj(src, dst)
    return GEO_CSV


@pytest.mark.slow
async def test_analyse_csv_big_file(setup_catalog, rmock, db, fake_check, produce_mock):
    """Parse a large CSV end-to-end (download, csv-detective, DB insert)."""
    test_name = "test_analyse_csv_big_file"
    expected_count = 45522

    check = await fake_check(headers={"content-type": "text/csv"})
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()

    csv_path = Path("tests/data") / ANNUAIRE_CSV
    with csv_path.open("rb") as f:
        data = f.read()
    rmock.get(url, status=200, body=data)

    resource = await Resource.get(check["resource_id"])
    assert resource is not None
    assert resource["status"] is None

    start = perf_counter()
    file = await download_from_check(check, Csv)
    await file.analyse(check=check)
    duration = perf_counter() - start

    resource = await Resource.get(check["resource_id"])
    assert resource is not None
    assert resource["status"] is None

    count = await db.fetchrow(f'SELECT count(*) AS count FROM "{table_name}"')
    assert count["count"] == expected_count
    profile = await db.fetchrow(
        "SELECT csv_detective FROM tables_index WHERE resource_id = $1", check["resource_id"]
    )
    profile = json.loads(profile["csv_detective"])
    for attr in ("header", "columns", "formats", "profile"):
        assert profile[attr]
    assert profile["total_lines"] == expected_count

    _append_benchmark_row(
        test_name=test_name,
        input_file=ANNUAIRE_CSV,
        execution_time_seconds=duration,
    )
    print(f"{test_name}: {duration:.2f}s")


@pytest.mark.slow
async def test_csv_to_geojson_big_file(geo_big_csv_path: Path):
    """Convert a large geographical CSV to GeoJSON."""
    test_name = "test_csv_to_geojson_big_file"
    csv_file = Csv(file_name=geo_big_csv_path.as_posix())

    start = perf_counter()
    await csv_file.inspect()
    geojson = await csv_file.to_geojson()
    duration = perf_counter() - start

    assert geojson is not None
    with geojson.path.open(encoding="utf-8") as f:
        geojson_data = json.load(f)
    assert geojson_data["type"] == "FeatureCollection"
    assert len(geojson_data["features"]) > 0
    assert geojson.filesize > 1_000_000

    _benchmark_state["geojson"] = geojson
    _append_benchmark_row(
        test_name=test_name,
        input_file=GEO_INPUT_LABEL,
        execution_time_seconds=duration,
    )
    print(f"{test_name}: {duration:.2f}s, features={len(geojson_data['features'])}")


@pytest.mark.slow
async def test_geojson_to_pmtiles_big_file():
    """Convert a large GeoJSON file to PMTiles."""
    test_name = "test_geojson_to_pmtiles_big_file"
    geojson = _benchmark_state.get("geojson")
    if geojson is None:
        pytest.skip(reason="Run test_csv_to_geojson_big_file first")

    with patch("udata_hydra.config.REMOVE_GENERATED_FILES", False):
        start = perf_counter()
        pmtiles_file = await geojson.to_pmtiles()
        duration = perf_counter() - start

    with pmtiles_file.path.open("rb") as f:
        header = f.read(7)
    assert header == b"PMTiles"
    assert pmtiles_file.filesize > 5000

    _append_benchmark_row(
        test_name=test_name,
        input_file=GEO_INPUT_LABEL,
        execution_time_seconds=duration,
    )
    print(f"{test_name}: {duration:.2f}s, pmtiles_size={pmtiles_file.filesize}")

    pmtiles_file.path.unlink(missing_ok=True)
