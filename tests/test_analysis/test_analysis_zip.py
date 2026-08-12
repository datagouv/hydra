import hashlib
import zipfile
from io import BytesIO
from pathlib import Path

import pytest
from asyncpg.exceptions import UndefinedTableError

from tests.conftest import RESOURCE_ID, SIMPLE_CSV_CONTENT
from udata_hydra.analysis.helpers import download_from_check
from udata_hydra.analysis.resource import analyse_resource
from udata_hydra.data_formats import Csv, Csvgz, Zip
from udata_hydra.data_formats.detect import detect_data_format_from_check_or_catalog
from udata_hydra.db.resource import Resource
from udata_hydra.utils import storage_path

pytestmark = pytest.mark.asyncio

CSV_CONTENT: bytes = SIMPLE_CSV_CONTENT.encode("utf-8")

# DVF, the resource this format was built for, ships its data as a pipe separated `.txt`
DVF_LIKE_CONTENT: bytes = "\n".join(
    ["Identifiant de document|Date mutation|Valeur fonciere|Commune"]
    + [f"{i:06d}|07/01/2025|{100000 + i * 1000},00|FARGES" for i in range(20)]
).encode("utf-8")


def make_zip(members: dict[str, bytes]) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    return buffer.getvalue()


async def analyse_zip(check: dict, rmock, body: bytes) -> None:
    rmock.get(check["url"], status=200, body=body)
    file = await download_from_check(check, Zip)
    await file.analyse(check=check)


async def assert_analysed(db, check: dict, expected_lines: int) -> None:
    table_name: str = hashlib.md5(check["url"].encode("utf-8")).hexdigest()
    rows = list(await db.fetch(f'SELECT * FROM "{table_name}"'))
    assert len(rows) == expected_lines
    res = await db.fetchrow("SELECT * FROM checks")
    assert res["parsing_error"] is None
    assert res["parsing_table"] == table_name


async def assert_analysis_failed(db, check: dict, error_fragment: str) -> None:
    res = await db.fetchrow("SELECT * FROM checks")
    assert res["parsing_error"].startswith("zip_extraction:")
    assert error_fragment in res["parsing_error"]
    assert res["parsing_table"] is None

    # the resource must not stay stuck in an analysis status
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] is None

    table_name: str = hashlib.md5(check["url"].encode("utf-8")).hexdigest()
    with pytest.raises(UndefinedTableError):
        await db.fetch(f'SELECT * FROM "{table_name}"')


async def test_analyse_zip_with_single_csv(setup_catalog, rmock, db, fake_check, produce_mock):
    check = await fake_check(headers={"content-type": "application/zip"})
    await analyse_zip(check, rmock, make_zip({"data.csv": CSV_CONTENT}))
    await assert_analysed(db, check, expected_lines=2)


async def test_analyse_zip_with_single_txt(setup_catalog, rmock, db, fake_check, produce_mock):
    """The DVF case: a pipe separated .txt is the only file of the archive"""
    check = await fake_check(headers={"content-type": "application/zip"})
    await analyse_zip(check, rmock, make_zip({"ValeursFoncieres-2025.txt": DVF_LIKE_CONTENT}))
    await assert_analysed(db, check, expected_lines=20)


async def test_analyse_zip_ignores_readme(setup_catalog, rmock, db, fake_check, produce_mock):
    """A .txt next to a .csv is a readme, not the data"""
    check = await fake_check(headers={"content-type": "application/zip"})
    await analyse_zip(
        check, rmock, make_zip({"data.csv": CSV_CONTENT, "readme.txt": b"some documentation"})
    )
    await assert_analysed(db, check, expected_lines=2)


async def test_analyse_zip_ignores_macos_resource_fork(
    setup_catalog, rmock, db, fake_check, produce_mock
):
    """Archives zipped on macOS shadow every file with a __MACOSX/._name entry"""
    check = await fake_check(headers={"content-type": "application/zip"})
    await analyse_zip(
        check,
        rmock,
        make_zip({"data.csv": CSV_CONTENT, "__MACOSX/._data.csv": b"\x00\x05\x16\x07"}),
    )
    await assert_analysed(db, check, expected_lines=2)


async def test_analyse_zip_with_several_csv(setup_catalog, rmock, db, fake_check, produce_mock):
    check = await fake_check(headers={"content-type": "application/zip"})
    await analyse_zip(check, rmock, make_zip({"first.csv": CSV_CONTENT, "second.csv": CSV_CONTENT}))
    await assert_analysis_failed(db, check, "Several analysable files")


async def test_analyse_zip_without_analysable_file(
    setup_catalog, rmock, db, fake_check, produce_mock
):
    check = await fake_check(headers={"content-type": "application/zip"})
    await analyse_zip(check, rmock, make_zip({"doc.pdf": b"%PDF-1.4", "logo.png": b"\x89PNG"}))
    await assert_analysis_failed(db, check, "No analysable file")


async def test_analyse_zip_corrupted_archive(setup_catalog, rmock, db, fake_check, produce_mock):
    check = await fake_check(headers={"content-type": "application/zip"})
    truncated: bytes = make_zip({"data.csv": CSV_CONTENT})[:40]
    await analyse_zip(check, rmock, truncated)
    await assert_analysis_failed(db, check, "not a zip file")


async def test_analyse_zip_corrupted_member(setup_catalog, rmock, db, fake_check, produce_mock):
    """Central directory intact, compressed data damaged: it only blows up while extracting"""
    check = await fake_check(headers={"content-type": "application/zip"})
    archive = bytearray(make_zip({"data.csv": CSV_CONTENT * 10}))
    archive[60:80] = b"\x00" * 20
    await analyse_zip(check, rmock, bytes(archive))
    await assert_analysis_failed(db, check, "Bad CRC-32")


async def test_analyse_zip_member_too_large(
    setup_catalog, rmock, db, fake_check, produce_mock, mocker
):
    """A member is refused on the limit of the format it would be analysed with"""
    mocker.patch.object(Csv, "max_filesize_allowed", 10)
    check = await fake_check(headers={"content-type": "application/zip"})
    await analyse_zip(check, rmock, make_zip({"data.csv": CSV_CONTENT}))
    await assert_analysis_failed(db, check, "too large")


@pytest.mark.parametrize("fails", (False, True))
async def test_analyse_zip_leaves_no_file_behind(
    setup_catalog, rmock, db, fake_check, produce_mock, mocker, fails
):
    """Neither the archive nor the extracted member should survive the analysis, failed or not"""
    if fails:
        mocker.patch.object(Csv, "max_filesize_allowed", 10)
    check = await fake_check(headers={"content-type": "application/zip"})
    download_folder: Path = storage_path("")
    before: set[str] = {path.name for path in download_folder.iterdir()}

    await analyse_zip(check, rmock, make_zip({"data.csv": CSV_CONTENT}))

    assert {path.name for path in download_folder.iterdir()} == before


async def test_analyse_resource_extracts_zip(setup_catalog, rmock, db, fake_check, produce_mock):
    """The whole path: format detected from the catalog, download, extraction, csv analysis"""
    await Resource.update(RESOURCE_ID, {"format": "txt.zip"})
    check = await fake_check(headers={"content-type": "application/zip"})
    rmock.get(check["url"], status=200, body=make_zip({"data.txt": DVF_LIKE_CONTENT}))

    await analyse_resource(check=check, last_check=None)

    await assert_analysed(db, check, expected_lines=20)


@pytest.mark.parametrize(
    "resource_format,content_type,expected",
    (
        ("txt.zip", "application/zip", Zip),
        ("zip", "application/octet-stream", Zip),
        (None, "application/zip", Zip),
        # regression: "gzip".endswith("zip"), a gzipped csv must not be taken for an archive
        ("csv.gz", "application/gzip", Csvgz),
    ),
)
async def test_detect_zip_from_check_or_catalog(
    setup_catalog, fake_check, resource_format, content_type, expected
):
    await Resource.update(RESOURCE_ID, {"format": resource_format})
    check = await fake_check(headers={"content-type": content_type})
    assert await detect_data_format_from_check_or_catalog(check) is expected
