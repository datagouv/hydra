import hashlib
import zipfile
from io import BytesIO
from pathlib import Path

import pytest
from asyncpg.exceptions import UndefinedTableError

from tests.conftest import RESOURCE_ID, SIMPLE_CSV_CONTENT
from udata_hydra.analysis.helpers import download_from_check
from udata_hydra.analysis.resource import analyse_resource
from udata_hydra.data_formats import Csv, Csvgz, Geojson, Parquet, Xls, Xlsx, Zip
from udata_hydra.data_formats.data_format import DataFormat
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


def make_zip(members: dict[str, bytes], compression: int = zipfile.ZIP_DEFLATED) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression) as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def make_zip_with_corrupted_member() -> bytes:
    """Central directory intact, payload damaged: the archive only blows up on the CRC check, at
    the very end of the extraction, once a temporary file has been written.

    The member is stored rather than deflated so that the failure is exactly that one: damaging a
    deflate stream raises from zlib instead, depending on which byte was hit."""
    name: str = "data.csv"
    archive = bytearray(make_zip({name: CSV_CONTENT * 10}, compression=zipfile.ZIP_STORED))
    with zipfile.ZipFile(BytesIO(bytes(archive))) as reader:
        member: zipfile.ZipInfo = reader.infolist()[0]
        # a stored payload sits right after its local header: 30 fixed bytes, name, extra field
        payload_start: int = member.header_offset + 30 + len(member.filename) + len(member.extra)
    archive[payload_start] ^= 0xFF
    return bytes(archive)


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

    # the resource must not stay stuck in an analysis status
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] is None


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


async def assert_not_analysed(db, check: dict) -> None:
    """Holding nothing analysable is not a failure: no error must be recorded"""
    res = await db.fetchrow("SELECT * FROM checks")
    assert res["parsing_error"] is None
    assert res["parsing_table"] is None

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
    """A dataset split in several csv is not a broken resource, we just leave it alone"""
    check = await fake_check(headers={"content-type": "application/zip"})
    await analyse_zip(check, rmock, make_zip({"first.csv": CSV_CONTENT, "second.csv": CSV_CONTENT}))
    await assert_not_analysed(db, check)


async def test_analyse_zip_without_analysable_file(
    setup_catalog, rmock, db, fake_check, produce_mock, mocker
):
    """A shapefile bundle or a set of documents is not analysable, and not a failure either:
    udata must not be told the parsing failed"""
    notify_udata = mocker.patch(
        "udata_hydra.data_formats.zip.helpers.notify_udata", new=mocker.AsyncMock()
    )
    check = await fake_check(headers={"content-type": "application/zip"})
    await analyse_zip(
        check, rmock, make_zip({"parcels.shp": b"\x00\x00'\n", "parcels.dbf": b"\x03d"})
    )
    await assert_not_analysed(db, check)
    notify_udata.assert_not_awaited()


async def test_analyse_zip_does_not_recurse_into_a_nested_zip(
    setup_catalog, rmock, db, fake_check, produce_mock
):
    """The 42.zip dead end: an archive is never an analysable member of another archive"""
    check = await fake_check(headers={"content-type": "application/zip"})
    await analyse_zip(check, rmock, make_zip({"inner.zip": make_zip({"data.csv": CSV_CONTENT})}))
    await assert_not_analysed(db, check)


@pytest.mark.parametrize("compression", (zipfile.ZIP_LZMA, zipfile.ZIP_BZIP2))
async def test_analyse_zip_refuses_unbounded_compression(
    setup_catalog, rmock, db, fake_check, produce_mock, compression
):
    """zipfile only bounds what it decompresses for stored and deflated members: with lzma or
    bzip2 it decompresses a whole chunk in one go, so a member lying about its file_size would
    allocate gigabytes before any cap of ours can apply"""
    check = await fake_check(headers={"content-type": "application/zip"})
    await analyse_zip(check, rmock, make_zip({"data.csv": CSV_CONTENT}, compression=compression))
    await assert_analysis_failed(db, check, "Unsupported compression method")


@pytest.mark.parametrize("compression", (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED))
async def test_analyse_zip_accepts_bounded_compression(
    setup_catalog, rmock, db, fake_check, produce_mock, compression
):
    check = await fake_check(headers={"content-type": "application/zip"})
    await analyse_zip(check, rmock, make_zip({"data.csv": CSV_CONTENT}, compression=compression))
    await assert_analysed(db, check, expected_lines=2)


async def test_analyse_zip_corrupted_archive(
    setup_catalog, rmock, db, fake_check, produce_mock, mocker
):
    """A real failure, unlike an archive holding nothing analysable, must reach udata"""
    notify_udata = mocker.patch(
        "udata_hydra.data_formats.zip.helpers.notify_udata", new=mocker.AsyncMock()
    )
    check = await fake_check(headers={"content-type": "application/zip"})
    truncated: bytes = make_zip({"data.csv": CSV_CONTENT})[:40]
    await analyse_zip(check, rmock, truncated)
    await assert_analysis_failed(db, check, "not a zip file")
    notify_udata.assert_awaited_once()


async def test_analyse_zip_corrupted_member(setup_catalog, rmock, db, fake_check, produce_mock):
    check = await fake_check(headers={"content-type": "application/zip"})
    await analyse_zip(check, rmock, make_zip_with_corrupted_member())
    await assert_analysis_failed(db, check, "Bad CRC-32")


async def test_analyse_zip_member_too_large(
    setup_catalog, rmock, db, fake_check, produce_mock, mocker
):
    """A member is refused on the limit of the format it would be analysed with"""
    mocker.patch.object(Csv, "max_filesize_allowed", 10)
    check = await fake_check(headers={"content-type": "application/zip"})
    await analyse_zip(check, rmock, make_zip({"data.csv": CSV_CONTENT}))
    await assert_analysis_failed(db, check, "too large")


@pytest.mark.parametrize(
    "body,refuse_on_size",
    (
        (make_zip({"data.csv": CSV_CONTENT}), False),
        (make_zip({"data.csv": CSV_CONTENT}), True),
        (make_zip_with_corrupted_member(), False),
    ),
    ids=("analysed", "refused-before-extraction", "failed-mid-extraction"),
)
async def test_analyse_zip_leaves_no_file_behind(
    setup_catalog, rmock, db, fake_check, produce_mock, mocker, body, refuse_on_size
):
    """Neither the archive nor the extracted member should survive the analysis, whether it
    succeeds, is refused before anything is written, or blows up halfway through the extraction"""
    if refuse_on_size:
        mocker.patch.object(Csv, "max_filesize_allowed", 10)
    check = await fake_check(headers={"content-type": "application/zip"})
    download_folder: Path = storage_path("")
    before: set[str] = {path.name for path in download_folder.iterdir()}

    await analyse_zip(check, rmock, body)

    assert {path.name for path in download_folder.iterdir()} == before


@pytest.mark.parametrize(
    "member_name,expected_format",
    (
        ("data.csv", Csv),
        ("data.tsv", Csv),
        ("data.xls", Xls),
        ("data.xlsx", Xlsx),
        ("data.geojson", Geojson),
        ("data.parquet", Parquet),
        ("data.txt", Csv),
    ),
)
async def test_extract_hands_the_member_to_its_own_format(
    setup_catalog, rmock, db, fake_check, produce_mock, member_name, expected_format
):
    """The extension table is what routes a member to its analyser: an inverted entry would have
    a xls analysed as a xlsx without anything failing"""
    check = await fake_check(headers={"content-type": "application/zip"})
    rmock.get(check["url"], status=200, body=make_zip({member_name: CSV_CONTENT}))
    archive: DataFormat = await download_from_check(check, Zip)
    assert isinstance(archive, Zip)

    extracted: DataFormat | None = archive.extract(check)

    assert extracted is not None
    assert type(extracted) is expected_format
    archive.path.unlink(missing_ok=True)
    extracted.path.unlink()


@pytest.mark.parametrize("filename", ("catalog.xls", "catalog.xlsx"))
async def test_analyse_zip_with_a_single_excel_file(
    setup_catalog, rmock, db, fake_check, produce_mock, filename
):
    """Extracted members are named after a temporary file: the analysers must cope with a path
    that carries no extension of its own"""
    check = await fake_check(headers={"content-type": "application/zip"})
    with open(f"tests/data/{filename}", "rb") as f:
        content: bytes = f.read()
    await analyse_zip(check, rmock, make_zip({filename: content}))
    await assert_analysed(db, check, expected_lines=2)


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
        (None, "application/x-zip-compressed", Zip),
        # regression: "gzip".endswith("zip"), a gzipped csv must not be taken for an archive
        ("csv.gz", "application/gzip", Csvgz),
        # regression: an xlsx IS a zip container and is often served as such, the catalog format
        # of another format must not lose to the zip mime type
        ("xlsx", "application/zip", Xlsx),
        ("xlsx", "application/x-zip-compressed", Xlsx),
    ),
)
async def test_detect_zip_from_check_or_catalog(
    setup_catalog, fake_check, resource_format, content_type, expected
):
    await Resource.update(RESOURCE_ID, {"format": resource_format})
    check = await fake_check(headers={"content-type": content_type})
    assert await detect_data_format_from_check_or_catalog(check) is expected
