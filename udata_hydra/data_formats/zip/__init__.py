import logging
import shutil
import zipfile
from pathlib import Path
from tempfile import NamedTemporaryFile

from asyncpg import Record

from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.data_formats.csv_like import Csv, Xls, Xlsx
from udata_hydra.data_formats.data_format import DataFormat
from udata_hydra.data_formats.geojson import Geojson
from udata_hydra.data_formats.parquet import Parquet
from udata_hydra.db.resource import Resource
from udata_hydra.utils import IOException, handle_parse_exception, storage_path

log = logging.getLogger("udata-hydra")

# Extensions that only ever carry data, mapped to the format that knows how to analyse them.
# `.zip` is deliberately absent: this is what makes a zip of zips (the 42.zip bomb) a dead end.
DATA_EXTENSIONS: dict[str, type[DataFormat]] = {
    ".csv": Csv,
    ".tsv": Csv,
    ".xls": Xls,
    ".xlsx": Xlsx,
    ".geojson": Geojson,
    ".parquet": Parquet,
}

# A `.txt` is worth analysing (csv-detective sniffs the separator, DVF ships its data that way), but
# it is also how a readme is named: only consider those when the archive holds no proper data file.
FALLBACK_EXTENSIONS: dict[str, type[DataFormat]] = {".txt": Csv}

EXTRACTION_CHUNK_SIZE = 1024 * 1024

# The only two methods whose output zipfile bounds while decompressing: it passes our read size
# down to the deflate decompressor, and a stored member is copied as is. For bzip2 and lzma it
# hands the whole compressed chunk over and truncates the result afterwards, so a member lying
# about its file_size would have allocated gigabytes before we get a chance to count anything.
SAFE_COMPRESSION_METHODS = {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}

# Member names end up in parsing_error, which we send to udata: never relay an arbitrary long string
MAX_MEMBER_NAME_LENGTH = 100


def is_data_member(info: zipfile.ZipInfo) -> bool:
    """Directories and macOS resource forks carry no data, but the forks (__MACOSX/._name) do carry
    the extension of the file they shadow, so they would be counted as candidates."""
    name = Path(info.filename)
    return not info.is_dir() and "__MACOSX" not in name.parts and not name.name.startswith("._")


class Zip(DataFormat):
    """A zip archive holding a single analysable file.

    The archive itself carries no data: we extract that file and hand it over to the format that
    knows how to analyse it, so the rest of the pipeline never has to know it came from an archive.
    """

    standard_mime_type = "application/zip"
    # not "application/octet-stream", which already belongs to Csvgz
    valid_mime_types = {standard_mime_type, "application/x-zip-compressed"}
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["zip"])
    further_analysis = True

    @classmethod
    def detect_from_catalog_format(cls, format: str | None) -> bool:
        # udata declares archives either as "zip" or as "<inner extension>.zip", e.g. "txt.zip".
        # Matching on ".zip" and not "zip" keeps "gzip" out.
        return format is not None and (format == "zip" or format.endswith(".zip"))

    def io_error(self, message: str, check: dict) -> IOException:
        return IOException(
            message,
            step="zip_extraction",
            resource_id=self.resource_id,
            url=check["url"],
            check_id=check["id"],
        )

    def select_member(
        self, archive: zipfile.ZipFile
    ) -> tuple[zipfile.ZipInfo, type[DataFormat]] | None:
        """Pick the one file of the archive we know how to analyse, or None if there isn't exactly
        one. An archive of shapefiles, of pictures, or a dataset split in several csv is not a
        broken resource: there is simply nothing for us to analyse, and saying so as a parsing
        error would report a failure where hydra never committed to analysing anything."""
        members: list[zipfile.ZipInfo] = [
            info for info in archive.infolist() if is_data_member(info)
        ]

        candidates: list[tuple[zipfile.ZipInfo, type[DataFormat]]] = []
        for extensions in (DATA_EXTENSIONS, FALLBACK_EXTENSIONS):
            candidates = [
                (info, data_format)
                for info in members
                if (data_format := extensions.get(Path(info.filename).suffix.lower()))
            ]
            if candidates:
                break

        if not candidates:
            log.debug("No analysable file in the zip archive, skipping.")
            return None
        if len(candidates) > 1:
            names = ", ".join(info.filename[:MAX_MEMBER_NAME_LENGTH] for info, _ in candidates[:5])
            log.debug(f"Several analysable files in the zip archive, skipping: {names}")
            return None

        return candidates[0]

    def extract_member(
        self, archive: zipfile.ZipFile, member: zipfile.ZipInfo, max_size: int, check: dict
    ) -> Path:
        """Write a member of the archive next to it, without ever letting it grow over :max_size:"""
        if member.compress_type not in SAFE_COMPRESSION_METHODS:
            raise self.io_error(
                f"Unsupported compression method in the zip archive: {member.compress_type}", check
            )

        # Together with the check above, this is what caps a decompression bomb: for those two
        # methods zipfile stops decompressing at the announced file_size (and fails the CRC check
        # if the content does not match it), so an archive cannot deliver more bytes than it
        # declares here.
        if member.file_size > max_size:
            raise self.io_error(
                f"Extracted file too large: {member.filename[:MAX_MEMBER_NAME_LENGTH]}", check
            )

        destination = NamedTemporaryFile(dir=storage_path(""), delete=False)
        try:
            # never `archive.extract()`: it would write the member under the name the archive
            # chose, wherever that name points to inside the shared download folder, while the
            # rest of the pipeline expects the generated one
            with destination, archive.open(member) as source:
                shutil.copyfileobj(source, destination, EXTRACTION_CHUNK_SIZE)
        except Exception as e:
            Path(destination.name).unlink(missing_ok=True)
            raise self.io_error(
                f"Could not extract {member.filename[:MAX_MEMBER_NAME_LENGTH]} from the zip archive",
                check,
            ) from e

        return Path(destination.name)

    def extract(self, check: dict) -> DataFormat | None:
        """Extract the single analysable file of the archive, as the format able to analyse it,
        or None if the archive holds nothing we know how to analyse"""
        try:
            # Opening an archive reads its whole central directory into memory, one ZipInfo per
            # entry and no cap: an archive made of nothing but entry headers costs roughly ten
            # times its own size in RSS. Bounding that would mean parsing the end of central
            # directory record ourselves before handing the file to zipfile — the amplification
            # stays bounded by MAX_FILESIZE_ALLOWED.zip, so we live with it.
            archive = zipfile.ZipFile(self.path)
        except Exception as e:
            # BadZipFile, but also RuntimeError (encrypted archive), NotImplementedError (unsupported
            # compression method), EOFError, OSError... none of which should take the worker down
            raise self.io_error("Could not open the zip archive", check) from e

        with archive:
            selected = self.select_member(archive)
            if selected is None:
                return None
            member, data_format = selected
            extracted: Path = self.extract_member(
                archive, member, data_format.max_filesize_allowed, check
            )

        return data_format(
            file_name=extracted.name,
            resource_id=self.resource_id,
            dataset_id=self.dataset_id,
        )

    async def analyse(self, check: dict) -> None:
        """Extract the archive and run the analysis of the format it holds"""
        resource_id: str = str(check["resource_id"])
        resource: Record | None = await Resource.update(resource_id, {"status": "EXTRACTING_ZIP"})

        file: DataFormat | None = None
        try:
            file = self.extract(check)
        except IOException as e:
            check = await handle_parse_exception(e, None, check)  # type: ignore[assignment]
            await helpers.notify_udata(resource, check)
        finally:
            self.path.unlink(missing_ok=True)

        if file is None:
            await Resource.update(resource_id, {"status": None})
            return

        log.debug(f"Extracted {file.file_name} from archive, analysing it as {type(file).__name__}")
        await file.analyse(check=check)
