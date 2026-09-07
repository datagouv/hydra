import json
import logging
import os

import magic

from udata_hydra import config
from udata_hydra.data_formats.data_format import DataFormat
from udata_hydra.db.resource import Resource
from udata_hydra.utils import IOException, extract_gzip

log = logging.getLogger("udata-hydra")


class Gz(DataFormat):
    """Gzip wrapper: unwrap the payload and analyse it as its own format."""

    standard_mime_type = "application/gzip"
    valid_mime_types = {standard_mime_type, "application/x-gzip"}
    # octet-stream is accepted only with ".gz" in the URL by `detect_from_check()`;
    # in `valid_mime_types` it would match shapefile, FlatGeobuf, etc. without that AND.
    gzip_like_mime_types = {*valid_mime_types, "application/octet-stream"}
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["gz"])
    check_url = ".gz"
    further_analysis = True

    @classmethod
    def detect_from_catalog_format(cls, format: str | None) -> bool:
        return format is not None and (format.endswith(".gz") or format in {"gz", "gzip"})

    @classmethod
    def detect_from_check(cls, check: dict, **kwargs) -> bool:
        """HTTP gzip hint: gzip-like Content-Type AND ".gz" in the URL."""
        headers: dict = json.loads(check.get("headers") or "{}")
        content_type = headers.get("content-type", "").lower()
        gzip_like = any(content_type.startswith(ct) for ct in cls.gzip_like_mime_types)
        return gzip_like and cls.check_url in check.get("url", "")

    def unwrap(self) -> None:
        """Gunzip in place; call before checksum or analysis on the payload."""
        mime_type = magic.from_file(str(self.path), mime=True)
        if mime_type not in self.valid_mime_types:
            return
        try:
            extracted = extract_gzip(str(self.path))
        except IOException:
            self.path.unlink(missing_ok=True)
            raise
        self.path.unlink(missing_ok=True)
        self.file_name = os.path.basename(extracted.name)

    async def analyse(self, check: dict, debug_insert: bool = False) -> None:
        from udata_hydra.data_formats.detect import (
            catalog_format_for,
            detect_format_from_payload,
        )

        payload_mime = magic.from_file(str(self.path), mime=True)
        inner_cls = detect_format_from_payload(
            check,
            payload_mime,
            await catalog_format_for(self.resource_id),
        )
        if inner_cls is None:
            log.info(
                f"Gzip payload is {payload_mime}, not an analysable format, "
                f"skipping (resource_id={self.resource_id})"
            )
            if self.resource_id:
                await Resource.update(self.resource_id, {"status": None})
            self.path.unlink(missing_ok=True)
            return

        log.debug(f"Unwrapped gzip, analysing as {inner_cls.__name__}")
        inner = inner_cls(
            file_name=self.file_name,
            resource_id=self.resource_id,
            dataset_id=self.dataset_id,
        )
        await inner.analyse(check, debug_insert=debug_insert)
