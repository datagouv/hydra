import json
import os
from abc import ABC, abstractmethod
from pathlib import Path


class DataFormat(ABC):
    standard_mime_type: str
    valid_mime_types: set[str]
    filesize: int
    max_filesize_allowed: int
    further_analysis: bool = False
    check_url: str | None
    inspection: dict

    def __init__(
        self,
        *,
        path: Path | str | None = None,
        table_name: str | None = None,
        inspection: dict | None = None,
        resource_id: str | None = None,
    ) -> None:
        if path:
            self.path = Path(path) if isinstance(path, str) else path
            self.filesize = os.path.getsize(self.path)
        elif table_name:
            self.table_name = table_name
        else:
            raise ValueError("A FileFormat must have either a path or a table_name")
        if inspection:
            # passing it on
            self.inspection = inspection

    @classmethod
    def detect_from_check(cls, check: dict, **kwargs) -> bool:
        # this method may require other arguments for specific formats
        headers: dict = json.loads(check.get("headers") or "{}")
        return any(
            headers.get("content-type", "").lower().startswith(ct) for ct in cls.valid_mime_types
        ) or (cls.check_url is not None and cls.check_url in check.get("url", ""))

    @classmethod
    def detect_from_catalog_format(cls, format: str | None) -> bool:
        # overridden in specific formats
        return cls.__name__.lower() == format

    @abstractmethod
    def analyse(self, **kwargs): ...
