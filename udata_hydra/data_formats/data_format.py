from abc import ABC, abstractmethod
import json
import os
from pathlib import Path


class DataFormat(ABC):
    standard_mime_type: str
    valid_mime_types: set[str]
    filesize : int
    max_filesize_allowed: int
    resource_id: str | None
    inspection: dict
    url: str | None
    check_url: str | None

    def __init__(
        self,
        *,
        path: Path | str | None = None,
        table_name: str | None = None,
        inspection: dict | None = None,
        resource_id: str | None = None
    ) -> None:
        if path:
            self.path = Path(path) if isinstance(path, str) else path
            self.filesize = os.path.getsize(self.path)
        elif table_name:
            self.table_name = table_name
        else:
            raise ValueError("A FileFormat must have either a path or a table_name")
        if resource_id:
            self.resource_id = resource_id
        if inspection:
            self.inspection = inspection

    @classmethod
    def detect_from_check(cls, check: dict) -> bool:
        headers: dict = json.loads(check["headers"] or "{}")
        return any(
            headers.get("content-type", "").lower().startswith(ct)
            for ct in cls.valid_mime_types
        ) or (
            bool(cls.check_url) and cls.check_url in check.get("url", "")
        )

    @classmethod
    async def detect_from_catalog(cls) -> bool:
        # overridden in specific formats
        return False
    
    @abstractmethod
    async def to(self, target_format: str, **kwargs) -> "DataFormat|None": ...
    
    @abstractmethod
    def analyse(self, **kwargs): ...
