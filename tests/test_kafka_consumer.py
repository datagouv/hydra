import requests_mock
import io
from hydra.background_tasks import download_resource


def test_manage_resource():
    message = {
        "id": "6065c5cd9f89bfd828bcbf5c",
        "title": "Sample-title",
        "description": "Sample",
        "extras": {},
        "resources": [
            {
                "id": "503c39b0-58d8-45fe-ace0-55795bb0a6b7",
                "url": "https://local.dev/resources/sample-title/20210401-182031/rickroll.csv",
                "format": "csv",
                "title": "rickroll.csv",
                "schema": {},
                "description": None,
                "filetype": "file",
                "type": "main",
                "created_at": "2021-04-01T15:10:12",
                "modified": "2021-04-01T18:20:33",
                "published": "2021-04-01T15:10:12",
                "extras": {},
            }
        ],
    }

    output = b"First line.\nSecond line"
    with requests_mock.Mocker() as m:
        m.get("http://test.com", content=output)
        tmp_file = download_resource("http://test.com")
        with open(tmp_file.name, "r") as f1:
            assert len(f1.readlines()) == len(output.split(b"\n"))
