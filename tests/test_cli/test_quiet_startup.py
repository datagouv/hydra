import subprocess
from pathlib import Path


def test_cli_quiet_startup_has_no_botocore_logs() -> None:
    project_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        ["uv", "run", "udata-hydra", "purge-csv-tables", "--quiet", "--help"],
        capture_output=True,
        text=True,
        cwd=project_root,
    )
    combined = result.stderr + result.stdout
    assert "botocore" not in combined.lower()
    assert "boto3" not in combined.lower()
    assert result.returncode == 0
