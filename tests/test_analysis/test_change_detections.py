import datetime
from unittest.mock import MagicMock

import pytest

from udata_hydra.analysis.resource import (
    Change,
    detect_resource_change_from_checksum,
    detect_resource_change_from_content_length_header,
    detect_resource_change_from_harvest,
    detect_resource_change_from_last_modified_header,
)


def strp(date_str: str) -> datetime.datetime:
    return datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")


def fake_check_data(
    created_at="2000-06-01 00:00:00",
    last_modified="2000-01-01 00:00:00",
    content_length="100",
    detected_last_modified_at=None,
) -> dict:
    data = {
        "created_at": strp(created_at),
        "last_modified": last_modified,
        "content_length": content_length,
    }
    if detected_last_modified_at:
        data["detected_last_modified_at"] = strp(detected_last_modified_at)
    return data


@pytest.mark.parametrize(
    "args",
    [
        # only one check
        (
            None,
            fake_check_data(),
            (Change.NO_GUESS, None),
        ),
        # new check doesn't have content-length
        (
            fake_check_data(),
            fake_check_data(content_length=None),
            (Change.NO_GUESS, None),
        ),
        # checks have different content-lengths
        (
            fake_check_data(content_length=100),
            fake_check_data(created_at="2025-05-02 00:00:00", content_length=200),
            (
                Change.HAS_CHANGED,
                {
                    "analysis:last-modified-at": strp("2025-05-02 00:00:00").isoformat(),
                    "analysis:last-modified-detection": "content-length-header",
                },
            ),
        ),
        # checks have same content-length
        (
            fake_check_data(detected_last_modified_at="2025-04-01 00:00:00"),
            fake_check_data(created_at="2025-05-02 00:00:00"),
            (
                Change.HAS_NOT_CHANGED,
                {
                    "analysis:last-modified-at": strp("2025-04-01 00:00:00").isoformat(),
                    "analysis:last-modified-detection": "previous-check-detection",
                },
            ),
        ),
    ],
)
async def test_content_length_header(args):
    previous_check, current_check, expected_result = args
    data = [current_check, previous_check] if previous_check else [current_check]
    res = await detect_resource_change_from_content_length_header(data)
    assert res == expected_result


@pytest.mark.parametrize(
    "args",
    [
        # only one check which has last-modified
        (
            None,
            fake_check_data(last_modified="2025-01-01 00:00:00"),
            (
                Change.HAS_CHANGED,
                {
                    "analysis:last-modified-at": strp("2025-01-01 00:00:00").isoformat(),
                    "analysis:last-modified-detection": "last-modified-header",
                },
            ),
        ),
        # only one check which doesn't have last-modified
        (
            None,
            fake_check_data(last_modified=None),
            (Change.NO_GUESS, None),
        ),
        # two checks but the latest doesn't have last-modified
        (
            fake_check_data(),
            fake_check_data(created_at="2025-05-02 00:00:00", last_modified=None),
            (Change.NO_GUESS, None),
        ),
        # two checks, same last-modified
        (
            fake_check_data(),
            fake_check_data(created_at="2025-05-02 00:00:00"),
            (
                Change.HAS_NOT_CHANGED,
                {
                    "analysis:last-modified-at": strp("2000-01-01 00:00:00").isoformat(),
                    "analysis:last-modified-detection": "last-modified-header",
                },
            ),
        ),
        # two checks, different last-modified
        (
            fake_check_data(),
            fake_check_data(created_at="2025-05-02 00:00:00", last_modified="2025-05-01 00:00:00"),
            (
                Change.HAS_CHANGED,
                {
                    "analysis:last-modified-at": strp("2025-05-01 00:00:00").isoformat(),
                    "analysis:last-modified-detection": "last-modified-header",
                },
            ),
        ),
    ],
)
async def test_last_modified_header(args):
    previous_check, current_check, expected_result = args
    data = [current_check, previous_check] if previous_check else [current_check]
    res = await detect_resource_change_from_last_modified_header(data)
    assert res == expected_result


@pytest.mark.parametrize(
    "args",
    [
        # only one check
        (
            None,
            fake_check_data(),
            {"harvest_modified_at": strp("2025-05-01 00:00:00")},
            (Change.NO_GUESS, None),
        ),
        # two checks, no resource
        (
            fake_check_data(),
            fake_check_data(created_at="2025-05-02 00:00:00"),
            None,
            (Change.NO_GUESS, None),
        ),
        # two checks, resource not harvested
        (
            fake_check_data(),
            fake_check_data(created_at="2025-05-02 00:00:00"),
            {"foo": "bar"},
            (Change.NO_GUESS, None),
        ),
        # two checks, resource harvested and recently modified
        (
            fake_check_data(detected_last_modified_at="2000-06-01 00:00:00"),
            fake_check_data(created_at="2025-05-02 00:00:00"),
            {"harvest_modified_at": strp("2025-05-01 00:00:00")},
            (
                Change.HAS_CHANGED,
                {
                    "analysis:last-modified-at": strp("2025-05-01 00:00:00").isoformat(),
                    "analysis:last-modified-detection": "harvest-resource-metadata",
                },
            ),
        ),
        # two checks, resource harvested untouched
        (
            fake_check_data(detected_last_modified_at="2000-06-01 00:00:00"),
            fake_check_data(created_at="2025-05-02 00:00:00"),
            {"harvest_modified_at": strp("2000-06-01 00:00:00")},
            (
                Change.HAS_NOT_CHANGED,
                {
                    "analysis:last-modified-at": strp("2000-06-01 00:00:00").isoformat(),
                    "analysis:last-modified-detection": "harvest-resource-metadata",
                },
            ),
        ),
    ],
)
async def test_harvest(args):
    previous_check, current_check, resource, expected_result = args
    data = [current_check, previous_check] if previous_check else [current_check]
    res = await detect_resource_change_from_harvest(data, resource)
    assert res == expected_result


@pytest.mark.parametrize(
    "args",
    [
        # no previous check
        (
            "aaa",
            None,
            (Change.NO_GUESS, None),
        ),
        # previous check, checksum has changed
        (
            "aaa",
            {"checksum": "bbb"},
            (
                Change.HAS_CHANGED,
                {
                    "analysis:last-modified-at": strp("2025-01-01 00:00:00").isoformat(),
                    "analysis:last-modified-detection": "computed-checksum",
                },
            ),
        ),
        # previous check, checksum has changed
        (
            "aaa",
            {"checksum": "aaa", "detected_last_modified_at": strp("2025-06-01 00:00:00")},
            (
                Change.HAS_NOT_CHANGED,
                {
                    "analysis:last-modified-at": strp("2025-06-01 00:00:00").isoformat(),
                    "analysis:last-modified-detection": "previous-check-detection",
                },
            ),
        ),
    ],
)
async def test_checksum(args, mocker):
    checksum, last_check, expected_result = args
    if expected_result[0] == Change.HAS_CHANGED:
        mock_date = mocker.patch("udata_hydra.analysis.resource.datetime")
        mock_date.now.return_value = strp("2025-01-01 00:00:00")
    res = await detect_resource_change_from_checksum(checksum, last_check)
    assert res == expected_result
