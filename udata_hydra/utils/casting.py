import logging
from typing import Any, Iterator

from csv_detective.output.dataframe import cast

from udata_hydra.utils.reader import Reader

log = logging.getLogger("udata-hydra")


def smart_cast(_type: str, value, cast_json: bool = True, failsafe: bool = False) -> Any:
    try:
        if value is None or value == "":
            return None
        if _type == "json" and not cast_json:
            # handing JSON as string to postgres, which casts it itself
            return value
        return cast(value, _type)
    except ValueError as e:
        if not failsafe:
            raise e
        log.warning(f'Could not convert "{value}" to {_type}, defaulting to null')
        return None


def generate_records(
    file_path: str, inspection: dict, cast_json: bool = True, as_dict: bool = False
) -> Iterator[list | dict]:
    # because we need the iterator multiple times, not possible to
    # handle db, parquet and geojson through the same iteration
    columns = {col: v["python_type"] for col, v in inspection["columns"].items()}
    with Reader(file_path, inspection) as reader:
        for line in reader:
            if line:
                if not as_dict:
                    yield [
                        smart_cast(
                            _type,
                            value if isinstance(value, str) or value is None else str(value),
                            cast_json=cast_json,
                            failsafe=False,
                        )
                        for _type, value in zip(columns.values(), line)
                    ]
                else:
                    yield {
                        col: smart_cast(
                            _type,
                            value if isinstance(value, str) or value is None else str(value),
                            cast_json=cast_json,
                            failsafe=False,
                        )
                        for (col, _type), value in zip(columns.items(), line)
                    }
