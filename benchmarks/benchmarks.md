## Hydra Performance Benchmarks

### GeoJSON Performance Testing

*Generated from pytest benchmark data using `uv run pytest tests/test_analysis/test_geojson.py::test_geojson_to_pmtiles_large_file --durations=0` with Python 3.11.*

`test_geojson_to_pmtiles_large_file`: 50,000 features, ~14.4MB input, ~2.4MB output

#### Tippecanoe Argument Performance Comparison

*Testing different tippecanoe command configurations with 50,000 features. See [tippecanoe options documentation](https://github.com/felt/tippecanoe?tab=readme-ov-file#options) for more details.*

- **1.872s** - `--coalesce-densest-as-needed` (5 runs: 1.86s, 1.84s, 1.88s, 1.88s, 1.90s)
- **1.8725s** - `--drop-densest-as-needed` (4 runs: 1.86s, 1.90s, 1.88s, 1.85s)
- **1.8975s** - `--coalesce-densest-as-needed --drop-densest-as-needed` (4 runs: 1.87s, 1.97s, 1.89s, 1.86s)
- **1.9375s** - `--coalesce-densest-as-needed -P` (4 runs: 2.01s, 1.98s, 1.87s, 1.89s)

...to be continued

### Marshmallow vs. Pydantic

*Generated from pytest benchmark data comparing Marshmallow vs Pydantic implementations, using the command `poetry run pytest --durations=0`.*

*Tests were run after making sure the project dependencies (that are not Marshmallow nor Pydantic) use the exact same pinned version, pinning them to latest versions before the tests with `poetry update --lock`.*

#### Marshmallow (using Poetry run)
1. **61.89s** - `test_analyse_csv_big_file` (9 runs, range: 58.65s - 70.18s)
2. **59.47s** - `test_exception_analysis` (6 runs, range: 55.12s - 65.52s)
3. **0.60s** - `test_backoff_rate_limiting_cooled_off`
4. **0.54s** - `test_backoff_on_429_status_code`
5. **0.38s** - `test_formats_analysis[file_and_count0]`
6. **0.29s** - `test_save_as_parquet[file_and_count0]` (3 runs, range: 0.28s - 0.29s)
7. **0.25s** - `test_save_as_parquet[file_and_count2]` (3 runs, range: 0.24s - 0.25s)
8. **0.24s** - `test_save_as_parquet[file_and_count1]` (3 runs, range: 0.24s - 0.25s)

#### Pydantic (using Poetry run)
1. **30.11s** - `test_analyse_csv_big_file` (6 runs, range: 21.56s - 60.33s)
2. **41.41s** - `test_exception_analysis` (6 runs, range: 21.42s - 60.34s)
3. **0.043s** - `test_check_changed_content_type_header` (6 runs, range: 0.04s - 0.05s)
4. **0.97s** - `test_save_as_parquet[file_and_count0]` (6 runs, range: 0.28s - 2.50s)
5. **0.59s** - `test_backoff_rate_limiting_cooled_off`
6. **0.56s** - `test_backoff_on_429_status_code`
7. **0.28s** - `test_save_as_parquet[file_and_count1]` (6 runs, range: 0.24s - 0.26s)
8. **0.29s** - `test_save_as_parquet[file_and_count2]` (6 runs, range: 0.28s - 0.30s)
9. **0.30s** - `test_save_as_parquet[file_and_count0]` (6 runs, range: 0.28s - 0.35s)


### Python 3.11 vs. Python 3.13

#### Python 3.11, main branch with uv run

- **59.30s** - `test_analyse_csv_big_file` (2 runs: 59.44s, 59.16s)

#### Python 3.13, main branch with uv run

- **74.66s** - `test_analyse_csv_big_file` (2 runs: 73.13s, 76.18s)
