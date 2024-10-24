# Changelog

## Current (in progress)

- Fix wrong resource status [#196](https://github.com/datagouv/hydra/pull/196)
- Fix issue related to empty `table_indexes` column instead of default `{}` [#197](https://github.com/datagouv/hydra/pull/197)


## 2.0.3 (2024-10-22)

- Save git commit hash in CI and use it for health check [#182](https://github.com/datagouv/hydra/pull/182) and [#185](https://github.com/datagouv/hydra/pull/185)
- Add comment column/field to ressources exceptions [#191](https://github.com/datagouv/hydra/pull/191)
- Add extra args and DB fields for parquet export [#193](https://github.com/datagouv/hydra/pull/193)
- Fix CircleCI config for packaging version not to include commit hash when publishing [#194](https://github.com/datagouv/hydra/pull/194)

## 2.0.2 (2024-10-07)

- Fix typos in README in curl commands examples [#189](https://github.com/datagouv/hydra/pull/189)
- Bump csv-detective to 0.7.3 [#192](https://github.com/datagouv/hydra/pull/192)

## 2.0.1 (2024-10-04)

- Refactor function to get no_backoff domains and add PostgreSQL indexes to improve DB queries perfs [#171](https://github.com/datagouv/hydra/pull/171)
- Clean changelog and remove useless section in pyproject.toml [#175](https://github.com/datagouv/hydra/pull/175)
- Refactor purge_checks CLI to use a date limit instead of a number [#174](https://github.com/datagouv/hydra/pull/174)
- Fix resources exceptions routes responses, add resources exceptions tests [#176](https://github.com/datagouv/hydra/pull/176)
- Fix CSV analysis CLI [#181](https://github.com/datagouv/hydra/pull/181)
- Add a `PUT` `/api/resources-exceptions/{id}` route to update a resource exception [#178](https://github.com/datagouv/hydra/pull/178)
- Add a `quiet` argument for `purge_check` and `purge_csv_table` CLIs [#184](https://github.com/datagouv/hydra/pull/184)
- Fix wrong resource status [#187](https://github.com/datagouv/hydra/pull/187)
- More informative error relative to check resource CLI [#188](https://github.com/datagouv/hydra/pull/188)

## 2.0.0 (2024-09-24)

- Use Python 3.11 instead of 3.9 for performance improvements and future compatibility [#101](https://github.com/datagouv/hydra/pull/101)
- Refactor and split code from `crawl.py` into separate files using refactored `db.Resource` class methods and static methods [#135](https://github.com/datagouv/hydra/pull/135)
- Allow routes with or without trailing slashes [#158](https://github.com/datagouv/hydra/pull/158)
- Delete resource as a CRUD method [#161](https://github.com/datagouv/hydra/pull/161)
- Refactor routes URLs to be more RESTful and separate legacy routes code from new routes code [#132](https://github.com/datagouv/hydra/pull/132)
- Display app version and environment in health check endpoint [#164](https://github.com/datagouv/hydra/pull/164)
- Use ENVIRONMENT from config file instead of env var [#165](https://github.com/datagouv/hydra/pull/165)
- Manage large resources exceptions differently [#148](https://github.com/datagouv/hydra/pull/148)
- Add checks aggregate route [#167](https://github.com/datagouv/hydra/pull/167)

## 1.1.0 (2024-09-26)

- Use profiling option from csv-detective [#54](https://github.com/etalab/udata-hydra/pull/54)
- Remove csv_analysis, integrate into checks [#52](https://github.com/etalab/udata-hydra/pull/52)
- Add new types for csv parsing: json, date and datetime [#51](https://github.com/etalab/udata-hydra/pull/51)
- Notify udata of csv parsing [#51](https://github.com/etalab/udata-hydra/pull/51)
- Allow `None` values in udata notifications [#51](https://github.com/etalab/udata-hydra/pull/51)
- Add tests for udata-triggered checks [#49](https://github.com/etalab/udata-hydra/pull/49)
- Include migration files in package
- Allow to configure a dedicated PostgreSQL schema [#56](https://github.com/etalab/udata-hydra/pull/56)
- Fix typo in handle_parse_exception and schema in CLI [#57](https://github.com/etalab/udata-hydra/pull/57)
- Skip archived dataset when loading catalog [#58](https://github.com/etalab/udata-hydra/pull/58)
- Update resources expected dates in API following udata refactoring [#60](https://github.com/etalab/udata-hydra/pull/60)
- Download csv resource only if first check [#61](https://github.com/etalab/udata-hydra/pull/61)
- Send content-type and content-length info from header to udata [#64](https://github.com/etalab/udata-hydra/pull/64)
- Add timezone values to dates sent to udata [#63](https://github.com/etalab/udata-hydra/pull/63)
- Rename analysis filesize to content-length [#66](https://github.com/etalab/udata-hydra/pull/66)
- Sleep between all batches [#67](https://github.com/etalab/udata-hydra/pull/67)
- Support having multiple crawlers by setting a status column in the catalog table [#68](https://github.com/etalab/udata-hydra/pull/68)
- Add a health route [#69](https://github.com/etalab/udata-hydra/pull/69)
- Make temporary folder configurable [#70](https://github.com/etalab/udata-hydra/pull/70)
- Fix conflict on updating catalog with multiple entries for a resource [#73](https://github.com/etalab/udata-hydra/pull/73)
- Set check:available to None in case of a 429 [#75](https://github.com/etalab/udata-hydra/pull/75)
- Improve conditional analysis logic and readability [#76](https://github.com/etalab/udata-hydra/pull/76) [#80](https://github.com/etalab/udata-hydra/pull/80)
- Use latest csv-detective version [#89](https://github.com/etalab/udata-hydra/pull/89)
- Compare content type / length to check if changed [#78](https://github.com/etalab/udata-hydra/pull/78) [#79](https://github.com/etalab/udata-hydra/pull/79)
- Create a list of exceptions to analyse despite larger size [#85](https://github.com/etalab/udata-hydra/pull/85)
- Enable csv.gz analysis [#84](https://github.com/etalab/udata-hydra/pull/84)
- Add worker default timeout config [#86](https://github.com/etalab/udata-hydra/pull/86)
- Return None value early when casting in csv analysis [#87](https://github.com/etalab/udata-hydra/pull/87)
- Ping udata after loading a csv to database [#91](https://github.com/etalab/udata-hydra/pull/91)
- Allow for none value in resource schema [#93](https://github.com/etalab/udata-hydra/pull/93)
- Handle other file formats [#92](https://github.com/etalab/udata-hydra/pull/92)
- Add a quiet option on load catalog [#95](https://github.com/datagouv/hydra/pull/95)
- Select distinct parsing tables to delete [#96](https://github.com/datagouv/hydra/pull/96)
- Enable parquet export [#97](https://github.com/datagouv/hydra/pull/97)
- Update documentation [#98](https://github.com/datagouv/hydra/pull/98) and [#106](https://github.com/datagouv/hydra/pull/106)
- Add linter and formatter with `pyproject.toml` config, add lint and formatting step in CI, add pre-commit hook to lint and format, update docs and lint and format the code [#99](https://github.com/datagouv/hydra/pull/99)
- Update `sentry-sdk` dependency, and update Sentry logic to be able to send environment, app version and profiling/performance info [#100](https://github.com/datagouv/hydra/pull/100)
- Basic cleaning: use Python 3.11 in CI, remove Pandas in project dependencies, add type hints, fix wrong type hints, remove deprecated version field in docker compose files, update `.gitignore` [#102] [https://github.com/datagouv/hydra/pull/102] and [#107](https://github.com/datagouv/hydra/pull/107)
- Add missing content-type for csv.gz [#103](https://github.com/datagouv/hydra/pull/103)
- Remove deprecated `pytz` module [#109](https://github.com/datagouv/hydra/pull/109)
- Refactor project structure to use DB classes for each DB table, with their factorized DB methods [#112](https://github.com/datagouv/hydra/pull/112) and [#55](https://github.com/datagouv/hydra/pull/55)
- Add tests coverage feature [#122](https://github.com/datagouv/hydra/pull/122)
- Refactor routes [#117](https://github.com/datagouv/hydra/pull/117)
- Fix Ruff configuration [#125](https://github.com/datagouv/hydra/pull/125)
- Add some API tests to improve coverage [#123](https://github.com/datagouv/hydra/pull/123)
- Fix health check endpoint route which was wrongly removed, and add test for API health check endpoint to make sure this endpoint is working as expected [#128](https://github.com/datagouv/hydra/pull/128)
- Add basic authentication via API key using a bearer token auth for all POST/PUT/DELETE endpoints [#130](https://github.com/datagouv/hydra/pull/130)
- Simplify getting Sentry info by loading pyproject.toml info in config [#138](https://github.com/datagouv/hydra/pull/138)
- Add a `POST` `/api/checks/` route for force crawling [#118](https://github.com/datagouv/hydra/pull/118)
- Update `csv-detective` to 0.7.2 which doesn't include yanked version of `requests` anymore [#142](https://github.com/datagouv/hydra/pull/142) and [#144](https://github.com/datagouv/hydra/pull/144)
- Update resource statuses in DB when crawling and analysing, and add resource status route [#119](https://github.com/datagouv/hydra/pull/119)
- Simplify `save_as_parquet` method, and fix type not compatible with Python 3.9; remove unused import [#156](https://github.com/datagouv/hydra/pull/156)
- Fix and simplify project metadata loading [#157](https://github.com/datagouv/hydra/pull/157)
- Pin Numpy version to 1.26.4 to avoid conflicts with pandas and csv-detective

## 1.0.1 (2023-01-04)

- Packaging-fix release

## 1.0.0 (2023-01-04)

- Initial version
