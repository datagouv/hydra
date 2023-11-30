# Changelog

## Current (in progress)

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
- Use latest csv-detective version [#77](https://github.com/etalab/udata-hydra/pull/77)
- Compare content type / length to check if changed [#78](https://github.com/etalab/udata-hydra/pull/78) [#79](https://github.com/etalab/udata-hydra/pull/79)
- Create a list of exceptions to analyse despite larger size [#85](https://github.com/etalab/udata-hydra/pull/85)
- Enable csv.gz analysis [#84](https://github.com/etalab/udata-hydra/pull/84)


## 1.0.1 (2023-01-04)

- Packaging-fix release

## 1.0.0 (2023-01-04)

- Initial version
