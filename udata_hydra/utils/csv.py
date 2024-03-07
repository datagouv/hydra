import json


async def detect_tabular_from_headers(check) -> bool:
    """
    Determine from content-type header if file looks like:
        - a csv
        - a csv.gz (1. is the file's content binary?, 2. does the URL contain "csv.gz"?)
        - a xls(x)
        - an ods
    """
    headers = json.loads(check["headers"] or "{}")
    return any(
        headers.get("content-type", "").lower().startswith(ct) for ct in [
            "application/csv", "text/plain", "text/csv"
        ]
    ) or (
        any([
            headers.get("content-type", "").lower().startswith(ct) for ct in [
                "application/octet-stream", "application/x-gzip"
            ]
        ]) and "csv.gz" in check.get("url", "")
    ) or (
        any([
            headers.get("content-type", "").lower().startswith(ct) for ct in [
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                # https://static.data.gouv.fr/resources/election-presidentielle-des-10-et-24-avril-2022-resultats-definitifs-du-1er-tour/20220414-152438/resultats-par-niveau-cirlg-t1-france-entiere.xlsx
                "application/vnd.ms-excel",
                # https://www.data.gouv.fr/api/1/datasets/elections-municipales-2020-liste-des-candidats-elus-au-t1-et-liste-des-communes-entierement-pourvues/resources/7e94dc91-26d4-4926-8fa1-277ee948d536/
            ]
        ])
        # and "xls" in check.get("url", "")
    ) or (
        any([
            headers.get("content-type", "").lower().startswith(ct) for ct in [
                "application/vnd.oasis.opendocument.spreadsheet",
                # https://www.data.gouv.fr/api/1/datasets/liste-des-immeubles-proteges-au-titre-des-monuments-historiques-archives/resources/e898c3bd-ecd9-4ab2-b633-7ef2c075d842/
                # https://www.data.gouv.fr/api/1/datasets/observatoire-de-la-qualite-des-demarches-en-ligne/resources/9cb0e3a4-fd7d-458b-8ed4-d20312eeef41
            ]
        ])
        # and "ods" in check.get("url", "")
    )
