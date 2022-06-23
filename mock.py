import json

from kafka import KafkaProducer

DOCUMENT_LOURD = {
    "id": "5b7ffc618b4c4169d30727e0",
    "title": "Base Sirene des entreprises et de leurs établissements (SIREN, SIRET)",
    "description": "satisfaite.",
    "acronym": None,
    "url": "http://dev-02.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/",
    "tags": [
        "association",
        "companies",
        "entreprises",
        "etablissements",
        "register",
        "registre",
        "repertoire",
        "siren",
        "sirene",
        "siret",
    ],
    "license": "lov2",
    "badges": ["spd"],
    "frequency": "monthly",
    "created_at": "2018-08-24T14:38:57",
    "views": 29530,
    "followers": 56,
    "reuses": 19,
    "featured": 0,
    "resources_count": 15,
    "organization": {
        "id": "534fff81a3a7292c64a77e5c",
        "name": "Institut National de la Statistique et des Etudes Economiques (Insee)",
        "public_service": 1,
        "followers": 615,
    },
    "owner": None,
    "format": [
        "zip",
        "zip",
        "zip",
        "zip",
        "zip",
        "pdf",
        "pdf",
        "pdf",
        "pdf",
        "pdf",
        "csv",
        "csv",
        "csv",
        "csv",
        "csv",
    ],
    "schema": [],
    "extras": {},
    "resources": [
        {
            "id": "88fbb6b4-0320-443e-b739-b4376a012c32",
            "url": "https://files.data.gouv.fr/insee-sirene/StockEtablissementHistorique_utf8.zip",
            "format": "zip",
            "title": "Sirene : Fichier StockEtablissementHistorique du 01 Mars 2022",
            "schema": {},
            "latest": "http://dev-02.data.gouv.fr/fr/datasets/r/88fbb6b4-0320-443e-b739-b4376a012c32",
            "description": None,
            "filetype": "remote",
            "type": "main",
            "created_at": "2021-09-01T07:46:43",
            "modified": "2022-03-01T22:57:39",
            "published": "2022-03-01T21:57:39",
            "extras": {
                "check:status": 204,
                "check:available": True,
                "check:date": "2022-01-24T15:28:58",
                "check:count-availability": 3,
            },
        },
        {
            "id": "0651fb76-bcf3-4f6a-a38d-bc04fa708576",
            "url": "https://files.data.gouv.fr/insee-sirene/StockEtablissement_utf8.zip",
            "format": "zip",
            "title": "Sirene : Fichier StockEtablissement du 01 Mars 2022",
            "schema": {},
            "latest": "http://dev-02.data.gouv.fr/fr/datasets/r/0651fb76-bcf3-4f6a-a38d-bc04fa708576",
            "description": None,
            "filetype": "remote",
            "type": "main",
            "created_at": "2021-09-01T07:39:48",
            "modified": "2022-03-01T22:57:39",
            "published": "2022-03-01T21:57:39",
            "extras": {},
        },
        {
            "id": "0835cd60-2c2a-497b-bc64-404de704ce89",
            "url": "https://files.data.gouv.fr/insee-sirene/StockUniteLegaleHistorique_utf8.zip",
            "format": "zip",
            "title": "Sirene : Fichier StockUniteLegaleHistorique du 01 Mars 2022",
            "schema": {},
            "latest": "http://dev-02.data.gouv.fr/fr/datasets/r/0835cd60-2c2a-497b-bc64-404de704ce89",
            "description": None,
            "filetype": "remote",
            "type": "main",
            "created_at": "2021-09-01T07:21:25",
            "modified": "2022-03-01T22:57:39",
            "published": "2022-03-01T21:57:38",
            "extras": {},
        },
        {
            "id": "9c4d5d9c-4bbb-4b9c-837a-6155cb589e26",
            "url": "https://files.data.gouv.fr/insee-sirene/StockEtablissementLiensSuccession_utf8.zip",
            "format": "zip",
            "title": "Sirene : Fichier StockEtablissementLiensSuccession du 01 Mars 2022",
            "schema": {},
            "latest": "http://dev-02.data.gouv.fr/fr/datasets/r/9c4d5d9c-4bbb-4b9c-837a-6155cb589e26",
            "description": None,
            "filetype": "remote",
            "type": "main",
            "created_at": "2021-09-01T05:35:10",
            "modified": "2022-03-01T22:57:39",
            "published": "2022-03-01T21:57:39",
            "extras": {},
        },
        {
            "id": "825f4199-cadd-486c-ac46-a65a8ea1a047",
            "url": "https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip",
            "format": "zip",
            "title": "Sirene : Fichier StockUniteLegale du 01 Mars 2022",
            "schema": {},
            "latest": "http://dev-02.data.gouv.fr/fr/datasets/r/825f4199-cadd-486c-ac46-a65a8ea1a047",
            "description": None,
            "filetype": "remote",
            "type": "main",
            "created_at": "2021-09-01T05:34:07",
            "modified": "2022-03-01T22:57:39",
            "published": "2022-03-01T21:57:39",
            "extras": {},
        }
    ],
    "geozones": [
        {
            "id": "country:fr",
            "name": "France",
            "keys": ["fr", "fra", "250", "fr", "3017382", "fr", "2202162"],
        },
        {"id": "country-group:ue"},
        {"id": "country-group:world"},
    ],
    "granularity": "other",
}


DOCUMENT_LEGER = {
    "id": "6065c5cd9f89bfd828bcbf5c",
    "title": "Nombre de personnes rickrollées sur data.gouv.fr",
    "description": "A l'occasion du 1er avril, data.gouv.fr s'est donné pour mission de [Rickroll](https://fr.wikipedia.org/wiki/Rickroll) un nombre important d'usagers. Dans un souci de transparence, les indicateurs relatifs à cette mission sont publiés ici en open data sous forme de fichier CSV.\n\nLes données sont issues de [stats.data.gouv.fr](https://stats.data.gouv.fr/index.php?module=CoreHome&action=index&idSite=109&period=range&date=previous30#?idSite=109&period=day&date=2021-04-01&segment=&category=General_Actions&subcategory=General_Outlinks).",
    "acronym": None,
    "url": "http://dev-02.data.gouv.fr/fr/datasets/nombre-de-personnes-rickrollees-sur-data-gouv-fr/",
    "tags": ["never-gonna-give-you-up"],
    "license": "lov2",
    "badges": [],
    "frequency": "punctual",
    "created_at": "2021-04-01T15:08:29",
    "views": 160,
    "followers": 2,
    "reuses": 0,
    "featured": 0,
    "resources_count": 1,
    "organization": {
        "id": "534fff75a3a7292c64a77de4",
        "name": "Etalab",
        "public_service": 1,
        "followers": 356,
    },
    "owner": None,
    "format": ["csv"],
    "schema": [],
    "extras": {},
    "resources": [
        {
            "id": "503c39b0-58d8-45fe-ace0-55795bb0a6b7",
            "url": "https://static.data.gouv.fr/resources/nombre-de-personnes-rickrollees-sur-data-gouv-fr/20210401-182031/rickroll.csv",
            "format": "csv",
            "title": "rickroll.csv",
            "schema": {},
            "latest": "http://dev-02.data.gouv.fr/fr/datasets/r/503c39b0-58d8-45fe-ace0-55795bb0a6b7",
            "description": None,
            "filetype": "file",
            "type": "main",
            "created_at": "2021-04-01T15:10:12",
            "modified": "2021-04-01T18:20:33",
            "published": "2021-04-01T15:10:12",
            "extras": {},
        }
    ],
    "temporal_coverage_start": "2021-04-01T00:00:00",
    "temporal_coverage_end": "2021-04-02T00:00:00",
}


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    key = "5b7ffc618b4c4169d30727e0".encode("utf-8")

    value = {
        "service": "udata",
        "data": DOCUMENT_LEGER,
        "meta": {"message_type": "index"},
    }

    for i in range(10):
        print('SEND ONE')
        producer.send("dataset", value=value, key=key)
        producer.flush()


if __name__ == "__main__":
    main()
