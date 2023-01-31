# udata-hydra 🦀

`udata-hydra` is an async metadata crawler for [data.gouv.fr](https://www.data.gouv.fr).

URLs are crawled via _aiohttp_, catalog and crawled metadata are stored in a _PostgreSQL_ database.

Since it's called _hydra_, it also has mythical powers embedded:
- analyse remote resource metadata over time to detect changes in the smartest way possible
- if the remote resource is a CSV, convert it to a PostgreSQL table, ready for APIfication
- send crawl and analysis info to a udata instance

## CLI

### Create database structure

Install udata-hydra dependencies and cli.
`poetry install`

`poetry run udata-hydra migrate`

### Load (UPSERT) latest catalog version from data.gouv.fr

`udata-hydra load-catalog`

## Crawler

`udata-hydra-crawl`

It will crawl (forever) the catalog according to config set in `config.py`.

`BATCH_SIZE` URLs are queued at each loop run.

The crawler will start with URLs never checked and then proceed with URLs crawled before `SINCE` interval. It will then wait until something changes (catalog or time).

There's a by-domain backoff mecanism. The crawler will wait when, for a given domain in a given batch, `BACKOFF_NB_REQ` is exceeded in a period of `BACKOFF_PERIOD` seconds. It will retry until the backoff is lifted.

If an URL matches one of the `EXCLUDED_PATTERNS`, it will never be checked.

## Worker

A job queuing system is used to process long-running tasks. Launch the worker with the following command:

`poetry run rq worker -c udata_hydra.worker`

Monitor worker status:

`poetry run rq info -c udata_hydra.worker --interval 1`

## CSV conversion to database

Converted CSV tables will be stored in the database specified via `config.DATABASE_URL_CSV`. For tests it's same database as for the catalog. Locally, `docker compose` will launch two distinct database containers.

## API

### Run

```
poetry install
poetry run adev runserver udata_hydra/app.py
```

### Get latest check

Works with `?url={url}` and `?resource_id={resource_id}`.

```
$ curl -s "http://localhost:8000/api/checks/latest/?url=http://opendata-sig.saintdenis.re/datasets/661e19974bcc48849bbff7c9637c5c28_1.csv" | json_pp
{
   "status" : 200,
   "catalog_id" : 64148,
   "deleted" : false,
   "error" : null,
   "created_at" : "2021-02-06T12:19:08.203055",
   "response_time" : 0.830198049545288,
   "url" : "http://opendata-sig.saintdenis.re/datasets/661e19974bcc48849bbff7c9637c5c28_1.csv",
   "domain" : "opendata-sig.saintdenis.re",
   "timeout" : false,
   "id" : 114750,
   "dataset_id" : "5c34944606e3e73d4a551889",
   "resource_id" : "b3678c59-5b35-43ad-9379-fce29e5b56fe",
   "headers" : {
      "content-disposition" : "attachment; filename=\"xn--Dlimitation_des_cantons-bcc.csv\"",
      "server" : "openresty",
      "x-amz-meta-cachetime" : "191",
      "last-modified" : "Wed, 29 Apr 2020 02:19:04 GMT",
      "content-encoding" : "gzip",
      "content-type" : "text/csv",
      "cache-control" : "must-revalidate",
      "etag" : "\"20415964703d9ccc4815d7126aa3a6d8\"",
      "content-length" : "207",
      "date" : "Sat, 06 Feb 2021 12:19:08 GMT",
      "x-amz-meta-contentlastmodified" : "2018-11-19T09:38:28.490Z",
      "connection" : "keep-alive",
      "vary" : "Accept-Encoding"
   }
}
```

### Get all checks for an URL or resource

Works with `?url={url}` and `?resource_id={resource_id}`.

```
$ curl -s "http://localhost:8000/api/checks/all/?url=http://www.drees.sante.gouv.fr/IMG/xls/er864.xls" | json_pp
[
   {
      "domain" : "www.drees.sante.gouv.fr",
      "dataset_id" : "53d6eadba3a72954d9dd62f5",
      "timeout" : false,
      "deleted" : false,
      "response_time" : null,
      "error" : "Cannot connect to host www.drees.sante.gouv.fr:443 ssl:True [SSLCertVerificationError: (1, \"[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.drees.sante.gouv.fr'. (_ssl.c:1122)\")]",
      "catalog_id" : 232112,
      "url" : "http://www.drees.sante.gouv.fr/IMG/xls/er864.xls",
      "headers" : {},
      "id" : 165107,
      "created_at" : "2021-02-06T14:32:47.675854",
      "resource_id" : "93dfd449-9d26-4bb0-a6a9-ee49b1b8a4d7",
      "status" : null
   },
   {
      "timeout" : false,
      "deleted" : false,
      "response_time" : null,
      "error" : "Cannot connect to host www.drees.sante.gouv.fr:443 ssl:True [SSLCertVerificationError: (1, \"[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.drees.sante.gouv.fr'. (_ssl.c:1122)\")]",
      "domain" : "www.drees.sante.gouv.fr",
      "dataset_id" : "53d6eadba3a72954d9dd62f5",
      "created_at" : "2020-12-24T17:06:58.158125",
      "resource_id" : "93dfd449-9d26-4bb0-a6a9-ee49b1b8a4d7",
      "status" : null,
      "catalog_id" : 232112,
      "url" : "http://www.drees.sante.gouv.fr/IMG/xls/er864.xls",
      "headers" : {},
      "id" : 65092
   }
]
```

### Get crawling status

```
$ curl -s "http://localhost:8000/api/status/crawler/" | json_pp
{
   "fresh_checks_percentage" : 0.4,
   "pending_checks" : 142153,
   "total" : 142687,
   "fresh_checks" : 534,
   "checks_percentage" : 0.4
}
```

### Get worker status

```
$ curl -s "http://localhost:8000/api/status/worker/" | json_pp
{
   "queued" : {
      "default" : 0,
      "high" : 825,
      "low" : 655
   }
}
```

### Get crawling stats

```
$ curl -s "http://localhost:8000/api/stats/" | json_pp
{
   "status" : [
      {
         "count" : 525,
         "percentage" : 98.3,
         "label" : "ok"
      },
      {
         "label" : "error",
         "percentage" : 1.3,
         "count" : 7
      },
      {
         "label" : "timeout",
         "percentage" : 0.4,
         "count" : 2
      }
   ],
   "status_codes" : [
      {
         "code" : 200,
         "count" : 413,
         "percentage" : 78.7
      },
      {
         "code" : 501,
         "percentage" : 12.4,
         "count" : 65
      },
      {
         "percentage" : 6.1,
         "count" : 32,
         "code" : 404
      },
      {
         "code" : 500,
         "percentage" : 2.7,
         "count" : 14
      },
      {
         "code" : 502,
         "count" : 1,
         "percentage" : 0.2
      }
   ]
}
```

## Using Webhook integration

** Set the config values**

Create a `config.toml` where your service and commands are launched, or specify a path to a TOML file via the `HYDRA_SETTINGS` environment variable. `config.toml` or equivalent will override values from `udata_hydra/config_default.toml`, lookup there for values that can/need to be defined.

```toml
UDATA_URI = "https://dev.local:7000/api/2"
UDATA_URI_API_KEY = "example.api.key"
SENTRY_DSN = "https://{my-sentry-dsn}"
```

The webhook integration sends HTTP messages to `udata` when resources are analyzed or checked to fill resources extras.

Regarding analysis, there is a phase called "change detection". It will try to guess if a resource has been modified based on different criterions:
- harvest modified date in catalog
- content-length and last-modified headers
- checksum comparison over time

The payload should look something like:

```json
{
   "analysis:filesize": 91661,
   "analysis:mime-type": "application/zip",
   "analysis:checksum": "bef1de04601dedaf2d127418759b16915ba083be",
   "analysis:last-modified-at": "2022-11-27T23:00:54.762000",
   "analysis:last-modified-detection": "harvest-resource-metadata",
}
```

## Development

### docker-compose

Multiple docker-compose files are provided:
- a minimal `docker-compose.yml` with two PostgreSQL containers (one for catalog and metadata, the other for converted CSV to database)
- `docker-compose.broker.yml` adds a Redis broker
- `docker-compose.test.yml` launches a test DB, needed to run tests

NB: you can launch compose from multiple files like this: `docker-compose -f docker-compose.yml -f docker-compose.test.yml up`

### Logging & Debugging

The log level can be adjusted using the environment variable LOG_LEVEL.
For example, to set the log level to `DEBUG` when initializing the database, use `LOG_LEVEL="DEBUG" udata-hydra init_db `.

### Writing a migration

1. Add a file named `migrations/{YYYYMMDD}_{description}.sql` and write the SQL you need to perform migration.
2. `udata-hydra migrate` will migrate the database as needeed.

## Deployment

3 services need to be deployed for the full stack to run:
- worker
- api / app
- crawler

Refer to each section to learn how to launch them. The only differences from dev to prod are:
- use `HYDRA_SETTINGS` env var to point to your custom `config.toml`
- use `HYDRA_APP_SOCKET_PATH` to configure where aiohttp should listen to a [reverse proxy connection (eg nginx)](https://docs.aiohttp.org/en/stable/deployment.html#nginx-configuration) and use `udata-hydra-app` to launch the app server
