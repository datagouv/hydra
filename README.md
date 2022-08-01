# udata-hydra 🦀

`udata-hydra` is an async metadata crawler for [data.gouv.fr](https://www.data.gouv.fr).

URLs are crawled via _aiohttp_, catalog and crawled metadata are stored in a _PostgreSQL_ database.

![](docs/screenshot.png)

## CLI

### Create database structure

Install udata-hydra dependencies and cli.
`make deps`

`udata-hydra init-db`

### Load (UPSERT) latest catalog version from data.gouv.fr

`udata-hydra load-catalog`

## Crawler

`udata-hydra-crawl`

It will crawl (forever) the catalog according to config set in `config.py`.

`BATCH_SIZE` URLs are queued at each loop run.

The crawler will start with URLs never checked and then proceed with URLs crawled before `SINCE` interval. It will then wait until something changes (catalog or time).

There's a by-domain backoff mecanism. The crawler will wait when, for a given domain in a given batch, `BACKOFF_NB_REQ` is exceeded in a period of `BACKOFF_PERIOD` seconds. It will sleep and retry until the backoff is lifted.

If an URL matches one of the `EXCLUDED_PATTERNS`, it will never be checked.

A curses interface is available via:

`HYDRA_CURSES_ENABLED=True udata-hydra-crawl`

## API

### Run

```
pip install -r requirements.txt
adev runserver udata-hydra/app.py
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

### Get modification date on resources

This tries to find a modification date for a given resource, by order of priority:
1. `last-modified` header if any
2. `content-length` comparison over multiple checks if any (precision depends on crawling frequency)

Works with `?url={url}` and `?resource_id={resource_id}`.

```
$ curl -s "http://localhost:8000/api/changed/?resource_id=f2d3e1ad-4d7d-46fc-91f8-c26f02c1e487" | json_pp
{
   "changed_at" : "2014-09-15T14:51:52",
   "detection" : "last-modified"
}
```

```
$ curl -s "http://localhost:8000/api/changed/?resource_id=f2d3e1ad-4d7d-46fc-91f8-c26f02c1e487" | json_pp
{
   "changed_at" : "2020-09-15T14:51:52",
   "detection" : "content-length"
}
```

### Get crawling status

```
$ curl -s "http://localhost:8000/api/status/" | json_pp
{
   "fresh_checks_percentage" : 0.4,
   "pending_checks" : 142153,
   "total" : 142687,
   "fresh_checks" : 534,
   "checks_percentage" : 0.4
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

### Using Kafka integration

** Set the environment variables **
Rename the `.env.sample` to `.env` and fill it with the right values.

```shell
REDIS_URL = redis://localhost:6380/0
REDIS_HOST = localhost
REDIS_PORT = 6380
KAFKA_HOST = localhost
KAFKA_PORT = 9092
KAFKA_API_VERSION = 2.5.0
MINIO_URL = https://object.local.dev/
MINIO_USER = sample_user
MINIO_BUCKET = benchmark-de
MINIO_PWD = sample_pwd
MINIO_FOLDER = data
MAX_FILESIZE_ALLOWED = 1e9
UDATA_INSTANCE_NAME = udata
```

The `kafka_integration` module retrieves messages with the topics `resource.created`, `resource.modified` and `resource.deleted` sent by `udata`.
The Kafka instance URI, Hydra API URL and Data Gouv API URL to be used can be defined in `udata-hydra/config` or overwritten with env variables.
It can be launched using the CLI: `udata-hydra run_kafka_integration`.
This will mark the corresponding resources as highest priority for the next crawling batch.


### Logging & Debugging
The log level can be adjusted using the environment variable LOGLEVEL.
For example, to set the log level to `DEBUG` when running the Kafka integration, use `LOGLEVEL="DEBUG" udata-hydra run_kafka_integration `.

## TODO

- [x] non curse interface :sad:
- [x] tests
- [x] expose summary/status as API
- [x] change detection API on url / resource
- [x] handle `GET` request when `501` on `HEAD`
- [x] handle `GET` requests for some domains
- [ ] denormalize interesting headers (length, mimetype, last-modified...)
- [x] some sort of dashboard (dash?), or just plug postgrest and handle that elsewhere
- [x] custom config file for pandas_profiling
- [x] move API endpoints to /api endpoints
