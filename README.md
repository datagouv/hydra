# udata-datalake-service

This service's purpose is to build the udata datalake, by saving uploaded resources on a Minio database.

## Installation

Install **udata-datalake-service**:

```shell
pip install udata-datalake-service
```

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
```

## Usage

Start the Kafka consumer:

```shell
udata-datalake-service consume
```

Start the Celery worker:

```shell
udata-datalake-service work
```
