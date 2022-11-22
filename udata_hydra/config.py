import os

from dotenv import load_dotenv

load_dotenv("../.env")

# -- general settings -- #
LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG")
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgres://postgres:postgres@localhost:5432/postgres"
)

# -- crawler settings -- #

# sql LIKE syntax
EXCLUDED_PATTERNS = [
    'http%geo.data.gouv.fr%',
    # opendatasoft shp
    '%?format=shp%',
]
# no backoff for those domains
NO_BACKOFF_DOMAINS = [
    'static.data.gouv.fr',
    'www.data.gouv.fr',
    # dead domain, no need to backoff
    'inspire.data.gouv.fr',
]
# max number of _completed_ requests per domain per period
BACKOFF_NB_REQ = 180
BACKOFF_PERIOD = 360  # in seconds
# crawl batch size, beware of open file limits
BATCH_SIZE = 100
# crawl url if last check is older than
SINCE = '1w'
# seconds to wait for between batches
SLEEP_BETWEEN_BATCHES = int(os.environ.get("SLEEP_BETWEEN_BATCHES", "60"))
# max download filesize in bytes (100 MB)
MAX_FILESIZE_ALLOWED = 104857600

# -- Webhook integration config -- #
ENABLE_WEBHOOK = True
UDATA_URI = os.environ.get("UDATA_URI")
UDATA_URI_API_KEY = os.environ.get("UDATA_URI_API_KEY")

# -- Minio / datalake settings -- #
SAVE_TO_MINIO = False
MINIO_FOLDER = os.environ.get("MINIO_FOLDER", "folder")
MINIO_URL = os.environ.get("MINIO_URL")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET")
MINIO_USER = os.environ.get("MINIO_USER")
MINIO_PWD = os.environ.get("MINIO_PWD")
