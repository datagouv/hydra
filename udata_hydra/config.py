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
SLEEP_BETWEEN_BATCHES = 60

# -- Webhook integration config -- #
ENABLE_WEBHOOK = True
UDATA_URI = ''
UDATA_URI_API_KEY = ''
