# Minio
MINIO_ENDPOINT   = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadminpassword"
MINIO_SECURE     = False

RAW_BUCKET       = "crypto-raw-data"
PROCESSED_BUCKET = "crypto-processed-data"

# Postgres
POSTGRES_CONN_STR = "postgresql+psycopg2://de_user:de_password@localhost:5432/crypto_dw"

# Coingeko api 
COINGECKO_URL    = "https://api.coingecko.com/api/v3/coins/markets"
TOP_N_COINS      = 20