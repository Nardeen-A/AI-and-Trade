# --------------------------------------------------
# 1. PATHS
# --------------------------------------------------
data_dir   <- "G:/data/academic_individual_position"
output_dir <- "E:/UofT/00_RA/T_AI/output/data/migration and AI count"
db_path    <- file.path(output_dir, "ai_migration.duckdb")

if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

# --------------------------------------------------
# 2. LIST REAL PARQUET FILES ONLY
# excludes hidden sidecar files like ._filename.parquet
# does NOT delete anything
# --------------------------------------------------
parquet_files <- list.files(
  path = data_dir,
  pattern = "\\.parquet$",
  full.names = TRUE
)

parquet_files <- parquet_files[!grepl("(^|/)\\._", gsub("\\\\", "/", parquet_files))]

cat("Number of parquet files being scanned:", length(parquet_files), "\n")

# --------------------------------------------------
# 3. CONNECT
# --------------------------------------------------
con <- dbConnect(duckdb::duckdb(), dbdir = db_path)
dbExecute(con, "PRAGMA threads=8")

# --------------------------------------------------
# 4. BUILD FILE LIST FOR DUCKDB
# --------------------------------------------------
files_sql <- paste(
  sprintf("'%s'", gsub("'", "''", gsub("\\\\", "/", parquet_files))),
  collapse = ", "
)

scan_sql <- sprintf("parquet_scan([%s])", files_sql)

# --------------------------------------------------
# 3. AI TITLE PATTERN
# --------------------------------------------------
ai_pattern <- paste(
  c(
    "artificial intelligence",
    "machine learning",
    "machine learning engineer",
    "ml engineer",
    "\\bai engineer\\b",
    "\\bai scientist\\b",
    "computer vision",
    "computer vision engineer",
    "\\bnlp\\b",
    "reinforcement learning",
    "natural language processing",
    "deep learning",
    "neural network",
    "large language model",
    "neural networks",
    "\\bllm\\b",
    "\\bai\\b",
    "\\bml\\b",
    "generative ai",
    "\\bgenai\\b"
  ),
  collapse = "|"
)

# --------------------------------------------------
# 6. AI WORKER STOCK
# distinct AI users by country x state x year
# --------------------------------------------------
ai_stock_query <- sprintf("
SELECT
    country,
    state,
    EXTRACT(YEAR FROM TRY_CAST(startdate AS DATE)) AS year,
    COUNT(DISTINCT user_id) AS ai_user_count
FROM %s
WHERE title_translated IS NOT NULL
  AND regexp_matches(lower(title_translated), '%s')
  AND country IS NOT NULL
  AND state IS NOT NULL
  AND trim(country) <> ''
  AND trim(state) <> ''
  AND lower(trim(country)) <> 'empty'
  AND lower(trim(state)) <> 'empty'
  AND TRY_CAST(startdate AS DATE) IS NOT NULL
GROUP BY country, state, year
ORDER BY ai_user_count DESC
", scan_sql, ai_pattern)

ai_stock <- dbGetQuery(con, ai_stock_query)

# --------------------------------------------------
# 7. AI MIGRATION FLOWS
# worker-by-worker country transitions
# --------------------------------------------------
migration_query <- sprintf("
WITH ai_jobs AS (
    SELECT
        user_id,
        trim(country) AS country,
        TRY_CAST(startdate AS DATE) AS start_dt,
        EXTRACT(YEAR FROM TRY_CAST(startdate AS DATE)) AS year
    FROM %s
    WHERE title_translated IS NOT NULL
      AND regexp_matches(lower(title_translated), '%s')
      AND country IS NOT NULL
      AND trim(country) <> ''
      AND lower(trim(country)) <> 'empty'
      AND TRY_CAST(startdate AS DATE) IS NOT NULL
),
ordered_jobs AS (
    SELECT
        user_id,
        country,
        start_dt,
        year,
        LAG(country) OVER (
            PARTITION BY user_id
            ORDER BY start_dt
        ) AS previous_country
    FROM ai_jobs
),
moves AS (
    SELECT
        user_id,
        previous_country AS from_country,
        country AS to_country,
        year AS move_year
    FROM ordered_jobs
    WHERE previous_country IS NOT NULL
      AND previous_country <> country
)
SELECT
    from_country,
    to_country,
    move_year,
    COUNT(*) AS n_moves,
    COUNT(DISTINCT user_id) AS n_unique_movers
FROM moves
GROUP BY from_country, to_country, move_year
ORDER BY n_moves DESC
", scan_sql, ai_pattern)

migration_results <- dbGetQuery(con, migration_query)

# --------------------------------------------------
# CLOSE CONNECTION
# --------------------------------------------------
dbDisconnect(con, shutdown = TRUE)


proj.path <- getwd()

write.csv(
  ai_stock,
  file.path(proj.path, "output", "data", "ai_stock.csv"),
  row.names = FALSE
)
write.csv(
  migration_results,
  file.path(proj.path, "output", "data", "migration.csv"),
  row.names = FALSE
)
