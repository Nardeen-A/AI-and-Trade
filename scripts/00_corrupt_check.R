dir_path <- "E:\\data\\academic_postings_unified_individual"
files <- list.files(dir_path, pattern = "\\.parquet$", full.names = TRUE)

is_valid_parquet <- function(path) {
  # must be big enough to contain footer
  sz <- file.info(path)$size
  if (is.na(sz) || sz < 12) return(FALSE)
  
  con <- file(path, "rb")
  on.exit(close(con), add = TRUE)
  
  start <- readBin(con, "raw", n = 4)
  seek(con, where = -4, origin = "end")
  end <- readBin(con, "raw", n = 4)
  
  identical(start, charToRaw("PAR1")) && identical(end, charToRaw("PAR1"))
}

ok <- vapply(files, is_valid_parquet, logical(1))
bad_files <- files[!ok]
good_files <- files[ok]

length(bad_files)
head(bad_files, 10)
