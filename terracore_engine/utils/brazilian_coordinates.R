# ==============================================================================
# Shared Utility Functions for Air Pollution & Pollinator Networks Project
# ==============================================================================
# Author: Science Team (coordination by Dante AI)
# Project: TerraRisk - Air Pollution Impact on Pollinator Networks
# Description: Common functions used by CAMS and ERA5 extraction scripts
# ==============================================================================

# Required packages (ensure these are installed)
required_packages <- c("dplyr", "readr", "readxl", "sf", "lubridate", "glue", "httr", "jsonlite")

# Check and load packages
for (pkg in required_packages) {
  if (!require(pkg, quietly = TRUE, character.only = TRUE)) {
    stop(glue::glue("Package '{pkg}' is not installed. Run: install.packages('{pkg}')"))
  }
}

# ==============================================================================
# COPERNICUS API CONSTANTS
# ==============================================================================

CDS_BASE_URL <- "https://cds.climate.copernicus.eu/api/retrieve/v1"
ADS_BASE_URL <- "https://ads.atmosphere.copernicus.eu/api/retrieve/v1"

# ==============================================================================
# COORDINATE CLEANING
# ==============================================================================

#' Fix Brazilian CSV Coordinate Format
#'
#' Brazilian CSV exports use dots as thousands separators, which breaks
#' coordinates. This function reconstructs valid lat/lon values.
#'
#' @param coord_string Character string with dots (e.g., "3.732.028.297")
#' @param coord_type Character "lon" or "lat" (helps validate range)
#'
#' @return Numeric coordinate value or NA if invalid
#'
#' @examples
#' fix_brazilian_coordinates("38.462.539", "lat")  # -> 38.462539
#' fix_brazilian_coordinates("-90.825.881", "lon") # -> -90.825881
#' fix_brazilian_coordinates("3.732.028.297", "lat") # -> 37.32028297
#' fix_brazilian_coordinates("-324.788.624", "lon") # -> -3.24788624
fix_brazilian_coordinates <- function(coord_string, coord_type = "lon") {

  # Handle NA or empty strings
  if (is.na(coord_string) || coord_string == "") {
    return(NA_real_)
  }

  # Store original sign
  is_negative <- grepl("^-", coord_string)

  # Remove all dots and sign
  clean_digits <- gsub("[.-]", "", coord_string)

  # If empty after cleaning, return NA
  if (clean_digits == "") {
    return(NA_real_)
  }

  # Define valid ranges
  valid_range <- if (coord_type == "lat") c(-90, 90) else c(-180, 180)

  # Try different decimal positions
  # Strategy: place decimal at positions that yield valid coordinates
  n_digits <- nchar(clean_digits)

  for (decimal_pos in 1:(n_digits - 1)) {
    # Insert decimal point
    integer_part <- substr(clean_digits, 1, decimal_pos)
    decimal_part <- substr(clean_digits, decimal_pos + 1, n_digits)

    # Reconstruct number
    value <- as.numeric(paste0(integer_part, ".", decimal_part))

    # Apply sign
    if (is_negative) value <- -value

    # Check if in valid range
    if (!is.na(value) && value >= valid_range[1] && value <= valid_range[2]) {
      return(value)
    }
  }

  # If no valid position found, log warning and return NA
  warning(glue("Could not fix coordinate: '{coord_string}' (type: {coord_type})"))
  return(NA_real_)
}

# ==============================================================================
# DATA LOADING
# ==============================================================================

#' Load and Clean Network Metadata
#'
#' Reads the pollinator network metadata CSV and optional dates Excel file.
#' Cleans Brazilian coordinate format and merges temporal information.
#'
#' @param metadata_path Path to semicolon-delimited CSV file
#' @param dates_path Optional path to Excel file with start/end dates
#'
#' @return Data frame with columns: Netcode, lon, lat, country, region,
#'         start_date, end_date (if dates_path provided)
#'
#' @examples
#' networks <- load_network_metadata(
#'   "data/Networks_metadata.csv",
#'   "data/Dates_2025.xlsx"
#' )
load_network_metadata <- function(metadata_path, dates_path = NULL) {

  # Check file exists
  if (!file.exists(metadata_path)) {
    stop(glue("Metadata file not found: {metadata_path}"))
  }

  log_message(glue("Loading network metadata from: {metadata_path}"), NULL)

  # Read semicolon-delimited CSV
  # Expected columns: Netcode, Longitude, Latitude, Country, Region
  df <- read_delim(
    metadata_path,
    delim = ";",
    col_types = cols(
      Netcode = col_character(),
      Longitude = col_character(),  # Read as character first
      Latitude = col_character(),
      Country = col_character(),
      Region = col_character()
    ),
    locale = locale(decimal_mark = ","),  # Brazilian format
    show_col_types = FALSE
  )

  # Clean coordinates
  log_message("Cleaning coordinate format...", NULL)

  df <- df %>%
    mutate(
      lon = sapply(Longitude, fix_brazilian_coordinates, coord_type = "lon"),
      lat = sapply(Latitude, fix_brazilian_coordinates, coord_type = "lat")
    ) %>%
    select(Netcode, lon, lat, country = Country, region = Region)

  # Remove rows with invalid coordinates
  n_invalid <- sum(is.na(df$lon) | is.na(df$lat))
  if (n_invalid > 0) {
    warning(glue("Removed {n_invalid} networks with invalid coordinates"))
    df <- df %>% filter(!is.na(lon), !is.na(lat))
  }

  # Validate coordinate ranges
  invalid_coords <- df %>%
    filter(lon < -180 | lon > 180 | lat < -90 | lat > 90)

  if (nrow(invalid_coords) > 0) {
    warning(glue("Found {nrow(invalid_coords)} networks with out-of-range coordinates"))
    print(invalid_coords)
  }

  # Load temporal data if provided
  if (!is.null(dates_path) && file.exists(dates_path)) {
    log_message(glue("Loading temporal data from: {dates_path}"), NULL)

    dates_df <- read_excel(dates_path) %>%
      select(Netcode, start_date, end_date) %>%
      mutate(
        start_date = as.Date(start_date),
        end_date = as.Date(end_date)
      )

    # Merge with metadata
    df <- df %>%
      left_join(dates_df, by = "Netcode")

    log_message(glue("Merged temporal data for {nrow(dates_df)} networks"), NULL)
  }

  log_message(glue("Loaded {nrow(df)} networks with valid coordinates"), NULL)

  return(df)
}

# ==============================================================================
# SPATIAL OPERATIONS
# ==============================================================================

#' Create Regional Bounding Box
#'
#' Creates a bounding box for a region with buffer to ensure full coverage.
#'
#' @param networks_df Data frame with lon, lat, region columns
#' @param region Character string matching region name (e.g., "USA", "Tanzania")
#' @param buffer_deg Numeric buffer in degrees (default 1.0)
#'
#' @return Named list with xmin, xmax, ymin, ymax
#'
#' @examples
#' bbox <- create_regional_bbox(networks, "USA", buffer_deg = 1.5)
create_regional_bbox <- function(networks_df, region, buffer_deg = 1.0) {

  # Filter networks for this region
  region_nets <- networks_df %>%
    filter(region == !!region)

  if (nrow(region_nets) == 0) {
    stop(glue("No networks found for region: {region}"))
  }

  # Calculate bounding box with buffer
  bbox <- list(
    xmin = min(region_nets$lon) - buffer_deg,
    xmax = max(region_nets$lon) + buffer_deg,
    ymin = min(region_nets$lat) - buffer_deg,
    ymax = max(region_nets$lat) + buffer_deg
  )

  # Validate ranges
  bbox$xmin <- max(bbox$xmin, -180)
  bbox$xmax <- min(bbox$xmax, 180)
  bbox$ymin <- max(bbox$ymin, -90)
  bbox$ymax <- min(bbox$ymax, 90)

  log_message(glue(
    "Region {region}: bbox [{bbox$xmin}, {bbox$xmax}] x [{bbox$ymin}, {bbox$ymax}]"
  ), NULL)

  return(bbox)
}

# ==============================================================================
# VALIDATION
# ==============================================================================

#' Validate Extracted Values
#'
#' Checks extracted environmental data for outliers and missing values.
#'
#' @param df Data frame with variable column to validate
#' @param variable Character name of column to check
#' @param min_val Numeric expected minimum (flags values below this)
#' @param max_val Numeric expected maximum (flags values above this)
#'
#' @return List with validation statistics
#'
#' @examples
#' report <- validate_extracted_values(df, "pm2p5", 0, 500)
validate_extracted_values <- function(df, variable, min_val, max_val) {

  if (!variable %in% names(df)) {
    stop(glue("Variable '{variable}' not found in data frame"))
  }

  values <- df[[variable]]

  report <- list(
    variable = variable,
    total_records = length(values),
    missing = sum(is.na(values)),
    below_min = sum(values < min_val, na.rm = TRUE),
    above_max = sum(values > max_val, na.rm = TRUE),
    mean = mean(values, na.rm = TRUE),
    median = median(values, na.rm = TRUE),
    sd = sd(values, na.rm = TRUE),
    min = min(values, na.rm = TRUE),
    max = max(values, na.rm = TRUE)
  )

  # Print summary
  cat("\n===== Validation Report =====\n")
  cat(glue("Variable: {report$variable}\n"))
  cat(glue("Total records: {report$total_records}\n"))
  cat(glue("Missing values: {report$missing} ({round(100*report$missing/report$total_records, 1)}%)\n"))
  cat(glue("Below minimum ({min_val}): {report$below_min}\n"))
  cat(glue("Above maximum ({max_val}): {report$above_max}\n"))
  cat(glue("Range: [{round(report$min, 2)}, {round(report$max, 2)}]\n"))
  cat(glue("Mean ± SD: {round(report$mean, 2)} ± {round(report$sd, 2)}\n"))
  cat(glue("Median: {round(report$median, 2)}\n"))
  cat("==============================\n\n")

  return(report)
}

# ==============================================================================
# COPERNICUS API AUTHENTICATION
# ==============================================================================

#' Read API Key from .cdsapirc File
#'
#' Reads the Copernicus API key from the configuration file.
#' Expected format (new API as of Feb 2025):
#'   url: https://cds.climate.copernicus.eu/api
#'   key: <UUID>
#'
#' @param key_file Path to .cdsapirc file (default: ~/.cdsapirc)
#'
#' @return Character string with API key (UUID format)
#'
#' @examples
#' api_key <- read_api_key()
read_api_key <- function(key_file = NULL) {
  # Search multiple locations if no explicit path given (Windows compatibility)
  if (is.null(key_file)) {
    candidates <- c(
      path.expand("~/.cdsapirc"),
      file.path(Sys.getenv("USERPROFILE"), ".cdsapirc"),
      file.path(Sys.getenv("HOME"), ".cdsapirc"),
      file.path(Sys.getenv("HOMEDRIVE"), Sys.getenv("HOMEPATH"), ".cdsapirc")
    )
    candidates <- unique(candidates[candidates != "/.cdsapirc"])
    key_file <- Find(file.exists, candidates)

    if (is.null(key_file)) {
      stop(
        "API key file .cdsapirc not found. Searched:\n",
        paste("  -", candidates, collapse = "\n"), "\n",
        "See docs/COPERNICUS_SETUP_CHECKLIST.md for setup instructions"
      )
    }
  } else {
    key_file <- path.expand(key_file)
  }

  if (!file.exists(key_file)) {
    stop(
      glue("API key file not found: {key_file}\n"),
      "See docs/COPERNICUS_SETUP_CHECKLIST.md for setup instructions"
    )
  }

  lines <- readLines(key_file, warn = FALSE)
  key_line <- grep("^key:", lines, value = TRUE)

  if (length(key_line) == 0) {
    stop(glue("No 'key:' line found in {key_file}"))
  }

  api_key <- trimws(sub("^key:", "", key_line[1]))

  # Validate UUID format (simple check)
  if (!grepl("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$", api_key)) {
    warning(glue("API key does not appear to be a valid UUID: {api_key}"))
  }

  return(api_key)
}

# ==============================================================================
# COPERNICUS API SETUP
# ==============================================================================

#' Setup Copernicus Data Store Connection
#'
#' Validates that API key file exists and has correct format.
#' New API format (Feb 2025): key is a UUID, not UID:API_KEY
#'
#' @param key_file Path to .cdsapirc file (default: ~/.cdsapirc)
#'
#' @return TRUE if setup is valid, stops with error message otherwise
#'
#' @examples
#' setup_cds_connection()
setup_cds_connection <- function(key_file = NULL) {

  # If no explicit path, find the file automatically (Windows compatibility)
  if (is.null(key_file)) {
    candidates <- c(
      path.expand("~/.cdsapirc"),
      file.path(Sys.getenv("USERPROFILE"), ".cdsapirc"),
      file.path(Sys.getenv("HOME"), ".cdsapirc"),
      file.path(Sys.getenv("HOMEDRIVE"), Sys.getenv("HOMEPATH"), ".cdsapirc")
    )
    candidates <- unique(candidates[candidates != "/.cdsapirc"])
    key_file <- Find(file.exists, candidates)

    if (is.null(key_file)) {
      stop(
        "Copernicus API key file .cdsapirc not found. Searched:\n",
        paste("  -", candidates, collapse = "\n"), "\n\n",
        "Create the file at: ", file.path(Sys.getenv("USERPROFILE"), ".cdsapirc"), "\n",
        "With content:\n",
        "url: https://cds.climate.copernicus.eu/api\n",
        "key: <YOUR_UUID_KEY>\n\n",
        "See: docs/COPERNICUS_SETUP_CHECKLIST.md for detailed instructions"
      )
    }
  } else {
    key_file <- path.expand(key_file)
  }

  # Check API key file exists
  if (!file.exists(key_file)) {
    stop(
      glue("Copernicus API key file not found: {key_file}\n"),
      "Create this file with your CDS credentials:\n",
      "url: https://cds.climate.copernicus.eu/api\n",
      "key: <YOUR_UUID_KEY>\n\n",
      "See: docs/COPERNICUS_SETUP_CHECKLIST.md for detailed instructions"
    )
  }

  # Read and validate key file format
  lines <- readLines(key_file, warn = FALSE)
  has_url <- any(grepl("^url:", lines))
  has_key <- any(grepl("^key:", lines))

  if (!has_url || !has_key) {
    stop(
      glue("Invalid format in {key_file}\n"),
      "Expected format:\n",
      "url: https://cds.climate.copernicus.eu/api\n",
      "key: <YOUR_UUID_KEY>\n\n",
      "Example:\n",
      "key: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    )
  }

  # Try to read the key to validate format
  tryCatch({
    api_key <- read_api_key(key_file)
    log_message(glue("API key validated: {substr(api_key, 1, 8)}..."), NULL)
  }, error = function(e) {
    stop(glue("Error reading API key: {e$message}"))
  })

  log_message("Copernicus API connection validated", NULL)
  return(TRUE)
}

# ==============================================================================
# COPERNICUS HTTP REQUEST HANDLER
# ==============================================================================

#' Submit and Download Data from Copernicus API
#'
#' Generic function to submit a data request to Copernicus CDS or ADS,
#' poll for job completion, and download the result.
#'
#' Uses the new REST API (Feb 2025) with PRIVATE-TOKEN authentication.
#'
#' @param base_url API base URL (CDS_BASE_URL or ADS_BASE_URL)
#' @param dataset Dataset name (e.g., "reanalysis-era5-single-levels")
#' @param request_body List with request parameters
#' @param api_key API key (UUID format)
#' @param output_file Path to save downloaded file
#' @param max_wait_minutes Maximum minutes to wait for job (default 60)
#'
#' @return Path to downloaded file on success, NULL on failure
#'
#' @examples
#' result <- copernicus_request(
#'   base_url = CDS_BASE_URL,
#'   dataset = "reanalysis-era5-single-levels",
#'   request_body = list(
#'     variable = "2m_temperature",
#'     year = "2020",
#'     month = "01",
#'     day = "01",
#'     time = "00:00",
#'     area = c(90, -180, -90, 180),
#'     format = "netcdf"
#'   ),
#'   api_key = read_api_key(),
#'   output_file = "data/era5_temp.nc"
#' )
copernicus_request <- function(base_url, dataset, request_body, api_key,
                                output_file, max_wait_minutes = 60) {

  # Validate inputs
  if (is.null(api_key) || api_key == "") {
    stop("API key is required")
  }

  # Prepare request
  submit_url <- glue("{base_url}/processes/{dataset}/execution")
  headers <- add_headers(`PRIVATE-TOKEN` = api_key)

  # Configure SSL (disable verification if needed)
  ssl_config <- config(ssl_verifypeer = FALSE)

  log_message(glue("Submitting request to: {submit_url}"), NULL)
  log_message(glue("Dataset: {dataset}"), NULL)

  # Submit job
  tryCatch({
    response <- POST(
      submit_url,
      headers,
      ssl_config,
      body = list(inputs = request_body),
      encode = "json"
    )

    if (status_code(response) != 201) {
      error_msg <- content(response, as = "text", encoding = "UTF-8")
      stop(glue("Failed to submit request (status {status_code(response)}): {error_msg}"))
    }

    job_info <- content(response, as = "parsed")
    job_id <- job_info$jobID

    if (is.null(job_id)) {
      stop("No jobID returned from API")
    }

    log_message(glue("Job submitted successfully. Job ID: {job_id}"), NULL)

  }, error = function(e) {
    log_message(glue("ERROR submitting request: {e$message}"), NULL)
    return(NULL)
  })

  # Poll for job completion
  status_url <- glue("{base_url}/jobs/{job_id}")
  poll_interval <- 10  # seconds
  max_polls <- (max_wait_minutes * 60) / poll_interval

  log_message("Polling job status...", NULL)

  for (i in 1:max_polls) {
    Sys.sleep(poll_interval)

    tryCatch({
      status_response <- GET(status_url, headers, ssl_config)

      if (status_code(status_response) != 200) {
        warning(glue("Failed to get job status (attempt {i}/{max_polls})"))
        next
      }

      status_info <- content(status_response, as = "parsed")
      job_status <- status_info$status

      log_message(glue("Job status: {job_status} ({i}/{max_polls})"), NULL)

      if (job_status == "successful") {
        log_message("Job completed successfully!", NULL)
        break
      } else if (job_status == "failed") {
        error_msg <- status_info$message %||% "Unknown error"
        stop(glue("Job failed: {error_msg}"))
      } else if (job_status %in% c("dismissed", "cancelled")) {
        stop(glue("Job was {job_status}"))
      }

    }, error = function(e) {
      log_message(glue("ERROR polling status: {e$message}"), NULL)
      return(NULL)
    })
  }

  # Check if job completed within time limit
  if (i >= max_polls) {
    log_message(glue("Job timed out after {max_wait_minutes} minutes"), NULL)
    return(NULL)
  }

  # Get download URL
  results_url <- glue("{base_url}/jobs/{job_id}/results")

  log_message("Fetching download URL...", NULL)

  tryCatch({
    results_response <- GET(results_url, headers, ssl_config)

    if (status_code(results_response) != 200) {
      error_msg <- content(results_response, as = "text", encoding = "UTF-8")
      stop(glue("Failed to get results (status {status_code(results_response)}): {error_msg}"))
    }

    results <- content(results_response, as = "parsed")
    download_url <- results$asset$value$href

    if (is.null(download_url)) {
      stop("No download URL found in results")
    }

    log_message(glue("Download URL: {download_url}"), NULL)

  }, error = function(e) {
    log_message(glue("ERROR fetching results: {e$message}"), NULL)
    return(NULL)
  })

  # Download file
  log_message(glue("Downloading to: {output_file}"), NULL)

  # Create output directory if needed
  output_dir <- dirname(output_file)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  tryCatch({
    download_response <- GET(download_url, ssl_config, write_disk(output_file, overwrite = TRUE))

    if (status_code(download_response) != 200) {
      error_msg <- content(download_response, as = "text", encoding = "UTF-8")
      stop(glue("Download failed (status {status_code(download_response)}): {error_msg}"))
    }

    file_size <- file.info(output_file)$size
    log_message(glue("Download complete! File size: {round(file_size / 1024 / 1024, 2)} MB"), NULL)

    return(output_file)

  }, error = function(e) {
    log_message(glue("ERROR downloading file: {e$message}"), NULL)
    return(NULL)
  })
}

# ==============================================================================
# LOGGING
# ==============================================================================

#' Log Message with Timestamp
#'
#' Appends timestamped message to log file and prints to console.
#'
#' @param msg Character message to log
#' @param log_file Path to log file (if NULL, only prints to console)
#'
#' @examples
#' log_message("Starting extraction", "logs/extraction.log")
log_message <- function(msg, log_file = NULL) {

  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  log_line <- glue("[{timestamp}] {msg}")

  # Print to console
  cat(log_line, "\n")

  # Append to file if specified
  if (!is.null(log_file)) {
    # Create directory if needed
    log_dir <- dirname(log_file)
    if (!dir.exists(log_dir)) {
      dir.create(log_dir, recursive = TRUE)
    }

    cat(log_line, "\n", file = log_file, append = TRUE)
  }
}

# ==============================================================================
# STARTUP MESSAGE
# ==============================================================================

log_message("Loaded shared utilities for Air Pollution & Pollinator Networks project", NULL)
log_message("Functions available: fix_brazilian_coordinates, load_network_metadata, create_regional_bbox, validate_extracted_values, setup_cds_connection, read_api_key, copernicus_request, log_message", NULL)
