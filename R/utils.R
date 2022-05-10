#' Get health region data
#'
#' @return A data frame containing the information from geo/health_regions.csv
#' @export
get_hr <- function() {
  tryCatch(
    {
      if (!exists("hr", envir = covid_etl_env)) {
        # download and cache hr data
        assign(
          "hr",
          readr::read_csv("https://raw.githubusercontent.com/ccodwg/CovidTimelineCanada/main/geo/health_regions.csv",
                          col_types = "ccccccccc"),
          envir = covid_etl_env
        )
      }
      # return hr data
      return(covid_etl_env$hr)
    },
    error = function(e) {
      print(e)
      cat("Error in get_hr", fill = TRUE)
    }
  )
}

#' Get province/territory data
#'
#' @return A data frame containing the information from geo/pt.csv
#' @export
get_pt <- function() {
  tryCatch(
    {
      if (!exists("pt", envir = covid_etl_env)) {
        # download and cache pt data
        assign(
          "pt",
          readr::read_csv("https://raw.githubusercontent.com/ccodwg/CovidTimelineCanada/main/geo/pt.csv",
                          col_types = c("ccccc")),
          envir = covid_etl_env
        )
      }
      # return prov data
      return(covid_etl_env$pt)
    },
    error = function(e) {
      print(e)
      cat("Error in get_pt", fill = TRUE)
    }
  )
}

#' Read raw dataset using readr::read_csv
#'
#' @param file The CSV file to read.
#'
#' @export
read_d <- function(file) {
  tryCatch(
    {
      # define column types
      cols <- readr::cols(
        name = readr::col_character(),
        region = readr::col_character(),
        sub_region_1 = readr::col_character(),
        sub_region_2 = readr::col_character(),
        date = readr::col_date("%Y-%m-%d"),
        value = readr::col_integer(),
        value_daily = readr::col_integer()
      )
      # read dataset
      readr::read_csv(
        file = file,
        col_types = cols) %>%
        suppressWarnings() # suppress warnings about unused column names
    },
    error = function(e) {
      print(e)
      cat("Error in read_d", fill = TRUE)
    }
  )
}

#' Get PHAC data for a particular value and region
#'
#' @param val The value to read data for.
#' @param region Either "all" or the region to read data for.
#' @param exclude_repatriated Exclude "Repatriated" region when reading data for "all" regions? Default: TRUE.
#'
#' @export
get_phac_d <- function(val, region, exclude_repatriated = TRUE) {
  tryCatch(
    {
      match.arg(val, c("cases", "deaths", "tests_completed"))
      # get relevant value
      d <- switch(
        val,
        "cases" = {read_d("raw_data/active_ts/can/can_cases_pt_ts.csv")},
        "deaths" = {read_d("raw_data/active_ts/can/can_deaths_pt_ts.csv")},
        "tests_completed" = {read_d("raw_data/active_ts/can/can_tests_completed_pt_ts.csv")}
      )
      # exclude repatriated
      if (exclude_repatriated) {
        d <- d[d$region != "RT", ]
      }
      # filter to region
      if (region == "all") {
        d <- d[d$region != "CAN", ]
      } else {
        d <- d[d$region == region, ]
      }
      # return data
      d
    },
    error = function(e) {
      print(e)
      cat("Error in phac_d", fill = TRUE)
    }
  )
}

#' Get CCODWG data for a particular value and region
#'
#' @param val The value to read data for. One of "cases" or "deaths".
#' @param region Either "all" or the region to read data for.
#' @param from Optional. Exclude data prior to this date.
#' @param to Optional. Exclude data after this date.
#' @param drop_not_reported Should "Not Reported" sub-regions be dropped? Default: FALSE.
#'
#' @export
get_ccodwg_d <- function(val, region, from = NULL, to = NULL, drop_not_reported = FALSE) {
  tryCatch(
    {
      match.arg(val, c("cases", "deaths"))
      # get relevant value
      d <- switch(
        val,
        "cases" = {read_d("raw_data/ccodwg/can_cases_hr_ts.csv")},
        "deaths" = {read_d("raw_data/ccodwg/can_deaths_hr_ts.csv")}
      )
      # filter to region
      d <- d[d$region == region, ]
      # date filter
      if (!is.null(from)) {
        d <- d[d$date >= as.Date(from), ]
      }
      if (!is.null(to)) {
        d <- d[d$date <= as.Date(to), ]
      }
      # drop "Not Reported"
      if (drop_not_reported) {
        d <- d[d$sub_region_1 != "Unknown", ] # we converted the name earlier
      }
      # return data
      d
    },
    error = function(e) {
      print(e)
      cat("Error in get_ccodwg_d", fill = TRUE)
    }
  )
}

#' Get covid19tracker.ca data for a particular value and region
#'
#' @param val The value to read data for. One of "hospitalizations" or "icu".
#' @param region Either "all" or the region to read data for.
#' @param from Optional. Exclude data prior to this date.
#' @param to Optional. Exclude data after this date.
#'
#' @export
get_covid19tracker_d <- function(val, region, from = NULL, to = NULL) {
  tryCatch(
    {
      match.arg(val, c("hospitalizations", "icu"))
      # get relevant value
      d <- switch(
        val,
        "hospitalizations" = {read_d("raw_data/covid19tracker/can_hospitalizations_pt_ts.csv")},
        "icu" = {read_d("raw_data/covid19tracker/can_icu_pt_ts.csv")}
      )
      # filter to region
      d <- d[d$region == region, ]
      # date filter
      if (!is.null(from)) {
        d <- d[d$date >= as.Date(from), ]
      }
      if (!is.null(to)) {
        d <- d[d$date <= as.Date(to), ]
      }
      # return data
      d
    },
    error = function(e) {
      print(e)
      cat("Error in get_covid19tracker_d", fill = TRUE)
    }
  )
}

#' Load datasets
#'
#' Bulk load CSV datasets from `CovidTimelineCanada`.
#'
#' @export
load_datasets <- function() {
  files <- list.files("data", pattern = "*.csv", recursive = TRUE, full.names = TRUE)
  list2env(lapply(stats::setNames(files, make.names(sub("*.csv$", "", basename(files)))),
                  FUN = function(x) read_d(x)),
           envir = parent.frame()) %>% invisible()
}

#' Log error
#'
#' @param e Error to write to log.
#' Write errors to a temporary file called COVID19CanadaETL.log.
log_error <- function(e) {
  # check if there are any errors
  if (identical(e, character(0))) {
    return(invisible(NULL))
  }
  log_path <- file.path(tempdir(), "COVID19CanadaETL.log")
  # ensure log file exists, if not, create it
  if (!file.exists(log_path)) {
    file.create(log_path)
  }
  # write error to log file
  write(paste0(e, "\n"), file = log_path, append = TRUE)
}
