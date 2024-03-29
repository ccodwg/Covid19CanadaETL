#' Get health region data
#'
#' @return A data frame containing the information from geo/hr.csv
#' @export
get_hr <- function() {
  tryCatch(
    {
      if (!exists("hr", envir = covid_etl_env)) {
        # download and cache hr data
        assign(
          "hr",
          readr::read_csv("https://raw.githubusercontent.com/ccodwg/CovidTimelineCanada/main/geo/hr.csv",
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
#' @param val_numeric Is the value numeric (versus an integer)? Default: FALSE.
#'
#' @export
read_d <- function(file, val_numeric = FALSE) {
  tryCatch(
    {
      # define column types
      if (val_numeric) {
        cols <- readr::cols(
          name = readr::col_character(),
          region = readr::col_character(),
          sub_region_1 = readr::col_character(),
          sub_region_2 = readr::col_character(),
          date = readr::col_date("%Y-%m-%d"),
          value = readr::col_double(),
          value_daily = readr::col_double()
        )
      } else {
        cols <- readr::cols(
          name = readr::col_character(),
          region = readr::col_character(),
          sub_region_1 = readr::col_character(),
          sub_region_2 = readr::col_character(),
          date = readr::col_date("%Y-%m-%d"),
          value = readr::col_integer(),
          value_daily = readr::col_integer()
        )
      }
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
#' @param keep_up_to_date Keep data for each province/territory only up to the date it was most recently updated?
#' Method of filtering differs by value. Default: FALSE (i.e., keep all data).
#' Ignored if there is no `update` column.
#'
#' @export
get_phac_d <- function(val, region, exclude_repatriated = TRUE, keep_up_to_date = FALSE) {
  tryCatch(
    {
      match.arg(val, c(
        "cases", "cases_daily",
        "deaths", "deaths_daily",
        "hospitalizations",
        "icu",
        "tests_completed", "tests_completed_rvdss",
        "vaccine_coverage_dose_1", "vaccine_coverage_dose_2",
        "vaccine_coverage_dose_3", "vaccine_coverage_dose_4",
        "vaccine_administration_dose_1", "vaccine_administration_dose_2",
        "vaccine_administration_dose_3", "vaccine_administration_dose_4",
        "vaccine_administration_dose_5plus", "vaccine_administration_total_doses",
        "vaccine_distribution_total_doses"))
      # get relevant value
      d <- switch(
        val,
        "cases" = {read_d("raw_data/active_ts/can/can_cases_pt_ts.csv")}, # cases from (current) weekly epidemiology update
        "cases_daily" = {read_d("raw_data/static/can/can_cases_pt_ts.csv")}, # cases from (retired) daily epidemiology update
        "deaths" = {read_d("raw_data/active_ts/can/can_deaths_pt_ts.csv")}, # deaths from (current) weekly epidemiology update
        "deaths_daily" = {read_d("raw_data/static/can/can_deaths_pt_ts.csv")}, # deaths from (retired) daily epidemiology update
        "hospitalizations" = {read_d("raw_data/active_ts/can/can_hospitalizations_pt_ts.csv")},
        "icu" = {read_d("raw_data/active_ts/can/can_icu_pt_ts.csv")},
        "tests_completed" = {read_d("raw_data/static/can/can_tests_completed_pt_ts.csv")},
        "tests_completed_rvdss" = {read_d("raw_data/active_ts/can/can_tests_completed_pt_ts.csv")},
        "vaccine_coverage_dose_1" = {read_d(
          "raw_data/active_ts/can/can_vaccine_coverage_dose_1_pt_ts.csv", val_numeric = TRUE)},
        "vaccine_coverage_dose_2" = {read_d(
          "raw_data/active_ts/can/can_vaccine_coverage_dose_2_pt_ts.csv", val_numeric = TRUE)},
        "vaccine_coverage_dose_3" = {read_d(
          "raw_data/active_ts/can/can_vaccine_coverage_dose_3_pt_ts.csv", val_numeric = TRUE)},
        "vaccine_coverage_dose_4" = {read_d(
          "raw_data/active_ts/can/can_vaccine_coverage_dose_4_pt_ts.csv", val_numeric = TRUE)},
        "vaccine_administration_dose_1" = {read_d(
          "raw_data/active_ts/can/can_vaccine_administration_dose_1_pt_ts.csv")},
        "vaccine_administration_dose_2" = {read_d(
          "raw_data/active_ts/can/can_vaccine_administration_dose_2_pt_ts.csv")},
        "vaccine_administration_dose_3" = {read_d(
          "raw_data/active_ts/can/can_vaccine_administration_dose_3_pt_ts.csv")},
        "vaccine_administration_dose_4" = {read_d(
          "raw_data/active_ts/can/can_vaccine_administration_dose_4_pt_ts.csv")},
        "vaccine_administration_dose_5plus" = {read_d(
          "raw_data/active_ts/can/can_vaccine_administration_dose_5plus_pt_ts.csv")},
        "vaccine_administration_total_doses" = {read_d(
          "raw_data/active_ts/can/can_vaccine_administration_total_doses_pt_ts.csv")},
        "vaccine_distribution_total_doses" = {read_d(
          "raw_data/static/can/can_vaccine_distribution_total_doses_pt_ts.csv")}
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
      # filter up to most recent date updated for each P/T
      if (keep_up_to_date) {
        if (val %in% c("cases", "cases_daily", "deaths", "deaths_daily", "tests_completed", "tests_completed_rvdss")) {
          # get unique PTs
          pts <- unique(d$region)
          pts <- pts[!pts %in% c("CAN", "RT")]
          if (length(pts) == 0) {
            warning("Ignoring 'keep_up_to_date = TRUE' due to no valid PTs...")
          } else {
            # calculate cumsum of update
            # first instance of maximum value is most recent update date
            d <- d %>%
              dplyr::mutate(update = ifelse(is.na(.data$update), 0, .data$update)) %>%
              dplyr::group_by(.data$region) %>%
              dplyr::mutate(update = cumsum(.data$update)) %>%
              dplyr::ungroup()
            # filter is ignored for Canada and Repatriated groups
            for (pt in pts) {
              update_max <- max(d[d$region == pt, "update"])
              update_max_date <- d[d$region == pt & d$update == update_max, "date", drop = TRUE][1]
              d <- d %>%
                dplyr::filter(.data$region != pt | .data$date <= update_max_date)
            }
            # drop 'update' column
            d <- d[, names(d) != "update"]
          }
        } else {
          warning("keep_up_to_date = TRUE is not supported with this value, ignoring...")
        }
      }
      # filter weekly data to max date (2023-12-30)
      if (val %in% c("cases", "deaths", "tests_completed", "tests_completed_rvdss")) {
        d <- d[d$date <= as.Date("2023-12-30"), ]
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

#' Replace part of a health region-level time series
#'
#' @param d1 The health region-level time series to replace data in.
#' @param d2 The replacement data for a specific region.
#' @param val The value to replace. One of "cases" or "deaths".
#' @param region The region to replace data for.
#' @param date The date to start replacing data from.
#'
#' @export
replace_hr <- function(d1, d2, val, region, date) {
  tryCatch(
    {
      match.arg(val, c("cases", "deaths"))
      # filter to date
      d2 <- d2[d2$date >= as.Date({{date}}), ]
      # replace data
      d1 <- d1 |>
        dplyr::filter(!(.data$region == .env$region & .data$date >= as.Date(.env$date))) |>
        dplyr::bind_rows(d2)
      # return data
      dataset_format(d1, "pt")
    },
    error = function(e) {
      print(e)
      cat("Error in replace_hr", fill = TRUE)
    }
  )
}

#' Replace part of health region-level time series with PHAC data
#'
#' @param d The health region-level time series to replace data in.
#' @param val The value to replace. One of "cases" or "deaths".
#' @param region The region to replace data for.
#' @param date The date to start replacing data from.
#'
#' @export
replace_hr_phac <- function(d, val, region, date) {
  tryCatch(
    {
      match.arg(val, c("cases", "deaths"))
      # get PHAC data
      if (date > as.Date("2022-06-08")) {
        # get only weekly data
        phac_d <- get_phac_d(val, region, keep_up_to_date = TRUE) |>
          dplyr::filter(.data$date >= as.Date(.env$date))
      } else {
        # merge weekly and daily data
        phac_d <- dplyr::bind_rows(
          get_phac_d(paste0(val, "_daily"), region, keep_up_to_date = FALSE) |> # update column is sometimes wrong
            dplyr::filter(.data$date >= as.Date(.env$date)), # up to 2022-06-08
          get_phac_d(val, region, keep_up_to_date = TRUE) |>
            dplyr::filter(.data$date >= as.Date("2022-06-11"))) |> # 2022-06-11 and later
          dplyr::select(-.data$update)
      }
      # replace data
      replace_hr(d, phac_d, val, region, date)
    },
    error = function(e) {
      print(e)
      cat("Error in replace_hr_phac", fill = TRUE)
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
#' Bulk load CSV datasets from `CovidTimelineCanada`. If `path` is not provided,
#' the function will first check if the working directory is called
#' `CovidTimelineCanada` and if so, load files from the `data` directory. If
#' not, the function will attempt to download the data from GitHub into a
#' temporary directory. If `path` is provided, the function will attempt to load
#' files from the `data` directory at that path.
#'
#' @param path Path to `CovidTimelineCanada` directory. Optional, see
#' Description for more details.
#'
#' @export
load_datasets <- function(path = NULL) {
  tryCatch(
    {
      if (is.null(path)) {
        if (basename(getwd()) == "CovidTimelineCanada") {
          # use working directory if it is called "CovidTimelineCanada"
          cat("Loading CovidTimelineCanada datasets from working directory...", fill = TRUE)
          path <- "."
        } else {
          # attempt to clone from GitHub into temporary directory
          cat("Downloading CovidTimelineCanada datasets from GitHub...", fill = TRUE)
          temp_dir <- tempdir()
          temp_file <- tempfile(tmpdir = temp_dir)
          utils::download.file("https://github.com/ccodwg/CovidTimelineCanada/archive/refs/heads/main.zip",
                               destfile = temp_file)
          utils::unzip(temp_file, exdir = temp_dir)
          path <- file.path(temp_dir, "CovidTimelineCanada-main")
        }
      }
      # read data files
      files <- list.files(file.path(path, "data"), pattern = "*.csv", recursive = TRUE, full.names = TRUE)
      list2env(lapply(stats::setNames(files, make.names(sub("*.csv$", "", basename(files)))),
                      FUN = function(x) {
                        if (grepl("^vaccine_coverage_dose_", basename(x))) {
                          read_d(x, val_numeric = TRUE)
                        } else {
                          read_d(x, val_numeric = FALSE)
                        }}),
               envir = parent.frame())
      # read update time
      assign("update_time", readLines(file.path(path, "update_time.txt")), envir = parent.frame())
    },
    error = function(e) {
      print(e)
      cat("Error loading datasets. Is the path specified correctly?", fill = TRUE)
    }
  )
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

#' Get current date
#'
#' Get current date in the time zone of the dataset (America/Toronto).
get_current_date <- function() {
  lubridate::date(lubridate::with_tz(Sys.time(), "America/Toronto"))
}
