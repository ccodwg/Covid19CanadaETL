#' Get health region data
#'
#' @param sknew Use the new SK health regions? Default: True.
#' @return A data frame containing the information from hr_map.csv.
#'
#' @export
get_hr_data <- function(sknew = TRUE) {
  if (sknew) {
    if (!exists("hr_map_sk_new", envir = covid_etl_env)) {
      # download and cache hr data
      assign(
        "hr_map_sk_new",
        utils::read.csv("https://raw.githubusercontent.com/ccodwg/Covid19Canada/master/other/hr_map_sk_new.csv",
                 stringsAsFactors = FALSE),
        envir = covid_etl_env
      )
    }
    # return hr data
    return(covid_etl_env$hr_map_sk_new)
  } else {
    if (!exists("hr_map", envir = covid_etl_env)) {
      # download and cache hr data
      assign(
        "hr_map",
        utils::read.csv("https://raw.githubusercontent.com/ccodwg/Covid19Canada/master/other/hr_map.csv",
                 stringsAsFactors = FALSE),
        envir = covid_etl_env
      )
    }
    # return hr data
    return(covid_etl_env$hr_map)
  }
}

#' Get province data
#'
#' @return A data frame containing the information from prov_map.csv.
#' @export
get_prov_data <- function() {
  if (!exists("prov_map", envir = covid_etl_env)) {
    # download and cache prov data
    assign(
      "prov_map",
      utils::read.csv("https://raw.githubusercontent.com/ccodwg/Covid19Canada/master/other/prov_map.csv",
               stringsAsFactors = FALSE),
      envir = covid_etl_env
    )
  }
  # return prov data
  return(covid_etl_env$prov_map)
}

#' Authenticate with Google Sheets
#'
#' Authentication is required to access Google Sheets via the 'googlesheets4' package.
#'
#' @param email The email to authenticate with Google Sheets. If not provided,
#' manual authentication will be requested (unless `path` is provided).
#' @param path Alternative to email authentication. JSON identifying service
#' account (e.g., file path to JSON). See \code{\link[googlesheets4]{gs4_auth}}.
#' @export
auth_gs <- function(email = NULL, path = NULL) {
  if (!is.null(path)) {
    googlesheets4::gs4_auth(path = path)
  } else if (!is.null(email)) {
    googlesheets4::gs4_auth(email = email)
  } else {
    googlesheets4::gs4_auth()
  }
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
