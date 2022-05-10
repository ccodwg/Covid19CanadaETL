#' Functions to download/upload data from Google Sheets
#'
#' @importFrom rlang .data :=
#' @param email The email to authenticate with Google Sheets/Drive. If not
#' provided, manual authentication will be requested (unless `path` is provided).
#' @param path Alternative to email authentication. JSON identifying service
#' account (e.g., file path to JSON). See \code{\link[googledrive]{drive_auth}}
#' and \code{\link[googlesheets4]{gs4_auth}}.
#' @param d Dataset to upload.
#' @param files List of Google Drive files from \code{\link[googledrive]{drive_ls}}.
#' @param file Name of specific Google Sheets file.
#' @param sheet Number/name of specific sheet in the Google Sheets file.
#'
#' @name sheets_funs
NULL

#' Authenticate with Google Drive
#'
#' @rdname sheets_funs
#'
#' @export
auth_gd <- function(email = NULL, path = NULL) {
  if (!googledrive::drive_has_token()) {
    # only run if not already authenticated
    if (!is.null(path)) {
      googledrive::drive_auth(path = path)
    } else if (!is.null(email)) {
      googledrive::drive_auth(email = email)
    } else {
      googledrive::drive_auth()
    }
  }
}

#' Authenticate with Google Sheets
#'
#' @rdname sheets_funs
#'
#' @export
auth_gs <- function(email = NULL, path = NULL) {
  if (!googlesheets4::gs4_has_token()) {
    # only run if not already authenticated
    if (!is.null(path)) {
      googlesheets4::gs4_auth(path = path)
    } else if (!is.null(email)) {
      googlesheets4::gs4_auth(email = email)
    } else {
      googlesheets4::gs4_auth()
    }
  }
}

#' Quickly load data from Google Sheets
#'
#' @rdname sheets_funs
#'
#' @export
sheets_load <- function(files, file, sheet = NULL) {
  tryCatch(
    {
      id <- files %>%
        dplyr::filter(.data$name == file) %>%
        dplyr::pull(.data$id)
      if (!is.null(sheet)) {
        googlesheets4::read_sheet(
          ss = id,
          sheet = sheet,
          col_types = "c" # don't mangle dates
        )
      } else {
        googlesheets4::read_sheet(
          ss = id,
          col_types = "c" # don't mangle dates
        )
      }
    },
    error = function(e) {
      print(e)
      if (!is.null(sheet)) {
        cat("Error in sheets_load:", file, fill = TRUE)
      } else {
        cat("Error in sheets_load:", paste(file, sheet, sep = " / "), fill = TRUE)
      }
    }
  )
}

#' Merge data with existing data in Google Sheets
#'
#' @rdname sheets_funs
#'
#' @export
sheets_merge <- function(d, files, file, sheet = NULL) {
  tryCatch(
    {
      # bind new data together
      d <- dplyr::bind_rows(d)
      # extract the current date
      if (length(unique(d$date)) == 1) {
        date_today <- as.character(d$date[1])
      } else {
        stop("Dates do not all match")
      }
      # drop unneeded columns
      d <- dplyr::select(d, -.data$name, -.data$date)
      # determine geo columns
      if ("sub_region_2" %in% names(d)) {
        cols <- c("region", "sub_region_1", "sub_region_2")
      } else if ("sub_region_1" %in% names(d)) {
        cols <- c("region", "sub_region_1")
      } else {
        cols <- "region"
      }
      # load existing data
      d_merge <- sheets_load(files, file, sheet)
      # check if there is already data for today's date
      if (date_today %in% names(d_merge)) {
        # extract existing data
        d_old <- d_merge %>%
          dplyr::transmute(
            !!!rlang::syms(cols),
            value_old = !!rlang::sym(date_today)
          )
        # drop column
        d_merge <- d_merge %>%
          dplyr::select(-!!date_today)
        # combine new data with existing data
        d <- dplyr::left_join(
          d,
          d_old,
          by = cols
        ) %>%
          dplyr::transmute(
            !!!rlang::syms(cols),
            value = ifelse(is.na(.data$value), .data$value_old, .data$value)
          )
      }
      # rename value column with current date
      d <- dplyr::rename(d, !!date_today := .data$value)
      # merge data
      d <- dplyr::right_join(
        d,
        d_merge,
        by = cols
      )
      # return data
      d
    },
    error = function(e) {
      print(e)
      if (!is.null(sheet)) {
        cat("Error in sheets_merge:", file, fill = TRUE)
      } else {
        cat("Error in sheets_merge:", paste(file, sheet, sep = " / "), fill = TRUE)
      }
    }
  )
}

#' Upload data to Google Sheets
#'
#' @rdname sheets_funs
#'
#' @export
sheets_upload <- function(d, files, file, sheet = NULL) {
  tryCatch(
    {
      id <- files %>%
        dplyr::filter(.data$name == file) %>%
        dplyr::pull(.data$id)
      if (!is.null(sheet)) {
        googlesheets4::write_sheet(
          data = d,
          ss = id,
          sheet = sheet
        )
      } else {
        googlesheets4::write_sheet(
          data = d,
          ss = id,
          sheet = sheet
        )
      }
    },
    error = function(e) {
      print(e)
      if (!is.null(sheet)) {
        cat("Error in sheets_load:", file, fill = TRUE)
      } else {
        cat("Error in sheets_load:", paste(file, sheet, sep = " / "), fill = TRUE)
      }
    }
  )
}

#' Upload active_cumul data to Google Sheets
#'
#' @rdname sheets_funs
#'
#' @export
upload_active_cumul <- function(d, files, file, sheet = NULL) {
  tryCatch(
    {
      # merge new data
      d <- sheets_merge(d, files, file, sheet)
      # upload data
      sheets_upload(d, files, file, sheet)
    },
    error = function(e) {
      print(e)
      if (!is.null(sheet)) {
        cat("Error in upload_active_cumul:", file, fill = TRUE)
      } else {
        cat("Error in upload_active_cumul:", paste(file, sheet, sep = " / "), fill = TRUE)
      }
    }
  )
}
