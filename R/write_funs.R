#' Functions to write data
#'
#' @param d The dataset to write.
#' @param dir The directory to write to. One of "active_ts", "active_cumul".
#' @param region The region to write data for.
#' @param val The name identifying the value being written.
#' @param geo The geographic level of data being written. One of "can", "pt", "hr", "sub-hr".
#' @param name The name to give to the output file.
#' @param sheet Name of sheet to read from the reports or active_cumul Google Sheet.
#' @param regions Regions to sync from active_cumul Google Sheet.
#'
#' @name write_funs
NULL

#' Write processed data to a specific directory in "raw_data"
#'
#' @rdname write_funs
#'
#' @export
write_ts <- function(d,
                     dir = c("active_ts", "active_cumul"),
                     region,
                     val) {
  tryCatch(
    {
      match.arg(dir, choices = c("active_ts", "active_cumul"), several.ok = FALSE)
      # lowercase region
      region <- tolower(region)
      # determine geo
      if ("sub_region_2" %in% names(d)) {
        geo <- "sub-hr"
        quote_range <- 5 # sub-hr
      } else if ("sub_region_1" %in% names(d)) {
        geo <- "hr"
        quote_range <- 4 # hr
      } else {
        geo <- "pt"
        quote_range <- 3 # pt
      }
      # define file name and path
      dir_path <- file.path("raw_data", dir, region)
      dir.create(dir_path, showWarnings = FALSE)
      f_name <- paste0(paste(region, val, geo, "ts", sep = "_"), ".csv")
      utils::write.csv(
        d,
        file.path(dir_path, f_name),
        row.names = FALSE,
        na = "",
        quote = 1:quote_range
      )
    },
    error = function(e) {
      print(e)
      cat("Error in write_ts:", paste(dir, region, val, sep = " / "), fill = TRUE)
    }
  )
}

#' Download and write covid19tracker.ca hospitalization or ICU data to "raw_data/covid19tracker" directory
#'
#' @rdname write_funs
#'
#' @export
update_covid19tracker <- function(val) {
  tryCatch(
    {
      match.arg(val, c("hospitalizations", "icu"))
      pt <- get_pt()[["region"]]
      if (val == "hospitalizations") {
        lapply(pt, function(x) {
          url <- paste0("https://api.covid19tracker.ca/reports/province/", x, "?stat=hospitalizations&fill_dates=false")
          jsonlite::fromJSON(url)$data %>%
            dplyr::transmute(
              name = "hospitalizations",
              region = x,
              .data$date,
              value = .data$total_hospitalizations
            )
        }) %>%
          dplyr::bind_rows() %>%
          utils::write.csv("raw_data/covid19tracker/can_hospitalizations_pt_ts.csv", row.names = FALSE)
      } else if (val == "icu") {
        lapply(pt, function(x) {
          url <- paste0("https://api.covid19tracker.ca/reports/province/", x, "?stat=criticals&fill_dates=false")
          jsonlite::fromJSON(url)$data %>%
            dplyr::transmute(
              name = "icu",
              region = x,
              .data$date,
              value = .data$total_criticals
            )
        }) %>%
          dplyr::bind_rows() %>%
          utils::write.csv("raw_data/covid19tracker/can_icu_pt_ts.csv", row.names = FALSE)
      } else {
        stop("Invalid val.")
      }
    },
    error = function(e) {
      print(e)
      cat("Error in update_covid19tracker:", val, fill = TRUE)
    }
  )
}

#' Sync and write report data from Google Sheets
#'
#' @rdname write_funs
#'
#' @export
sync_report <- function(sheet, region, geo) {
  tryCatch(
    {
      match.arg(geo, c("hr"), several.ok = FALSE)
      if (geo == "hr") {
        quote_cols <- 6
      }
      dir_path <- file.path("raw_data", "reports", region)
      dir.create(dir_path, showWarnings = FALSE)
      f_name <- paste0(sheet, ".csv")
      d <- googlesheets4::read_sheet(
        ss = "1ZTUb3fVzi6CLZAbU3lj6T6FTzl5Aq-arBNL49ru3VLo",
        sheet = sheet
      )
      utils::write.csv(
        d,
        file.path(dir_path, f_name),
        row.names = FALSE,
        na = "",
        quote = 1:quote_cols)
    },
    error = function(e) {
      print(e)
      cat("Error in sync_reports:", paste(sheet, region, geo, sep = " / "), fill = TRUE)
    }
  )
}

#' Sync and write active_cumul data from Google Sheets
#'
#' @rdname write_funs
#'
#' @export
sync_active_cumul <- function(sheet, val, regions) {
  tryCatch(
    {
      # load sheet
      d <- googlesheets4::read_sheet(
        ss = "14b_aksEvD3s_yonmG43rChCuL4u45qVsS2GmhUdiRd0",
        sheet = sheet)
      # format data frame
      d <- d %>%
        # ensure all relevant columns are character for writing
        dplyr::mutate(dplyr::across(!dplyr::matches("date"), as.character)) %>%
        # wide to long format
        tidyr::pivot_longer(
          cols = !dplyr::matches("region|sub_region_1|sub_region_2"),
          names_to = "date",
          values_to = "value") %>%
        # drop empty values
        dplyr::filter(!is.na(.data$value)) %>%
        # arrange data
        dplyr::arrange(dplyr::across(dplyr::matches("region|sub_region_1|sub_region_2|date"))) %>%
        # add column with value name
        dplyr::mutate(name = val, .before = 1)
      # subset and write file for each region
      for (r in regions) {
        dir_path <- file.path("raw_data", "active_cumul", tolower(r))
        dir.create(dir_path, showWarnings = FALSE)
        f_name <- paste0(tolower(r), "_", sheet, "_ts.csv")
        d %>%
          dplyr::filter(.data$region == r) %>%
          utils::write.csv(
            file.path(dir_path, f_name),
            row.names = FALSE,
            na = "",
            quote = 1:(ncol(d) - 1))
      }
    },
    error = function(e) {
      print(e)
      cat("Error in sync_active_cumul:", paste(sheet, val, regions, sep = " / "), fill = TRUE)
    }
  )
}

#' Write final CSV dataset
#'
#' @rdname write_funs
#'
#' @export
write_dataset <- function(d, geo, name) {
  tryCatch(
    {
      utils::write.csv(
        d,
        file.path("data", geo, paste0(name, ".csv")),
        row.names = FALSE,
        quote = 1:(ncol(d) - 2))
    },
    error = function(e) {
      print(e)
      cat("Error in write_dataset:", paste(geo, name, sep = " / "), fill = TRUE)
    }
  )
}
