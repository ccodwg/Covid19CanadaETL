#' Functions to further process COVID-19 data
#'
#' Functions to further process data downloaded via the `process_dataset`
#' function from `Covid19CanadaDataProcess`. Examples include normalizing
#' province and health region names and aggregating health region-level data up
#' to the province level.
#'
#' @param d The dataset to process.
#' @param n_days The number of days to shift dates forward by.
#' @param digits The number of digits to round to for numeric values. If not
#' provided, no rounding will be done for numeric values.
#' @param name The value name to add.
#' @param val The value to select.
#' @param out_col The name of the value column in the output dataset.
#' @param sr A vector of sub-regions to drop.
#' @param raw Is this function operating on raw data with only one of "value" or
#' "value_daily" columns?
#' @param as_of_date The date to add as the "as-of date".
#' @param geo The geographic level of the data. One of "pt", "hr", "sub-hr".
#' @param d1 Dataset to append to. A cumulative value dataset.
#' @param d2 Dataset being appended. A daily value dataset.
#' @param max_date The maximum date to trim the output dataset to.
#'
#' @name process_funs
NULL

#' Convert health region names to HRUIDs
#'
#' @rdname process_funs
#'
#' @export
convert_hr_names <- function(d) {
  # read and process health region file
  tryCatch(
    {
      hr <- get_hr() %>%
        dplyr::mutate(name_hruid = .data$hruid) %>%
        dplyr::select(!dplyr::starts_with("pop")) %>% # drop pop columns
        tidyr::pivot_longer(
          dplyr::starts_with("name_"),
          names_to = "hr_name"
        ) %>%
        dplyr::transmute(
          .data$region,
          sub_region_1 = .data$value,
          .data$hruid) %>%
        dplyr::filter(!is.na(.data$sub_region_1)) %>%
        dplyr::distinct()
      # convert health region names
      d <- d %>%
        dplyr::left_join(hr, by = c("region", "sub_region_1")) %>%
        dplyr::mutate(
          sub_region_1_original = .data$sub_region_1,
          sub_region_1 = ifelse(
            .data$sub_region_1 %in% c("Unknown", "9999"), "9999", .data$hruid)) %>%
        dplyr::arrange(.data$region, .data$sub_region_1)
      # check for conversion failures
      failed <- d[is.na(d$sub_region_1), "sub_region_1_original", drop = TRUE] %>%
        unique()
      if (length(failed) > 0) {
        stop(paste0("Some health region names failed to convert: ", paste(failed, collapse = ", ")))
      }
      # return converted data frame
      dplyr::select(d, -.data$sub_region_1_original, -.data$hruid)
    },
    error = function(e) {
      print(e)
      cat("Error in convert_hr_names", fill = TRUE)
    }
  )
}

#' Shift dates forward by x days
#'
#' @rdname process_funs
#'
#' @export
date_shift <- function(d, n_days) {
  tryCatch(
    {
      d %>%
        dplyr::mutate(date = date + n_days)
    },
    error = function(e) {
      print(e)
      cat("Error in date_shift", fill = TRUE)
    }
  )
}

#' Add name column in the first position
#'
#' @rdname process_funs
#'
#' @export
add_name_col <- function(d, name) {
  tryCatch(
    {
      # operation fails when .before is specified but the column already exists
      # so drop the column if it already exists
      d <- d[names(d) != "name"]
      d %>%
        dplyr::mutate(name = dplyr::all_of(name), .before = 1)
    },
    error = function(e) {
      print(e)
      cat("Error in add_name_col:", name, fill = TRUE)
    }
  )
}

#' Drop sub-regions
#'
#' @rdname process_funs
#'
#' @export
drop_sub_regions <- function(d, sr) {
  tryCatch(
    {
      d %>%
        dplyr::filter(!.data$sub_region_1 %in% dplyr::all_of(sr))
    },
    error = function(e) {
      print(e)
      cat("Error in drop_regions:", paste(sr, collapse = ", "), fill = TRUE)
    }
  )
}

#' Add row for as of date
#'
#' @rdname process_funs
#'
#' @export
add_as_of_date <- function(d, as_of_date) {
  tryCatch(
    {
      d <- d %>%
        dplyr::mutate(value = as.character(.data$value), date = as.character(.data$date)) %>%
        {dplyr::add_row(.,
                        dplyr::tibble(
                          name = .[1, "name", drop = TRUE],
                          region = "as_of_date",
                          date = .[1, "date", drop = TRUE],
                          value = as.character(as_of_date)))}
      d[is.na(d)] <- ""
      d
    },
    error = function(e) {
      print(e)
      cat("Error in add_as_of_date", fill = TRUE)
    }
  )
}

#' Pluck relevant data from weekly reports
#'
#' @rdname process_funs
#'
#' @export
report_pluck <- function(d, name, val, out_col, geo) {
  tryCatch(
    {
      match.arg(geo, c("pt", "hr"), several.ok = FALSE)
      # filter data
      if (geo == "pt") {
        d <- d %>%
          dplyr::filter(is.na(.data$sub_region_1)) %>%
          dplyr::transmute(
            name = name,
            .data$region,
            date = .data$date_end,
            "{out_col}" := !!rlang::sym(val)
          )
      } else {
        d <- d %>%
          dplyr::filter(!is.na(.data$sub_region_1)) %>%
          dplyr::transmute(
            name = name,
            .data$region,
            .data$sub_region_1,
            date = .data$date_end,
            "{out_col}" := !!rlang::sym(val)
          )
      }
      # return data
      d %>% dplyr::filter(!is.na(!!rlang::sym(out_col)))
    },
    error = function(e) {
      print(e)
      cat("Error in report_pluck:", paste(name, val, out_col, geo, sep = " / "), fill = TRUE)
    }
  )
}

#' Filter to most recent data in report with multiple entries for the same date
#'
#' @rdname process_funs
#'
#' @export
report_recent <- function(d) {
  tryCatch(
    {
      col_vars <- c("name", "region", "sub_region_1", "sub_region_2", "date")
      col_vars <- col_vars[col_vars %in% names(d)]
      d %>%
        dplyr::group_by(!!!rlang::syms(col_vars)) %>%
        dplyr::slice_tail(n = 1) %>%
        dplyr::ungroup()
    },
    error = function(e) {
      print(e)
      cat("Error in report_recent")
    }
  )
}

#' Filter a dataset to a maximum date
#'
#' @rdname process_funs
#'
#' @export
max_date <- function(d, max_date) {
  tryCatch(
    {
      d %>%
        dplyr::filter(.data$date <= as.Date(max_date))
    },
    error = function(e) {
      print(e)
      cat("Error in max_date", fill = TRUE)
    }
  )
}

#' Append a daily value dataset to a cumulative value dataset
#'
#' @rdname process_funs
#'
#' @export
append_daily_d <- function(d1, d2) {
  tryCatch(
    {
      if ("sub_region_1" %in% names(d1)) {
        d2 <- d2 %>%
          dplyr::group_by(.data$name, .data$region, .data$sub_region_1) %>%
          dplyr::transmute(
            .data$name,
            .data$region,
            .data$sub_region_1,
            .data$date,
            value = cumsum(.data$value_daily)) %>%
          dplyr::ungroup()
        d1_h <- unique(d1$sub_region_1)
        for (h in unique(d2$sub_region_1)) {
          # check for incompatible sub_region_1 names
          if (!h %in% d1_h) {
            # special case for "Unknown" sub_region_1 in d2, which may be missing from d1
            if (h == "Unknown") {
              break
            } else {
              stop("Sub-region names are incompatible")
            }
          } else {
            # convert daily values in d2 to cumulative values based on final cumulative value in d1
            d2[d2$sub_region_1 == h, "value"] <- d2[d2$sub_region_1 == h, "value"] +
              d1[d1$sub_region_1 == h & d1$date == max(d1$date), "value", drop = TRUE]
          }
        }
      } else {
        d2 <- d2 %>%
          dplyr::group_by(.data$name, .data$region) %>%
          dplyr::transmute(
            .data$name,
            .data$region,
            .data$date,
            value = cumsum(.data$value_daily)) %>%
          dplyr::ungroup()
        d1_h <- unique(d1$region)
        for (h in unique(d2$region)) {
          # check for incompatible region names
          if (!h %in% d1_h) {stop("Sub-region names are incompatible")
          } else {
            # convert daily values in d2 to cumulative values based on final cumulative value in d1
            d2[d2$region == h, "value"] <- d2[d2$region == h, "value"] +
              d1[d1$region == h & d1$date == max(d1$date), "value", drop = TRUE]
          }
        }
      }
      # returned combined dataset
      dplyr::bind_rows(d1, d2)
    },
    error = function(e) {
      print(e)
      cat("Error in append_daily_d", fill = TRUE)
    }
  )
}

#' Collate datasets together
#'
#' @rdname process_funs
#'
#' @export
collate_datasets <- function(val) {
  tryCatch(
    {
      dplyr::bind_rows(
        mget(ls(parent.frame(),
                pattern = paste0(val, "_(", paste(tolower(get_pt()[["region"]]), collapse = "|"), ")")), envir = parent.frame())
      )
    },
    error = function(e) {
      print(e)
      cat("Error in collate datasets:", val, fill = TRUE)
    }
  )
}

#' Fortify a PT-level dataset with a health region column
#'
#' @rdname process_funs
#'
#' @export
add_hr_col <- function(d, name) {
  tryCatch(
    {
      d %>% tibble::add_column(data.frame(sub_region_1 = name), .after = "region")
    },
    error = function(e) {
      print(e)
      cat("add_hr_col", fill = TRUE)
    }
  )
}

#' Aggregate HR-level data up to PT-level
#'
#' @rdname process_funs
#'
#' @export
agg2pt <- function(d, raw = FALSE) {
  tryCatch(
    {
      if (!raw) {
        d %>%
          dplyr::select(-.data$sub_region_1) %>%
          dplyr::group_by(.data$name, .data$region, .data$date) %>%
          dplyr::summarise(value = sum(.data$value), value_daily = sum(.data$value_daily), .groups = "drop")
      } else {
        if ("value" %in% names(d) & "value_daily" %in% names(d)) {
          stop("Argument 'raw' can only be used on raw data with one of 'value' or 'value_daily'.")
        } else {
          if ("value" %in% names(d)) {
            d %>%
              dplyr::select(-.data$sub_region_1) %>%
              dplyr::group_by(.data$name, .data$region, .data$date) %>%
              dplyr::summarise(value = sum(.data$value), .groups = "drop")
          } else {
            d %>%
              dplyr::select(-.data$sub_region_1) %>%
              dplyr::group_by(.data$name, .data$region, .data$date) %>%
              dplyr::summarise(value_daily = sum(.data$value_daily), .groups = "drop")
          }
        }
      }
    },
    error = function(e) {
      print(e)
      cat("Error in agg2pt", fill = TRUE)
    }
  )
}

#' Aggregate PT-level data up to Canada-level
#'
#' @rdname process_funs
#'
#' @export
agg2can <- function(d) {
  tryCatch(
    {
      # fill missing dates for each pt
      out <- tidyr::expand(
        d,
        tidyr::nesting(d$name, d$region),
        date = seq.Date(min(d$date), max(d$date), by = "day"),
        .name_repair = function(y) sub("^d\\$", "", y))
      out <- dplyr::right_join(d, out, by = c("name", "region", "date")) %>%
        dplyr::select(-.data$value_daily) %>%
        # fill missing values
        dplyr::arrange(.data$name, .data$region, .data$date) %>%
        dplyr::group_by(dplyr::across(c(-.data$date, -.data$value))) %>%
        tidyr::fill(.data$value, .direction = "down") %>%
        tidyr::replace_na(list(value = 0)) %>%
        # calculate daily differences
        dplyr::mutate(value_daily = c(dplyr::first(.data$value), diff(.data$value))) %>%
        dplyr::ungroup()
      # aggregate up to Canada
      out <- out %>%
        dplyr::mutate(region = "CAN") %>%
        dplyr::group_by(.data$name, .data$region, .data$date) %>%
        dplyr::summarise(value = sum(.data$value), value_daily = sum(.data$value_daily), .groups = "drop")
      # return data
      out
    },
    error = function(e) {
      print(e)
      cat("Error in agg2can", fill = TRUE)
    }
  )
}

#' Generated completeness metadata for PT-level data aggregated up to Canada-level
#'
#' @rdname process_funs
#'
#' @export
agg2can_completeness <- function(d) {
  tryCatch(
    {
      # limit to maximum date (2023-12-31)
      d <- d[d$date <= as.Date("2023-12-31"), ]
      # get region values for each date
      out_pt <- split(d$region, d$date)
      # count number of regions for each date
      out_n <- lapply(out_pt, function(x) length(x))
      # combine
      out <- lapply(seq_along(out_pt), function(i) {
        list(
          "pt_n" = out_n[[i]],
          "pt" = out_pt[[i]]
        )
      })
      names(out) <- names(out_pt)
      # return list
      out
    },
    error = function(e) {
      print(e)
      cat("Error in agg2can_completeness", fill = TRUE)
    }
  )
}

#' Format the collated dataset for writing
#'
#' @rdname process_funs
#'
#' @export
dataset_format <- function(d, geo = c("pt", "hr", "sub-hr"), digits) {
  tryCatch(
    {
      match.arg(geo, c("pt", "hr", "sub-hr"), several.ok = FALSE)
      # drop value_daily column if present
      d <- d[, names(d) != "value_daily"]
      # expand dates and regions
      out <- split(d, d$region) # ensure separate date ranges for each region
      switch(
        geo,
        "pt" = {
          col_names <- c("name", "region", "date")
          out <- lapply(out, function(x) {
            tidyr::expand(
              x,
              tidyr::nesting(x$name, x$region),
              date = seq.Date(min(x$date), max(x$date), by = "day"),
              .name_repair = function(y) sub("^x\\$", "", y))
          }) %>% dplyr::bind_rows()
        },
        "hr" = {
          col_names <- c("name", "region", "date", "sub_region_1")
          out <- lapply(out, function(x) {
            tidyr::expand(
              x,
              tidyr::nesting(x$name, x$region, x$sub_region_1),
              date = seq.Date(min(x$date), max(x$date), by = "day"),
              .name_repair = function(y) sub("^x\\$", "", y))
          }) %>% dplyr::bind_rows()
        },
        "sub-hr" = {
          col_names <- c("name", "region", "date", "sub_region_1", "sub_region_2")
          out <- lapply(out, function(x) {
            tidyr::expand(
              x,
              tidyr::nesting(x$name, x$region, x$sub_region_1, x$sub_region_2),
              date = seq.Date(min(x$date), max(x$date), by = "day"),
              .name_repair = function(y) sub("^x\\$", "", y))
          }) %>% dplyr::bind_rows()
        }
      )
      # join and format data
      out <- dplyr::right_join(d, out, by = col_names) %>%
        # dplyr::arrange(!!!rlang::syms(col_names)[col_names != "date"]) %>%
        # fill missing values
        dplyr::group_by(dplyr::across(c(-.data$date, -.data$value))) %>%
        dplyr::arrange(.data$date) %>%
        tidyr::fill(.data$value, .direction = "down") %>%
        tidyr::replace_na(list(value = 0)) %>%
        # calculate daily differences
        dplyr::mutate(value_daily = c(dplyr::first(.data$value), diff(.data$value))) %>%
        dplyr::ungroup() %>%
        # final sort
        dplyr::arrange(!!!rlang::syms(col_names))
      # round numeric values if digits arg is provided
      if (!missing(digits)) {
        if (is.numeric(out$value)) {
          out$value <- round(out$value, digits = digits)
        }
        if (is.numeric(out$value_daily)) {
          out$value_daily <- round(out$value_daily, digits = digits)
        }
      }
      # check for duplicate observations
      dedup <- dplyr::distinct(out, dplyr::across(c(-.data$value, -.data$value_daily)))
      if (nrow(out) != nrow(dedup)) {
        print(
          dplyr::count(out, dplyr::across(c(-.data$value, -.data$value_daily))) %>%
            dplyr::filter(.data$n > 1))
        stop("Observations are not unique: check for data overlaps")
      }
      # return data
      out
    },
    error = function(e) {
      print(e)
      cat("Error in dataset_format:", geo, fill = TRUE)
    }
  )
}
