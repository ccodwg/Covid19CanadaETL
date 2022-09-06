#' Diffs for every dataset in CovidTimelineCanada
#'
#' @importFrom rlang .data
#'
#' @export
diff_datasets <- function() {
  # create "diffs" directory and sub-directories
  dir.create("diffs", showWarnings = FALSE)
  dirs <- list.dirs("data", recursive = FALSE)
  lapply(dirs, function(x) dir.create(file.path("diffs", basename(x)), showWarnings = FALSE))
  for (d in dirs) {
    files <- list.files(d, pattern = "*.csv$", full.names = TRUE)
    for (f in files) {
      # read dataset
      dat <- utils::read.csv(f, stringsAsFactors = FALSE)
      # get most recent data for each dataset
      dat <- dat %>%
        dplyr::group_by(dplyr::across(
          dplyr::any_of(
            c("name", "region", "sub_region_1", "sub_region_2")))) %>%
        dplyr::slice_tail(n = 1) %>%
        dplyr::ungroup() %>%
        dplyr::select(dplyr::any_of(
            c("name", "region", "sub_region_1", "sub_region_2", "date", "value")))
      # get diff dataset path
      diff_path <- file.path("diffs", basename(d), basename(f))
      # create new diff dataset if none exists, otherwise update existing dataset
      if (!file.exists(diff_path)) {
        diff <- dat %>%
          dplyr::rename(
            "date_current" = "date",
            "value_current" = "value") %>%
          dplyr::mutate(
            date_previous = "",
            value_previous = "",
            .after = "value_current") %>%
          dplyr::mutate(
            date_diff = "",
            value_diff = ""
          )
      } else {
        # load existing dataset
        diff <- dplyr::tibble(utils::read.csv(diff_path, stringsAsFactors = FALSE))
        # check if data have been updated
        diff_current <- diff %>%
          dplyr::select(dplyr::any_of(
            c("name", "region", "sub_region_1", "sub_region_2", "date_current", "value_current"))) %>%
          dplyr::rename("date" = "date_current", "value" = "value_current")
        # update if data have changed
        if (!identical(dat, diff_current)) {
          # if geographic units differ, bail with warning
          if (!identical(
            dat %>% dplyr::select(dplyr::any_of(
              c("name", "region", "sub_region_1", "sub_region_2"))),
            diff_current %>% dplyr::select(dplyr::any_of(
              c("name", "region", "sub_region_1", "sub_region_2")))
          )) {
            warning(paste0(f, ": Geographic units have changed, aborting diff..."))
            break
          }
          # construct new diff dataset with line-by-line comparisons
          diff <- lapply(seq_along(1:nrow(diff)), function(x) {
            # check if new data is the same as current data in diff
            if (!identical(dat[x, ], diff_current[x, ])) {
              # use new data
              new_diff <- dat %>%
                dplyr::slice(x) %>%
                dplyr::rename("date_current" = "date", "value_current" = "value")
              new_diff <- dplyr::bind_cols(
                new_diff,
                diff_current %>%
                  dplyr::slice(x) %>%
                  dplyr::transmute(date_previous = .data$date, value_previous = .data$value)
                )
            } else {
              # use current data in diff
              diff[x, ]
            }
          }) %>%
            dplyr::bind_rows() %>%
            # calculate date and value diffs
            dplyr::mutate(
              date_diff = as.Date(.data$date_current) - as.Date(.data$date_previous),
              value_diff = ifelse(
                # round vaccine coverage diff to account for floating point error
                grepl("^vaccine_coverage_", basename(f)),
                round(.data$value_current - .data$value_previous, 2),
                .data$value_current - .data$value_previous
              ))
        } else {
          # skip to next step of loop without re-writing diff dataset
          next
        }
        }
      # calculate columns to quote
      n <- ncol(diff)
      cols <- 1:n
      cols <- cols[!cols %in% c(n, n - 1, n - 2, n - 4)]
      # write diff dataset
      utils::write.csv(diff, diff_path, row.names = FALSE, na = "", quote = cols)
    }
  }
}
