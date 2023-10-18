#' Assemble and write extra datasets for CovidTimelineCanada
#'
#' @importFrom rlang .data
#'
#' @export
extra_datasets <- function() {

  # announce start
  cat("Assembling extra datasets...", fill = TRUE)

  # sk biweekly HR-level case snapshots

  ## process
  sk <- read_d("raw_data/reports/sk/sk_crisp_report.csv") |>
    dplyr::transmute(.data$date_start, .data$date_end, .data$region, .data$sub_region_1, cases_weekly = .data$cases) |>
    dplyr::filter(.data$date_start >= as.Date("2022-12-25") & !is.na(.data$sub_region_1) & !is.na(.data$cases_weekly)) |>
    convert_hr_names()

  ## write file
  write.csv(sk, file.path("extra_data", "sk_biweekly_cases_hr.csv"), row.names = FALSE, quote = 1:4)
  rm(sk) # clean up
}
