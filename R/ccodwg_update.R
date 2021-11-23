#' Perform daily update for CCODWG dataset
#'
#' Daily update is based on today's cumulative totals for each value.
#'
#' @importFrom rlang .data
#' @param email The email to authenticate with Google Sheets. If not provided,
#' manual authentication will be requested.
#'
#'@export
ccodwg_update <- function(email = NULL) {

  # authenticate with Google Sheets
  auth_gs(email)

  # set today's date
  date_today <- Sys.Date()

  ### UPDATE PHU DATA ###

  # download data
  ds_phu <- dl_datasets(mode = "phu")

  # process data
  d_phu <- e_t_datasets(ds_phu, mode = "phu")

  # collate data
  phu_cases <- process_collate(d_phu, "_cases_") %>%
    dplyr::mutate(province = "ON")
  phu_mortality <- process_collate(d_phu, "_mortality_") %>%
    dplyr::mutate(province = "ON")
  phu_recovered <- process_collate(d_phu, "_recovered_")

  ### ON PHU recovered data ###

  # format data for uploading
  phu_recovered <- process_format_sheets(phu_recovered, "hr")

  # merge data with existing data
  phu_recovered <- sheets_merge(phu_recovered,
                                "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
                                "hr",
                                date_today,
                                "recovered_timeseries_phu")

  # upload data
  googlesheets4::sheet_write(
    phu_recovered,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "recovered_timeseries_phu")

  ### UPDATE MAIN DATASET ###

  # download data
  ds <- dl_datasets(mode = "main")

  # process data
  d <- e_t_datasets(ds, mode = "main")

  # add Ontario recovered
  col_today <- as.character(date_today)
  col_today_manual <- paste0(col_today, "_manual")
  phu_rec <- phu_recovered %>%
    dplyr::select(!!rlang::sym(col_today), !!rlang::sym(col_today_manual)) %>%
    dplyr::transmute(
      !!rlang::sym(col_today) := dplyr::case_when(
        !is.na(!!rlang::sym(col_today_manual)) ~ !!rlang::sym(col_today_manual),
        TRUE ~ !!rlang::sym(col_today))) %>%
    dplyr::pull() %>%
    {sum(as.integer(.), na.rm = FALSE)}
  d[["on_recovered_prov"]] <- phu_rec %>%
    data.frame(
      name = "recovered",
      province = "ON",
      date = date_today,
      value = .
    )

  # add Ontario cases and mortality data
  d[["on_cases_hr"]] <- phu_cases
  d[["on_mortality_hr"]] <- phu_mortality

  # collate data and add "repatriated" row as needed
  hr_cases <- process_collate(d, "_cases_") %>%
    dplyr::add_row(
      data.frame(
        name = "cases",
        province = "Repatriated",
        sub_region_1 = "Not Reported",
        date = date_today,
        value = 13
      )
    )
  hr_mortality <- process_collate(d, "_mortality_") %>%
    dplyr::add_row(
      data.frame(
        name = "mortality",
        province = "Repatriated",
        sub_region_1 = "Not Reported",
        date = date_today,
        value = 0
      )
    )
  prov_recovered <- process_collate(d, "_recovered_") %>%
    dplyr::add_row(
      data.frame(
        name = "recovered",
        province = "Repatriated",
        date = date_today,
        value = 13
      )
    )
  prov_testing <- process_collate(d, "_testing_") %>%
    dplyr::add_row(
      data.frame(
        name = "testing",
        province = "Repatriated",
        date = date_today,
        value = 0
      )
    )
  prov_vaccine_distribution <- process_collate(d, "_vaccine_distribution_")
  prov_vaccine_administration <- process_collate(d, "_vaccine_administration_")
  prov_vaccine_completion <- process_collate(d, "_vaccine_completion_")
  prov_vaccine_additional_doses <- process_collate(d, "_vaccine_additional_doses_")

  # filter out data to be replaced by manual data

  ## cases
  hr_cases <- hr_cases %>%
    dplyr::filter(!(.data$province %in% c("Saskatchewan")))
  ## mortality
  hr_mortality <- hr_mortality %>%
    dplyr::filter(!(.data$province %in% c("Saskatchewan")))

  # format data for uploading
  hr_cases <- process_format_sheets(hr_cases, "hr")
  hr_mortality <- process_format_sheets(hr_mortality, "hr")
  prov_recovered <- process_format_sheets(prov_recovered, "prov")
  prov_testing <- process_format_sheets(prov_testing, "prov")
  prov_vaccine_distribution <- process_format_sheets(prov_vaccine_distribution, "prov")
  prov_vaccine_administration <- process_format_sheets(prov_vaccine_administration, "prov")
  prov_vaccine_completion <- process_format_sheets(prov_vaccine_completion, "prov")
  prov_vaccine_additional_doses <- process_format_sheets(prov_vaccine_additional_doses, "prov")

  # merge data with existing data
  hr_cases <- sheets_merge(
    hr_cases,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "hr",
    date_today,
    "cases_timeseries_hr")
  hr_mortality <- sheets_merge(
    hr_mortality,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "hr",
    date_today,
    "mortality_timeseries_hr")
  prov_recovered <- sheets_merge(
    prov_recovered,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "prov",
    date_today,
    "recovered_timeseries_prov")
  prov_testing <- sheets_merge(
    prov_testing,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "prov",
    date_today,
    "testing_timeseries_prov")
  prov_vaccine_distribution <- sheets_merge(
    prov_vaccine_distribution,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "prov",
    date_today,
    "vaccine_distribution_timeseries_prov")
  prov_vaccine_administration <- sheets_merge(
    prov_vaccine_administration,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "prov",
    date_today,
    "vaccine_administration_timeseries_prov")
  prov_vaccine_completion <- sheets_merge(
    prov_vaccine_completion,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "prov",
    date_today,
    "vaccine_completion_timeseries_prov")
  prov_vaccine_additional_doses <- sheets_merge(
    prov_vaccine_additional_doses,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "prov",
    date_today,
    "vaccine_additional_doses_timeseries_prov")

  # fill in missing data for additional doses (not all PTs reoporting)
  zero_today <- list(0)
  names(zero_today) <- eval(date_today)
  prov_vaccine_additional_doses <- prov_vaccine_additional_doses %>%
    tidyr::replace_na(zero_today)

  # upload data
  googlesheets4::sheet_write(
    hr_cases,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "cases_timeseries_hr")
  googlesheets4::sheet_write(
    hr_mortality,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "mortality_timeseries_hr")
  googlesheets4::sheet_write(
    prov_recovered,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "recovered_timeseries_prov")
  googlesheets4::sheet_write(
    prov_testing,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "testing_timeseries_prov")
  googlesheets4::sheet_write(
    prov_vaccine_distribution,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "vaccine_distribution_timeseries_prov")
  googlesheets4::sheet_write(
    prov_vaccine_administration,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "vaccine_administration_timeseries_prov")
  googlesheets4::sheet_write(
    prov_vaccine_completion,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "vaccine_completion_timeseries_prov")
  googlesheets4::sheet_write(
    prov_vaccine_additional_doses,
    "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
    "vaccine_additional_doses_timeseries_prov")
}
