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

  # download data
  ds <- dl_datasets()

  # set today's date
  date_today <- Sys.Date()
  date_today_2 <- format.Date(date_today, "%d-%m-%Y")

  # process data
  d <- e_t_datasets(ds)

  # add Ontario recovered
  d$on_recovered_prov <- sheets_load(
    "1kiN6BmshBHKRiBTmljUQqd4RdSmUMvuYg5sZA3cmCNA",
    "recovered_timeseries_phu") %>%
    dplyr::select(dplyr::one_of(date_today_2)) %>%
    `[[`(1) %>%
    as.integer() %>%
    sum() %>%
    data.frame(
      name = "recovered",
      province = "ON",
      date = date_today,
      value = .
    )

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

  # filter out data to be replaced by manual data

  ## cases
  hr_cases <- hr_cases %>%
    dplyr::filter(!(.data$province %in% c("Ontario", "Saskatchewan")))

  ## mortality
  hr_mortality <- hr_mortality %>%
    dplyr::filter(!(.data$province %in% c("Ontario", "Saskatchewan")))

  ## vaccine completion
  prov_vaccine_completion <- prov_vaccine_completion %>%
    dplyr::filter(!(.data$province %in% c("Nova Scotia")))

  # upload to Google Sheets

  ## convert date format
  process_format_dates("hr_cases", "hr_mortality",
                       "prov_recovered", "prov_testing",
                       "prov_vaccine_distribution",
                       "prov_vaccine_administration",
                       "prov_vaccine_completion")

  ## format data for uploading
  hr_cases <- process_format_sheets(hr_cases, "hr")
  hr_mortality <- process_format_sheets(hr_mortality, "hr")
  prov_recovered <- process_format_sheets(prov_recovered, "prov")
  prov_testing <- process_format_sheets(prov_testing, "prov")
  prov_vaccine_distribution <- process_format_sheets(prov_vaccine_distribution, "prov")
  prov_vaccine_administration <- process_format_sheets(prov_vaccine_administration, "prov")
  prov_vaccine_completion <- process_format_sheets(prov_vaccine_completion, "prov")

  ## combine with existing data
  hr_cases <- sheets_load(
    "14Hs9R0d9HIRX5t86jw4SowZ_bQ2_cr6e9wgjZe2bfRA",
    "cases_timeseries_hr") %>%
    ## drop today's data if re-doing data update
    dplyr::select(-dplyr::any_of(dplyr::sym(date_today_2))) %>%
    dplyr::left_join(
      hr_cases,
      by = c("province" = "province", "health_region" = "sub_region_1")) %>%
    dplyr::select(.data$province, .data$health_region, as.character(date_today_2),
                  !dplyr::matches(paste0("province|health_region", as.character(date_today_2))))

  hr_mortality <- sheets_load(
    "14Hs9R0d9HIRX5t86jw4SowZ_bQ2_cr6e9wgjZe2bfRA",
    "mortality_timeseries_hr") %>%
    ## drop today's data if re-doing data update
    dplyr::select(-dplyr::any_of(dplyr::sym(date_today_2))) %>%
    dplyr::left_join(
      hr_mortality,
      by = c("province" = "province", "health_region" = "sub_region_1")) %>%
    dplyr::select(.data$province, .data$health_region, as.character(date_today_2),
                  !dplyr::matches(paste0("province|health_region", as.character(date_today_2))))

  prov_recovered <- sheets_load(
    "14Hs9R0d9HIRX5t86jw4SowZ_bQ2_cr6e9wgjZe2bfRA",
    "recovered_timeseries_prov") %>%
    ## drop today's data if re-doing data update
    dplyr::select(-dplyr::any_of(dplyr::sym(date_today_2))) %>%
    dplyr::left_join(
      prov_recovered,
      by = c("province" = "province")) %>%
    dplyr::select(.data$province, as.character(date_today_2),
                  !dplyr::matches(paste0("province", as.character(date_today_2))))

  prov_testing <- sheets_load(
    "14Hs9R0d9HIRX5t86jw4SowZ_bQ2_cr6e9wgjZe2bfRA",
    "testing_timeseries_prov") %>%
    ## drop today's data if re-doing data update
    dplyr::select(-dplyr::any_of(dplyr::sym(date_today_2))) %>%
    dplyr::left_join(
      prov_testing,
      by = c("province" = "province")) %>%
    dplyr::select(.data$province, as.character(date_today_2),
                  !dplyr::matches(paste0("province", as.character(date_today_2))))

  prov_vaccine_distribution<- sheets_load(
    "14Hs9R0d9HIRX5t86jw4SowZ_bQ2_cr6e9wgjZe2bfRA",
    "vaccine_distribution_timeseries_prov") %>%
    ## drop today's data if re-doing data update
    dplyr::select(-dplyr::any_of(dplyr::sym(date_today_2))) %>%
    dplyr::left_join(
      prov_vaccine_distribution,
      by = c("province" = "province")) %>%
    dplyr::select(.data$province, as.character(date_today_2),
                  !dplyr::matches(paste0("province", as.character(date_today_2))))

  prov_vaccine_administration<- sheets_load(
    "14Hs9R0d9HIRX5t86jw4SowZ_bQ2_cr6e9wgjZe2bfRA",
    "vaccine_administration_timeseries_prov") %>%
    ## drop today's data if re-doing data update
    dplyr::select(-dplyr::any_of(dplyr::sym(date_today_2))) %>%
    dplyr::left_join(
      prov_vaccine_administration,
      by = c("province" = "province")) %>%
    dplyr::select(.data$province, as.character(date_today_2),
                  !dplyr::matches(paste0("province", as.character(date_today_2))))

  prov_vaccine_completion <- sheets_load(
    "14Hs9R0d9HIRX5t86jw4SowZ_bQ2_cr6e9wgjZe2bfRA",
    "vaccine_completion_timeseries_prov") %>%
    ## drop today's data if re-doing data update
    dplyr::select(-dplyr::any_of(dplyr::sym(date_today_2))) %>%
    dplyr::left_join(
      prov_vaccine_completion,
      by = c("province" = "province")) %>%
    dplyr::select(.data$province, as.character(date_today_2),
                  !dplyr::matches(paste0("province", as.character(date_today_2))))

  ## write newest data to Google Sheets
  googlesheets4::sheet_write(
    hr_cases,
    "14Hs9R0d9HIRX5t86jw4SowZ_bQ2_cr6e9wgjZe2bfRA",
    "cases_timeseries_hr")
  googlesheets4::sheet_write(
    hr_mortality,
    "14Hs9R0d9HIRX5t86jw4SowZ_bQ2_cr6e9wgjZe2bfRA",
    "mortality_timeseries_hr")
  googlesheets4::sheet_write(
    prov_recovered,
    "14Hs9R0d9HIRX5t86jw4SowZ_bQ2_cr6e9wgjZe2bfRA",
    "recovered_timeseries_prov")
  googlesheets4::sheet_write(
    prov_testing,
    "14Hs9R0d9HIRX5t86jw4SowZ_bQ2_cr6e9wgjZe2bfRA",
    "testing_timeseries_prov")
  googlesheets4::sheet_write(
    prov_vaccine_distribution,
    "14Hs9R0d9HIRX5t86jw4SowZ_bQ2_cr6e9wgjZe2bfRA",
    "vaccine_distribution_timeseries_prov")
  googlesheets4::sheet_write(
    prov_vaccine_administration,
    "14Hs9R0d9HIRX5t86jw4SowZ_bQ2_cr6e9wgjZe2bfRA",
    "vaccine_administration_timeseries_prov")
  googlesheets4::sheet_write(
    prov_vaccine_completion,
    "14Hs9R0d9HIRX5t86jw4SowZ_bQ2_cr6e9wgjZe2bfRA",
    "vaccine_completion_timeseries_prov")

}
