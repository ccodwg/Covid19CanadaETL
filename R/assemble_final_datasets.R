#' Assemble and write final datasets from raw datasets for CovidTimelineCanada
#'
#' @importFrom rlang .data
#'
#' @export
assemble_final_datasets <- function() {

  # announce start
  cat("Assembling final datasets...", fill = TRUE)

  # case dataset

  ## ab
  cases_ab <- read_d("raw_data/active_ts/ab/ab_cases_hr_ts.csv")

  ## bc
  cases_bc <- read_d("raw_data/active_ts/bc/bc_cases_hr_ts.csv") %>%
    drop_sub_regions("Out of Canada")

  ## mb
  mb1 <- dplyr::bind_rows(
    read_d("raw_data/static/mb/mb_cases_hr_ts.csv") %>%
      date_shift(1),
    read_d("raw_data/reports/mb/mb_weekly_report.csv") %>%
      report_pluck("cases", "cumulative_cases", "value", "hr")
  )
  mb2 <- read_d("raw_data/reports/mb/mb_weekly_report_2.csv") %>%
    report_pluck("cases", "cases", "value_daily", "hr")
  cases_mb <- append_daily_d(mb1, mb2)
  rm(mb1, mb2) # cleanup

  ## nb
  cases_nb <- dplyr::bind_rows(
    get_ccodwg_d("cases", "NB", to = "2021-03-07", drop_not_reported = TRUE) %>%
      convert_hr_names(),
    read_d("raw_data/static/nb/nb_cases_hr_ts.csv") %>%
      convert_hr_names(),
    read_d("raw_data/reports/nb/nb_weekly_report.csv") %>%
      report_pluck("cases", "cumulative_cases", "value", "hr") %>%
      convert_hr_names(),
  )
  nb1 <- read_d("raw_data/reports/nb/nb_weekly_report_2.csv") %>%
    report_pluck("cases", "cases", "value_daily", "hr") %>%
    convert_hr_names()
  cases_nb <- append_daily_d(cases_nb, nb1)
  rm(nb1) # cleanup

  ## nl
  cases_nl <- dplyr::bind_rows(
    get_ccodwg_d("cases", "NL", to = "2021-03-15", drop_not_reported = TRUE) %>%
      convert_hr_names(),
    read_d("raw_data/static/nl/nl_cases_hr_ts.csv") %>%
      convert_hr_names(),
    read_d("raw_data/active_ts/nl/nl_cases_pt_ts.csv") %>%
      dplyr::filter(.data$date >= as.Date("2022-03-12")) %>%
      dplyr::transmute(
        name = .data$name,
        region = .data$region,
        sub_region_1 = "Unknown",
        date = .data$date,
        value = cumsum(.data$value_daily))
  ) %>%
    convert_hr_names()

  ## ns
  ns1 <- read_d("raw_data/static/ns/ns_cases_hr_ts_1.csv")
  ns2 <- read_d("raw_data/static/ns/ns_cases_hr_ts_2.csv") %>%
    dplyr::filter(.data$date <= as.Date("2021-12-09"))
  cases_ns <- append_daily_d(ns1, ns2)
  ns3 <- read_d("raw_data/reports/ns/ns_daily_news_release.csv") %>%
    report_pluck("cases", "cases", "value_daily", "hr") %>%
    dplyr::mutate(sub_region_1 = sub("Zone \\d - ", "", .data$sub_region_1))
  cases_ns <- append_daily_d(cases_ns, ns3)
  ns4 <- read_d("raw_data/active_ts/ns/ns_cases_pt_ts.csv") %>%
    dplyr::mutate(value_daily = c(dplyr::slice_head(., n = 1) %>% dplyr::pull(.data$value), diff(.data$value))) %>% # convert to daily
    dplyr::filter(.data$date >= as.Date("2022-03-05")) %>%
    add_hr_col("Unknown")
  cases_ns <- append_daily_d(cases_ns, ns4)
  rm(ns1, ns2, ns3, ns4) # cleanup

  ## nt
  cases_nt <- dplyr::bind_rows(
    get_phac_d("cases_daily", "NT", keep_up_to_date = TRUE) %>%
      add_hr_col("Northwest Territories"),
    get_phac_d("cases", "NT", keep_up_to_date = TRUE) %>%
      add_hr_col("Northwest Territories") %>%
      dplyr::filter(.data$date >= as.Date("2022-06-11"))
  )

  ## nu
  cases_nu <- dplyr::bind_rows(
    get_phac_d("cases_daily", "NU", keep_up_to_date = TRUE) %>%
      add_hr_col("Nunavut"),
    get_phac_d("cases", "NU", keep_up_to_date = TRUE) %>%
      add_hr_col("Nunavut") %>%
      dplyr::filter(.data$date >= as.Date("2022-06-11"))
  )

  ## on
  cases_on <- dplyr::bind_rows(
    read_d("raw_data/active_ts/on/on_cases_hr_ts.csv")
  ) |>
    convert_hr_names()

  ## pe
  cases_pe <- dplyr::bind_rows(
    get_phac_d("cases_daily", "PE", keep_up_to_date = TRUE) %>%
      add_hr_col("Prince Edward Island"),
    get_phac_d("cases", "PE", keep_up_to_date = TRUE) %>%
      add_hr_col("Prince Edward Island") %>%
      dplyr::filter(.data$date >= as.Date("2022-06-11"))
  )

  ## qc
  tryCatch(
    {
      cases_qc <- read_d("raw_data/active_ts/qc/qc_cases_hr_ts.csv") %>%
        dplyr::filter(.data$sub_region_1 != "Hors Qu\u00E9bec") %>%
        dplyr::mutate(
          sub_region_1 = ifelse(.data$sub_region_1 == "Inconnu", "Unknown", .data$sub_region_1)) %>%
        date_shift(1)
    },
    error = function(e) {
      print(e)
      cat("Error in processing pipeline", fill = TRUE)
    }
  )

  ## sk
  tryCatch(
    {
      sk1 <- read_d("raw_data/static/sk/sk_cases_hr_ts.csv")
      sk2 <- read_d("raw_data/reports/sk/sk_weekly_report.csv") %>%
        report_pluck("cases", "cases", "value_daily", "hr") %>%
        dplyr::filter(.data$date > as.Date("2022-02-06")) # overlaps with end of TS
      sk3 <- read_d("raw_data/reports/sk/sk_monthly_report.csv") %>%
        report_pluck("cases", "cases", "value_daily", "hr")
      sk4 <- read_d("raw_data/reports/sk/sk_crisp_report.csv") %>%
        report_pluck("cases", "cases", "value_daily", "hr") %>%
        report_recent()
      cases_sk <- append_daily_d(sk1, sk2) %>%
        dplyr::mutate(
          sub_region_1 = ifelse(.data$sub_region_1 == "Not Assigned", "Unknown", .data$sub_region_1))
      cases_sk <- append_daily_d(cases_sk, sk3)
      cases_sk <- append_daily_d(cases_sk, sk4)
      rm(sk1, sk2, sk3, sk4) # cleanup
    },
  error = function(e) {
    print(e)
    cat("Error in processing pipeline", fill = TRUE)
    }
  )

  ## yt
  cases_yt <- read_d("raw_data/static/yt/yt_cases_pt_ts.csv") %>%
    add_hr_col("Yukon")

  ## collate and process final dataset
  suppressWarnings(rm(cases_hr)) # if re-running manually
  cases_hr <- collate_datasets("cases") %>%
    convert_hr_names() %>%
    dataset_format("hr")

  # death dataset

  ## ab
  deaths_ab <- dplyr::bind_rows(
    get_ccodwg_d("deaths", "AB", to = "2020-06-22", drop_not_reported = FALSE) %>%
      convert_hr_names(),
    read_d("raw_data/active_cumul/ab/ab_deaths_hr_ts.csv")
  )

  ## bc
  deaths_bc <- dplyr::bind_rows(
    get_ccodwg_d("deaths", "BC", to = "2022-04-01", drop_not_reported = TRUE) %>%
      convert_hr_names(),
    read_d("raw_data/active_cumul/bc/bc_deaths_hr_ts.csv")
  )

  ## mb
  tryCatch(
    {
      mb1 <- read_d("raw_data/static/mb/mb_deaths_hr_ts.csv") %>%
        date_shift(1) %>%
        dplyr::filter(.data$date <= as.Date("2022-03-19"))
      deaths_hr <- mb1 %>%
        dplyr::group_by(.data$sub_region_1) %>%
        dplyr::slice_tail(n = 1) %>%
        dplyr::pull(.data$value) %>%
        sum() # deaths assigned to a health region
      mb2 <- read_d("raw_data/reports/mb/mb_weekly_report.csv") %>%
        report_pluck("deaths", "cumulative_deaths", "value", "pt") %>%
        add_hr_col("Unknown") %>%
        dplyr::mutate(value = .data$value - deaths_hr) # subtract deaths assigned to a health region
      mb3 <- get_phac_d("deaths", "MB", keep_up_to_date = TRUE) %>%
        dplyr::filter(.data$date >= as.Date("2022-11-12")) %>%
          add_hr_col("Unknown") %>%
        dplyr::mutate(value = .data$value - deaths_hr) # subtract deaths assigned to a health region
      deaths_mb <- dplyr::bind_rows(mb1, mb2, mb3)
      rm(mb1, mb2, mb3, deaths_hr) # cleanup
    },
    error = function(e) {
      print(e)
      cat("Error in processing pipeline", fill = TRUE)
    }
  )

  ## nb
  deaths_nb <- dplyr::bind_rows(
    get_ccodwg_d("deaths", "NB", to = "2021-03-07", drop_not_reported = TRUE) %>%
      convert_hr_names(),
    read_d("raw_data/static/nb/nb_deaths_hr_ts.csv") %>%
      convert_hr_names(),
    read_d("raw_data/reports/nb/nb_weekly_report.csv") %>%
      report_pluck("deaths", "cumulative_deaths", "value", "hr") %>%
      convert_hr_names()
  )
  nb1 <- read_d("raw_data/reports/nb/nb_weekly_report_2.csv") %>%
    report_pluck("deaths", "deaths", "value_daily", "pt") %>%
    add_hr_col("Unknown")
  deaths_nb <- append_daily_d(deaths_nb, nb1)
  rm(nb1) # cleanup

  ## nl
  deaths_nl <- dplyr::bind_rows(
    get_ccodwg_d("deaths", "NL", to = "2021-03-15", drop_not_reported = TRUE) %>%
      convert_hr_names(),
    read_d("raw_data/static/nl/nl_deaths_hr_ts_1.csv") %>%
      convert_hr_names(),
    read_d("raw_data/static/nl/nl_deaths_hr_ts_2.csv") %>%
      convert_hr_names(),
    read_d("raw_data/active_cumul/nl/nl_deaths_hr_ts.csv")
  )

  ## ns
  tryCatch(
    {
      ns1 <- get_ccodwg_d("deaths", "NS", to = "2021-01-18", drop_not_reported = TRUE) %>%
        dplyr::mutate(sub_region_1 = sub("Zone \\d - ", "", .data$sub_region_1))
      ns2 <- read_d("raw_data/static/ns/ns_deaths_hr_ts.csv") %>%
        dplyr::filter(.data$date <= as.Date("2021-12-09")) %>%
        dplyr::mutate(sub_region_1 = sub("Zone \\d - ", "", .data$sub_region_1))
      deaths_ns <- dplyr::bind_rows(ns1, ns2)
      ns3 <- read_d("raw_data/reports/ns/ns_daily_news_release.csv") %>%
        report_pluck("deaths", "deaths", "value_daily", "hr") %>%
        dplyr::mutate(sub_region_1 = sub("Zone \\d - ", "", .data$sub_region_1))
      deaths_ns <- append_daily_d(deaths_ns, ns3)
      ns4 <- read_d("raw_data/reports/ns/ns_weekly_report.csv") %>%
        report_pluck("deaths", "deaths", "value_daily", "pt") %>%
        dplyr::filter(date == as.Date("2022-03-08")) %>% # use daily value from first weekly report
        add_hr_col("Unknown")
      deaths_ns <- append_daily_d(deaths_ns, ns4)
      ns5 <- read_d("raw_data/reports/ns/ns_weekly_report.csv") %>%
        report_pluck("deaths", "cumulative_deaths", "value", "pt") %>%
        # dplyr::filter(date >= as.Date("2022-03-15")) %>% # use cumulative value from second weekly report forward
        add_hr_col("Unknown")
      # subtract out deaths assigned to a health region from the cumulative value for unknown health region
      ns5$value <- ns5$value - deaths_ns %>%
        dplyr::filter(.data$sub_region_1 != "Unknown") %>%
        dplyr::group_by(.data$sub_region_1) %>%
        dplyr::summarize(value = max(.data$value)) %>%
        dplyr::pull(.data$value) %>%
        sum()
      deaths_ns <- dplyr::bind_rows(deaths_ns, ns5)
      rm(ns1, ns2, ns3, ns4, ns5) # cleanup
    },
    error = function(e) {
      print(e)
      cat("Error in processing pipeline", fill = TRUE)
    }
  )

  ## nt
  deaths_nt <- dplyr::bind_rows(
    get_phac_d("deaths_daily", "NT", keep_up_to_date = TRUE) %>%
      add_hr_col("Northwest Territories"),
    get_phac_d("deaths", "NT", keep_up_to_date = TRUE) %>%
      add_hr_col("Northwest Territories") %>%
      dplyr::filter(.data$date >= as.Date("2022-06-11"))
  )

  ## nu
  deaths_nu <- dplyr::bind_rows(
    get_phac_d("deaths_daily", "NU", keep_up_to_date = TRUE) %>%
      add_hr_col("Nunavut"),
  get_phac_d("deaths", "NU", keep_up_to_date = TRUE) %>%
    add_hr_col("Nunavut") %>%
    dplyr::filter(.data$date >= as.Date("2022-06-11"))
  )

  ## on
  deaths_on <- dplyr::bind_rows(
    read_d("raw_data/active_ts/on/on_deaths_hr_ts.csv")
  ) |>
    convert_hr_names()

  ## pe
  deaths_pe <- dplyr::bind_rows(
    get_phac_d("deaths_daily", "PE", keep_up_to_date = TRUE) %>%
      add_hr_col("Prince Edward Island"),
    get_phac_d("deaths", "PE", keep_up_to_date = TRUE) %>%
      add_hr_col("Prince Edward Island") %>%
      dplyr::filter(.data$date >= as.Date("2022-06-11"))
  )

  ## qc
  tryCatch(
    {
      deaths_qc <- read_d("raw_data/active_ts/qc/qc_deaths_hr_ts.csv") %>%
        dplyr::filter(.data$sub_region_1 != "Hors Qu\u00E9bec") %>%
        dplyr::mutate(
          sub_region_1 = ifelse(.data$sub_region_1 == "Inconnu", "Unknown", .data$sub_region_1)) %>%
        date_shift(1)
    },
    error = function(e) {
      print(e)
      cat("Error in processing pipeline", fill = TRUE)
    }
  )

  ## sk
  tryCatch(
    {
      sk1 <- read_d("raw_data/static/sk/sk_deaths_hr_ts.csv") %>%
        dplyr::mutate(name = "deaths") %>% # may want to fix in source data
        dplyr::filter(.data$sub_region_1 != "Total") # may want to fix in source data
      sk2 <- read_d("raw_data/reports/sk/sk_weekly_report.csv") %>%
        report_pluck("deaths", "deaths", "value_daily", "hr") %>%
        dplyr::filter(.data$date > as.Date("2022-02-06")) # overlaps with end of TS
      sk3 <- read_d("raw_data/reports/sk/sk_monthly_report.csv") %>%
        report_pluck("deaths", "deaths", "value_daily", "hr")
      sk4 <- read_d("raw_data/reports/sk/sk_crisp_report.csv") %>%
        report_pluck("deaths", "deaths", "value_daily", "hr") %>%
        report_recent()
      deaths_sk <- append_daily_d(sk1, sk2) %>%
        dplyr::mutate(
          sub_region_1 = ifelse(.data$sub_region_1 == "Not Assigned", "Unknown", .data$sub_region_1))
      deaths_sk <- append_daily_d(deaths_sk, sk3)
      deaths_sk <- append_daily_d(deaths_sk, sk4)
      rm(sk1, sk2, sk3, sk4) # cleanup
    },
    error = function(e) {
      print(e)
      cat("Error in processing pipeline", fill = TRUE)
    }
  )

  ## yt
  deaths_yt <- dplyr::bind_rows(
    get_phac_d("deaths_daily", "YT", keep_up_to_date = TRUE) %>%
      add_hr_col("Yukon"),
    get_phac_d("deaths", "YT", keep_up_to_date = TRUE) %>%
      add_hr_col("Yukon") %>%
      dplyr::filter(.data$date >= as.Date("2022-06-11"))
  )

  ## collate and process final dataset
  suppressWarnings(rm(deaths_hr)) # if re-running manually
  deaths_hr <- collate_datasets("deaths") %>%
    convert_hr_names() %>%
    dataset_format("hr")

  # hospitalizations dataset

  ## ab
  hospitalizations_ab <- get_covid19tracker_d("hospitalizations", "AB")

  ## bc
  hospitalizations_bc <- get_covid19tracker_d("hospitalizations", "BC")

  ## mb
  hospitalizations_mb <- dplyr::bind_rows(
    get_covid19tracker_d("hospitalizations", "MB", to = "2021-02-03"),
    read_d("raw_data/static/mb/mb_hospitalizations_pt_ts.csv"),
    get_covid19tracker_d("hospitalizations", "MB", from = "2022-03-26")
  )

  ## nb
  hospitalizations_nb <- dplyr::bind_rows(
    get_covid19tracker_d("hospitalizations", "NB", to = "2021-03-07"),
    read_d("raw_data/static/nb/nb_hospitalizations_pt_ts_1.csv"),
    read_d("raw_data/static/nb/nb_hospitalizations_pt_ts_2.csv"),
    read_d("raw_data/reports/nb/nb_weekly_report.csv") %>%
      report_pluck("hospitalizations", "active_hospitalizations", "value", "pt")
  )

  ## nl
  hospitalizations_nl <- dplyr::bind_rows(
    read_d("raw_data/static/nl/nl_hospitalizations_pt_ts.csv"),
    get_covid19tracker_d("hospitalizations", "NL", from = "2022-03-12")
  )

  ## ns
  hospitalizations_ns <- dplyr::bind_rows(
    get_covid19tracker_d("hospitalizations", "NS", to = "2021-01-18"),
    read_d("raw_data/static/ns/ns_hospitalizations_pt_ts.csv"),
    get_covid19tracker_d("hospitalizations", "NS", from = "2022-01-19")
  )

  ## nt
  hospitalizations_nt <- get_covid19tracker_d("hospitalizations", "NT")

  ## nu
  hospitalizations_nu <- get_covid19tracker_d("hospitalizations", "NU")

  ## on
  hospitalizations_on <- read_d("raw_data/active_ts/on/on_hospitalizations_pt_ts.csv")

  ## pe
  hospitalizations_pe <- get_covid19tracker_d("hospitalizations", "PE")

  ## qc
  hospitalizations_qc <- read_d("raw_data/active_ts/qc/qc_hospitalizations_pt_ts.csv") %>%
    date_shift(1)

  ## sk
  hospitalizations_sk <- dplyr::bind_rows(
    read_d("raw_data/static/sk/sk_hospitalizations_pt_ts.csv"),
    get_covid19tracker_d("hospitalizations", "SK", from = "2022-02-07")
  )

  ## yt
  hospitalizations_yt <- get_covid19tracker_d("hospitalizations", "YT")

  ## collate and process final dataset
  suppressWarnings(rm(hospitalizations_pt)) # if re-running manually
  hospitalizations_pt <- collate_datasets("hospitalizations") %>%
    dataset_format("pt")

  ## Canadian dataset (NOT an aggregate of PT datasets)
  hospitalizations_can <- get_phac_d("hospitalizations", "CAN") %>%
    dataset_format("pt")

  # icu dataset

  ## ab
  icu_ab <- get_covid19tracker_d("icu", "AB")

  ## bc
  icu_bc <- get_covid19tracker_d("icu", "BC")

  ## mb
  icu_mb <- dplyr::bind_rows(
    get_covid19tracker_d("icu", "MB", to = "2021-02-03"),
    read_d("raw_data/static/mb/mb_icu_pt_ts.csv"),
    get_covid19tracker_d("icu", "MB", from = "2022-03-26")
  )

  ## nb
  icu_nb <- dplyr::bind_rows(
    get_covid19tracker_d("icu", "NB", to = "2021-03-07"),
    read_d("raw_data/static/nb/nb_icu_pt_ts_1.csv"),
    read_d("raw_data/static/nb/nb_icu_pt_ts_2.csv"),
    read_d("raw_data/reports/nb/nb_weekly_report.csv") %>%
      report_pluck("icu", "active_icu", "value", "pt")
  )

  ## nl
  icu_nl <- dplyr::bind_rows(
    get_covid19tracker_d("icu", "NL", to = "2021-03-15"),
    read_d("raw_data/static/nl/nl_icu_pt_ts.csv"),
    get_covid19tracker_d("icu", "NL", from = "2022-03-12")
  )

  ## ns
  icu_ns <- dplyr::bind_rows(
    get_covid19tracker_d("icu", "NS", to = "2021-01-18"),
    read_d("raw_data/static/ns/ns_icu_pt_ts.csv"),
    get_covid19tracker_d("icu", "NS", from = "2022-01-19")
  )

  ## nt
  icu_nt <- get_covid19tracker_d("icu", "NT")

  ## nu
  icu_nu <- get_covid19tracker_d("icu", "NU")

  ## on
  icu_on <- read_d("raw_data/active_ts/on/on_icu_pt_ts.csv")

  ## pe
  icu_pe <- get_covid19tracker_d("icu", "PE")

  ## qc
  icu_qc <- read_d("raw_data/active_ts/qc/qc_icu_pt_ts.csv") %>%
    date_shift(1)

  ## sk
  icu_sk <- dplyr::bind_rows(
    read_d("raw_data/static/sk/sk_icu_pt_ts.csv"),
    get_covid19tracker_d("icu", "SK", from = "2022-02-07")
  )

  ## yt
  icu_yt <- get_covid19tracker_d("icu", "YT")

  ## collate and process final dataset
  suppressWarnings(rm(icu_pt)) # if re-running manually
  icu_pt <- collate_datasets("icu") %>%
    dataset_format("pt")

  ## Canadian dataset (NOT an aggregate of PT datasets)
  icu_can <- get_phac_d("icu", "CAN") %>%
    dataset_format("pt")

  # tests_completed dataset

  ## all regions
  tests_completed_pt <- get_phac_d("tests_completed", "all", keep_up_to_date = TRUE) %>%
    dplyr::filter(!.data$region %in% c("AB", "YT")) %>%
    dplyr::filter(.data$date <= as.Date("2022-11-22"))

  ## replace AB and YT
  tests_completed_pt <- dplyr::bind_rows(
    tests_completed_pt,
    read_d("raw_data/active_ts/ab/ab_tests_completed_pt_ts.csv"),
    read_d("raw_data/static/yt/yt_tests_completed_pt_ts.csv")
  )

  ## collate and process final dataset
   tests_completed_pt <- tests_completed_pt %>%
    dataset_format("pt")

  # vaccine coverage dataset

  ## collate and process final datasets
  vaccine_coverage_dose_1_pt <- get_phac_d("vaccine_coverage_dose_1", "all") %>%
    dplyr::filter(.data$region != "QC") %>%
    dplyr::bind_rows(read_d("raw_data/static/can/can_vaccine_coverage_dose_1_pt_ts_qc.csv", val_numeric = TRUE)) %>%
    dplyr::filter(!(.data$region == "QC" & date > as.Date("2022-09-11"))) %>%
    dataset_format("pt", digits = 2)
  vaccine_coverage_dose_2_pt <- get_phac_d("vaccine_coverage_dose_2", "all") %>%
    dplyr::filter(.data$region != "QC") %>%
    dplyr::bind_rows(read_d("raw_data/static/can/can_vaccine_coverage_dose_2_pt_ts_qc.csv", val_numeric = TRUE)) %>%
    dplyr::filter(!(.data$region == "QC" & date > as.Date("2022-09-11"))) %>%
    dataset_format("pt", digits = 2)
  vaccine_coverage_dose_3_pt <- get_phac_d("vaccine_coverage_dose_3", "all") %>%
    dplyr::filter(.data$region != "QC") %>%
    dplyr::bind_rows(read_d("raw_data/static/can/can_vaccine_coverage_dose_3_pt_ts_qc.csv", val_numeric = TRUE)) %>%
    dplyr::filter(!(.data$region == "QC" & date > as.Date("2022-09-11"))) %>%
    dataset_format("pt", digits = 2)
  vaccine_coverage_dose_4_pt <- get_phac_d("vaccine_coverage_dose_4", "all") %>%
    dplyr::filter(.data$region != "QC") %>%
    dplyr::bind_rows(read_d("raw_data/static/can/can_vaccine_coverage_dose_4_pt_ts_qc.csv", val_numeric = TRUE)) %>%
    dplyr::filter(!(.data$region == "QC" & date > as.Date("2022-09-11"))) %>%
    dataset_format("pt", digits = 2)

  ## Canadian datasets (NOT an aggregate of PT datasets)
  vaccine_coverage_dose_1_can <- get_phac_d("vaccine_coverage_dose_1", "CAN") %>%
    dataset_format("pt", digits = 2)
  vaccine_coverage_dose_2_can <- get_phac_d("vaccine_coverage_dose_2", "CAN") %>%
    dataset_format("pt", digits = 2)
  vaccine_coverage_dose_3_can <- get_phac_d("vaccine_coverage_dose_3", "CAN") %>%
    dataset_format("pt", digits = 2)
  vaccine_coverage_dose_4_can <- get_phac_d("vaccine_coverage_dose_4", "CAN") %>%
    dataset_format("pt", digits = 2)

  # vaccine administration dataset

  ## collate and process final datasets
  vaccine_administration_dose_1_pt <- get_phac_d("vaccine_administration_dose_1", "all") %>%
    dplyr::filter(.data$region != "QC") %>%
    dplyr::bind_rows(read_d("raw_data/static/can/can_vaccine_administration_dose_1_pt_ts_qc.csv")) %>%
    dplyr::filter(!(.data$region == "QC" & date > as.Date("2022-09-11"))) %>%
    dataset_format("pt")
  vaccine_administration_dose_2_pt <- get_phac_d("vaccine_administration_dose_2", "all") %>%
    dplyr::filter(.data$region != "QC") %>%
    dplyr::bind_rows(read_d("raw_data/static/can/can_vaccine_administration_dose_2_pt_ts_qc.csv")) %>%
    dplyr::filter(!(.data$region == "QC" & date > as.Date("2022-09-11"))) %>%
    dataset_format("pt")
  vaccine_administration_dose_3_pt <- get_phac_d("vaccine_administration_dose_3", "all") %>%
    dplyr::filter(.data$region != "QC") %>%
    dplyr::bind_rows(read_d("raw_data/static/can/can_vaccine_administration_dose_3_pt_ts_qc.csv")) %>%
    dplyr::filter(!(.data$region == "QC" & date > as.Date("2022-09-11"))) %>%
    dataset_format("pt")
  vaccine_administration_dose_4_pt <- get_phac_d("vaccine_administration_dose_4", "all") %>%
    dplyr::filter(.data$region != "QC") %>%
    dplyr::bind_rows(read_d("raw_data/static/can/can_vaccine_administration_dose_4_pt_ts_qc.csv")) %>%
    dplyr::filter(!(.data$region == "QC" & date > as.Date("2022-09-11"))) %>%
    dataset_format("pt")
  vaccine_administration_total_doses_pt <- get_phac_d("vaccine_administration_total_doses", "all") %>%
    dplyr::filter(.data$region != "QC") %>%
    dplyr::bind_rows(read_d("raw_data/static/can/can_vaccine_administration_total_doses_pt_ts_qc.csv")) %>%
    dplyr::filter(!(.data$region == "QC" & date > as.Date("2022-09-11"))) %>%
    dataset_format("pt")

  ## Canadian datasets (NOT an aggregate of PT datasets)
  vaccine_administration_dose_1_can <- get_phac_d("vaccine_administration_dose_1", "CAN") %>%
    dataset_format("pt")
  vaccine_administration_dose_2_can <- get_phac_d("vaccine_administration_dose_2", "CAN") %>%
    dataset_format("pt")
  vaccine_administration_dose_3_can <- get_phac_d("vaccine_administration_dose_3", "CAN") %>%
    dataset_format("pt")
  vaccine_administration_dose_4_can <- get_phac_d("vaccine_administration_dose_4", "CAN") %>%
    dataset_format("pt")
  vaccine_administration_total_doses_can <- get_phac_d("vaccine_administration_total_doses", "CAN") %>%
    dataset_format("pt")

  # create aggregated datasets (HR -> PT)
  cases_pt <- agg2pt(cases_hr)
  deaths_pt <- agg2pt(deaths_hr)

  # create aggregated datasets (PT -> CAN)
  cases_can <- agg2can(cases_pt)
  cases_can_completeness <- agg2can_completeness(cases_pt)
  deaths_can <- agg2can(deaths_pt)
  deaths_can_completeness <- agg2can_completeness(deaths_pt)
  tests_completed_can <- agg2can(tests_completed_pt)

  # write datasets
  cat("Writing final datasets...", fill = TRUE)
  write_dataset(cases_hr, "hr", "cases_hr")
  write_dataset(cases_pt, "pt", "cases_pt")
  write_dataset(cases_can, "can", "cases_can")
  write_dataset(cases_can_completeness, "can", "cases_can_completeness", ext = "json")
  write_dataset(deaths_hr, "hr", "deaths_hr")
  write_dataset(deaths_pt, "pt", "deaths_pt")
  write_dataset(deaths_can, "can", "deaths_can")
  write_dataset(deaths_can_completeness, "can", "deaths_can_completeness", ext = "json")
  write_dataset(hospitalizations_pt, "pt", "hospitalizations_pt")
  write_dataset(hospitalizations_can, "can", "hospitalizations_can")
  write_dataset(icu_pt, "pt", "icu_pt")
  write_dataset(icu_can, "can", "icu_can")
  write_dataset(tests_completed_pt, "pt", "tests_completed_pt")
  write_dataset(tests_completed_can, "can", "tests_completed_can")
  write_dataset(vaccine_coverage_dose_1_pt, "pt", "vaccine_coverage_dose_1_pt")
  write_dataset(vaccine_coverage_dose_1_can, "can", "vaccine_coverage_dose_1_can")
  write_dataset(vaccine_coverage_dose_2_pt, "pt", "vaccine_coverage_dose_2_pt")
  write_dataset(vaccine_coverage_dose_2_can, "can", "vaccine_coverage_dose_2_can")
  write_dataset(vaccine_coverage_dose_3_pt, "pt", "vaccine_coverage_dose_3_pt")
  write_dataset(vaccine_coverage_dose_3_can, "can", "vaccine_coverage_dose_3_can")
  write_dataset(vaccine_coverage_dose_4_pt, "pt", "vaccine_coverage_dose_4_pt")
  write_dataset(vaccine_coverage_dose_4_can, "can", "vaccine_coverage_dose_4_can")
  write_dataset(vaccine_administration_dose_1_pt, "pt", "vaccine_administration_dose_1_pt")
  write_dataset(vaccine_administration_dose_1_can, "can", "vaccine_administration_dose_1_can")
  write_dataset(vaccine_administration_dose_2_pt, "pt", "vaccine_administration_dose_2_pt")
  write_dataset(vaccine_administration_dose_2_can, "can", "vaccine_administration_dose_2_can")
  write_dataset(vaccine_administration_dose_3_pt, "pt", "vaccine_administration_dose_3_pt")
  write_dataset(vaccine_administration_dose_3_can, "can", "vaccine_administration_dose_3_can")
  write_dataset(vaccine_administration_dose_4_pt, "pt", "vaccine_administration_dose_4_pt")
  write_dataset(vaccine_administration_dose_4_can, "can", "vaccine_administration_dose_4_can")
  write_dataset(vaccine_administration_total_doses_pt, "pt", "vaccine_administration_total_doses_pt")
  write_dataset(vaccine_administration_total_doses_can, "can", "vaccine_administration_total_doses_can")
}
