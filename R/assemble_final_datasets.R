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
  ab1 <- read_d("raw_data/static/ab/ab_cases_hr_ts_1.csv") %>%
    convert_hr_names() %>%
    dplyr::filter(.data$date <= as.Date("2020-03-31"))
  ab2 <- read_d("raw_data/static/ab/ab_cases_hr_ts_2.csv") %>%
    convert_hr_names()
  ab3 <- read_d("raw_data/static/ab/ab_cases_pt_ts.csv") %>%
    dplyr::filter(.data$date >= as.Date("2020-04-01"))
  # calculate time series for "unknown" health region based on diff between PT and agg HR
  ab4 <- ab2 %>%
    dplyr::select(-.data$sub_region_1) %>%
    dplyr::group_by(.data$name, .data$region, .data$date) %>%
    dplyr::summarize(value = sum(.data$value), .groups = "drop")
  ab5 <- ab3 %>%
    add_hr_col("Unknown")
  ab5$value <- ab5$value - ab4$value
  cases_ab <- dplyr::bind_rows(ab1, ab2, ab5)
  rm(ab1, ab2, ab3, ab4, ab5) # clean up

  ## bc
  bc1 <- read_d("raw_data/static/bc/bc_cases_hr_ts.csv") %>%
    drop_sub_regions("Out of Canada") %>%
    convert_hr_names()
  bc2 <- read_d("raw_data/reports/bc/bc_monthly_report.csv") %>%
    report_pluck("cases", "cases", "value_daily", "hr") %>%
    dplyr::filter(.data$date > as.Date("2023-04-15")) %>%
    convert_hr_names() %>%
    report_recent()
  cases_bc <- append_daily_d(bc1, bc2)
  rm(bc1, bc2) # cleanup

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
  nb2 <- read_d("raw_data/reports/nb/nb_weekly_report_3.csv") %>%
    report_pluck("cases", "cases", "value_daily", "hr") %>%
    convert_hr_names()
  cases_nb <- append_daily_d(cases_nb, nb1)
  cases_nb <- append_daily_d(cases_nb, nb2)
  rm(nb1, nb2) # cleanup

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
  ns4 <- read_d("raw_data/static/ns/ns_cases_pt_ts_1.csv") %>%
    dplyr::mutate(value_daily = c(dplyr::slice_head(., n = 1) %>% dplyr::pull(.data$value), diff(.data$value))) %>% # convert to daily
    dplyr::filter(.data$date >= as.Date("2022-03-05") & .data$date < as.Date("2022-07-01")) %>%
    add_hr_col("Unknown")
  cases_ns <- append_daily_d(cases_ns, ns4)
  ns5 <- read_d("raw_data/static/ns/ns_cases_pt_ts_2.csv") %>%
    dplyr::mutate(value_daily = c(dplyr::slice_head(., n = 1) %>% dplyr::pull(.data$value), diff(.data$value))) %>% # convert to daily
    dplyr::filter(.data$date >= as.Date("2022-07-01")) %>%
    add_hr_col("Unknown")
  cases_ns <- append_daily_d(cases_ns, ns5)
  ns6 <- read_d("raw_data/reports/ns/ns_monthly_report.csv") %>%
    report_pluck("cases", "cases", "value_daily", "pt") %>%
    add_hr_col("Unknown")
  cases_ns <- append_daily_d(cases_ns, ns6)
  rm(ns1, ns2, ns3, ns4, ns5, ns6) # cleanup

  ## nt
  cases_nt <- dplyr::bind_rows(
    get_phac_d("cases_daily", "NT", keep_up_to_date = TRUE) %>%
      add_hr_col("Northwest Territories") %>%
      dplyr::filter(.data$date <= as.Date("2022-06-08")),
    get_phac_d("cases", "NT", keep_up_to_date = TRUE) %>%
      add_hr_col("Northwest Territories") %>%
      dplyr::filter(.data$date == "2022-06-11"),
    read_d("raw_data/static/nt/nt_cases_pt_ts.csv") |>
      add_hr_col("Northwest Territories")
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
  on1 <- read_d("raw_data/static/on/on_cases_hr_ts.csv") |>
    convert_hr_names()
  on2 <- read_d("raw_data/reports/on/on_pho_cases.csv") |>
    report_pluck("cases", "cases_weekly", "value_daily", "hr") |>
    dplyr::filter(.data$date >= as.Date("2023-09-02")) |>
    convert_hr_names()
  cases_on <- append_daily_d(on1, on2)
  rm(on1, on2) # clean up

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
        dplyr::filter(.data$date > as.Date("2022-02-06")) %>% # overlaps with end of TS
        report_recent()
      sk3 <- read_d("raw_data/reports/sk/sk_monthly_report.csv") %>%
        report_pluck("cases", "cases", "value_daily", "hr")
      sk4 <- read_d("raw_data/reports/sk/sk_crisp_report.csv") %>%
        report_pluck("cases", "cases", "value_daily", "pt") %>%
        add_hr_col("Unknown") %>%
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
    read_d("raw_data/static/ab/ab_deaths_hr_ts.csv")
  )

  ## bc
  bc1 <- dplyr::bind_rows(
    get_ccodwg_d("deaths", "BC", to = "2022-04-01", drop_not_reported = TRUE) %>%
      convert_hr_names(),
    read_d("raw_data/static/bc/bc_deaths_hr_ts.csv")
  )
  bc2 <- read_d("raw_data/reports/bc/bc_monthly_report.csv") %>%
    report_pluck("deaths", "deaths", "value_daily", "hr") %>%
    dplyr::filter(.data$date > as.Date("2023-04-15")) %>%
    convert_hr_names() %>%
    report_recent()
  deaths_bc <- append_daily_d(bc1, bc2)
  rm(bc1, bc2) # cleanup

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
  nb2 <- read_d("raw_data/reports/nb/nb_weekly_report_3.csv") %>%
    report_pluck("deaths", "deaths", "value_daily", "pt") %>%
    add_hr_col("Unknown")
  deaths_nb <- append_daily_d(deaths_nb, nb1)
  deaths_nb <- append_daily_d(deaths_nb, nb2)
  rm(nb1, nb2) # cleanup

  ## nl
  deaths_nl <- dplyr::bind_rows(
    get_ccodwg_d("deaths", "NL", to = "2021-03-15", drop_not_reported = TRUE) %>%
      convert_hr_names(),
    read_d("raw_data/static/nl/nl_deaths_hr_ts_1.csv") %>%
      convert_hr_names(),
    read_d("raw_data/static/nl/nl_deaths_hr_ts_2.csv") %>%
      convert_hr_names(),
    read_d("raw_data/static/nl/nl_deaths_hr_ts_3.csv")
  )
  nl1 <- get_phac_d("deaths", "NL", keep_up_to_date = TRUE) %>%
    dplyr::filter(.data$date >= as.Date("2023-07-01")) %>%
    add_hr_col("Unknown")
  deaths_hr <- deaths_nl |>
    dplyr::filter(.data$date == max(.data$date)) |>
    dplyr::pull(.data$value) |>
    sum()
  nl1$value <- nl1$value - deaths_hr # subtract deaths assigned to a health region
  # started with 2023-07-01 instead of 2023-06-24 to avoid negative values for deaths
  deaths_nl <- dplyr::bind_rows(deaths_nl, nl1)
  rm(nl1, deaths_hr) # clean up

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
      ns6 <- read_d("raw_data/reports/ns/ns_monthly_report.csv") %>%
        report_pluck("deaths", "deaths", "value_daily", "pt") %>%
        add_hr_col("Unknown")
      deaths_ns <- append_daily_d(deaths_ns, ns6)
      rm(ns1, ns2, ns3, ns4, ns5, ns6) # cleanup
    },
    error = function(e) {
      print(e)
      cat("Error in processing pipeline", fill = TRUE)
    }
  )

  ## nt
  deaths_nt <- dplyr::bind_rows(
    get_phac_d("deaths_daily", "NT", keep_up_to_date = TRUE) %>%
      add_hr_col("Northwest Territories") %>%
      dplyr::filter(.data$date <= as.Date("2022-06-08")),
    get_phac_d("deaths", "NT", keep_up_to_date = TRUE) %>%
      add_hr_col("Northwest Territories") %>%
      dplyr::filter(.data$date == "2022-06-11"),
    read_d("raw_data/static/nt/nt_deaths_pt_ts.csv") |>
      add_hr_col("Northwest Territories")
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
  on1 <- read_d("raw_data/static/on/on_deaths_hr_ts.csv") |>
    convert_hr_names()
  on2 <- read_d("raw_data/reports/on/on_pho_outcomes.csv") |>
    dplyr::filter(.data$outcome_weekly_type == "COVID-19 deaths") |>
    report_pluck("deaths", "outcome_weekly_value", "value_daily", "hr") |>
    dplyr::filter(.data$date >= as.Date("2023-09-02")) |>
    convert_hr_names()
  deaths_on <- append_daily_d(on1, on2)
  rm(on1, on2) # clean up

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
        dplyr::filter(.data$date > as.Date("2022-02-06")) %>% # overlaps with end of TS
        report_recent()
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
  hospitalizations_ab <- dplyr::bind_rows(
    read_d("raw_data/static/ab/ab_hospitalizations_pt_ts_1.csv") |>
      dplyr::filter(.data$date <= as.Date("2022-01-31")),
    read_d("raw_data/static/ab/ab_hospitalizations_pt_ts_2.csv")
  )

  ## bc
  hospitalizations_bc <- dplyr::bind_rows(
    get_covid19tracker_d("hospitalizations", "BC") |>
      dplyr::filter(.data$date <= as.Date("2021-03-12")),
    read_d("raw_data/static/bc/bc_hospitalizations_hr_ts.csv") |>
      agg2pt(raw = TRUE)
  )

  ## mb
  hospitalizations_mb <- dplyr::bind_rows(
    get_covid19tracker_d("hospitalizations", "MB", to = "2021-02-03"),
    read_d("raw_data/static/mb/mb_hospitalizations_pt_ts.csv")
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
    read_d("raw_data/static/nl/nl_hospitalizations_pt_ts.csv")
  )

  ## ns
  hospitalizations_ns <- dplyr::bind_rows(
    get_covid19tracker_d("hospitalizations", "NS", to = "2021-01-18"),
    read_d("raw_data/static/ns/ns_hospitalizations_pt_ts_1.csv"),
    read_d("raw_data/static/ns/ns_hospitalizations_pt_ts_2.csv"),
    read_d("raw_data/reports/ns/ns_weekly_report.csv") |>
      report_pluck("hospitalizations", "active_hospitalizations", "value", "pt")
  )

  ## on
  hospitalizations_on <- read_d("raw_data/active_ts/on/on_hospitalizations_pt_ts.csv")

  ## pe
  hospitalizations_pe <- get_covid19tracker_d("hospitalizations", "PE")

  ## qc
  hospitalizations_qc <- dplyr::bind_rows(
    read_d("raw_data/static/qc/qc_hospitalizations_pt_ts.csv") |>
      dplyr::filter(.data$date <= as.Date("2020-04-09")),
    read_d("raw_data/active_ts/qc/qc_hospitalizations_pt_ts.csv")) |>
    date_shift(1) # shift both datasets

  ## sk
  hospitalizations_sk <- dplyr::bind_rows(
    read_d("raw_data/static/sk/sk_hospitalizations_pt_ts.csv"),
    read_d("raw_data/reports/sk/sk_weekly_report.csv") |>
      report_pluck("hospitalizations", "active_hospitalizations", "value", "pt") |>
      dplyr::filter(.data$date > as.Date("2022-02-06")) |> # overlaps with end of TS
      report_recent()
  )

  ## collate and process final dataset
  suppressWarnings(rm(hospitalizations_pt)) # if re-running manually
  hospitalizations_pt <- collate_datasets("hospitalizations") %>%
    dataset_format("pt")

  ## Canadian dataset (NOT an aggregate of PT datasets)
  hospitalizations_can <- get_phac_d("hospitalizations", "CAN") %>%
    dataset_format("pt")

  # icu dataset

  ## ab
  icu_ab <- dplyr::bind_rows(
    read_d("raw_data/static/ab/ab_icu_pt_ts_1.csv") |>
      dplyr::filter(.data$date <= as.Date("2022-01-31")),
    read_d("raw_data/static/ab/ab_icu_pt_ts_2.csv")
  )

  ## bc
  icu_bc <- dplyr::bind_rows(
    get_covid19tracker_d("icu", "BC") |>
      dplyr::filter(.data$date <= as.Date("2021-03-12")),
    read_d("raw_data/static/bc/bc_icu_hr_ts.csv") |>
      agg2pt(raw = TRUE)
  )

  ## mb
  icu_mb <- dplyr::bind_rows(
    get_covid19tracker_d("icu", "MB", to = "2021-02-03"),
    read_d("raw_data/static/mb/mb_icu_pt_ts.csv")
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
    read_d("raw_data/static/nl/nl_icu_pt_ts.csv")
  )

  ## ns
  icu_ns <- dplyr::bind_rows(
    get_covid19tracker_d("icu", "NS", to = "2021-01-18"),
    read_d("raw_data/static/ns/ns_icu_pt_ts_1.csv"),
    read_d("raw_data/static/ns/ns_icu_pt_ts_2.csv"),
    read_d("raw_data/reports/ns/ns_weekly_report.csv") |>
      report_pluck("icu", "active_icu", "value", "pt")
  )

  ## on
  icu_on <- read_d("raw_data/active_ts/on/on_icu_pt_ts.csv")

  ## pe
  icu_pe <- get_covid19tracker_d("icu", "PE")

  ## qc
  icu_qc <- dplyr::bind_rows(
    read_d("raw_data/static/qc/qc_icu_pt_ts.csv") |>
      dplyr::filter(.data$date <= as.Date("2020-04-09")),
    read_d("raw_data/active_ts/qc/qc_icu_pt_ts.csv")) |>
    date_shift(1) # shift both datasets

  ## sk
  icu_sk <- dplyr::bind_rows(
    read_d("raw_data/static/sk/sk_icu_pt_ts.csv"),
    read_d("raw_data/reports/sk/sk_weekly_report.csv") |>
      report_pluck("icu", "active_icu", "value", "pt") |>
      dplyr::filter(.data$date > as.Date("2022-02-06")) |> # overlaps with end of TS
      report_recent()
  )

  ## collate and process final dataset
  suppressWarnings(rm(icu_pt)) # if re-running manually
  icu_pt <- collate_datasets("icu") %>%
    dataset_format("pt")

  ## Canadian dataset (NOT an aggregate of PT datasets)
  icu_can <- get_phac_d("icu", "CAN") %>%
    dataset_format("pt")

  # hosp_admissions dataset

  ## mb
  mb1 <- read_d("raw_data/static/mb/mb_hosp_admissions_pt_ts.csv") # up to 2022-03-19
  mb2 <- read_d("raw_data/reports/mb/mb_weekly_report.csv") %>%
    report_pluck("hosp_admissions", "cumulative_hospitalizations", "value", "pt") # up to 2022-11-05
  mb3 <- read_d("raw_data/reports/mb/mb_weekly_report_2.csv") %>%
    report_pluck("hosp_admissions", "cumulative_hospitalizations_diff", "value_daily", "pt") # from 2022-11-06
  hosp_admissions_mb <- dplyr::bind_rows(mb1, mb2)
  hosp_admissions_mb <- append_daily_d(hosp_admissions_mb, mb3)
  rm(mb1, mb2, mb3) # clean up

  ## yt
  hosp_admissions_yt <- read_d("raw_data/static/yt/yt_hosp_admissions_pt_ts.csv")

  ## collate and process final dataset
  suppressWarnings(rm(hosp_admissions_pt)) # if re-running manually
  hosp_admissions_pt <- collate_datasets("hosp_admissions") %>%
    dataset_format("pt")

  ## no Canadian dataset

  # icu_admissions dataset

  ## mb
  mb1 <- read_d("raw_data/static/mb/mb_icu_admissions_pt_ts.csv") # up to 2022-03-19
  mb2 <- read_d("raw_data/reports/mb/mb_weekly_report.csv") %>%
    report_pluck("icu_admissions", "cumulative_icu", "value", "pt") # up to 2022-11-05
  mb3 <- read_d("raw_data/reports/mb/mb_weekly_report_2.csv") %>%
    report_pluck("icu_admissions", "cumulative_icu_diff", "value_daily", "pt") # from 2022-11-06
  icu_admissions_mb <- dplyr::bind_rows(mb1, mb2)
  icu_admissions_mb <- append_daily_d(icu_admissions_mb, mb3)
  rm(mb1, mb2, mb3) # clean up

  ## collate and process final dataset
  suppressWarnings(rm(icu_admissions_pt)) # if re-running manually
  icu_admissions_pt <- collate_datasets("icu_admissions") %>%
    dataset_format("pt")

  ## no Canadian dataset

  # tests_completed dataset

  ## all regions (up to 2022-11-12 or earlier, depending on PT)
  tests_completed_pt <- get_phac_d("tests_completed", "all", keep_up_to_date = TRUE)

  ## remove regions: AB, MB, YT
  tests_completed_pt <- tests_completed_pt %>%
    dplyr::filter(!.data$region %in% c("AB", "MB", "YT"))

  ## add AB and YT data
  tests_completed_pt <- dplyr::bind_rows(
    tests_completed_pt,
    read_d("raw_data/static/ab/ab_tests_completed_pt_ts_1.csv") |>
      dplyr::filter(.data$date <= as.Date("2020-03-05")),
    read_d("raw_data/static/ab/ab_tests_completed_pt_ts_2.csv"),
    read_d("raw_data/static/yt/yt_tests_completed_pt_ts.csv")
  )

  ## add MB data
  mb1 <- get_phac_d("tests_completed", "MB", keep_up_to_date = TRUE)
  mb2 <- read_d("raw_data/reports/mb/mb_weekly_report_2.csv") %>%
    report_pluck("tests_completed", "tests_completed", "value_daily", "pt") %>%
    dplyr::filter(.data$date >= as.Date("2022-11-26"))
  mb3 <- append_daily_d(mb1, mb2)
  # there is technically a 1-day overlap between PHAC data ending 2022-11-20
  # and weekly MB report data ending 2022-11-26
  tests_completed_pt <- dplyr::bind_rows(
    tests_completed_pt,
    mb3
  )
  rm(mb1, mb2, mb3) # cleanup

  ## add ON data (2022-11-26 and later)
  on1 <- tests_completed_pt |>
    dplyr::filter(.data$region == "ON" & .data$date <= as.Date("2022-11-19")) # avoid overlap with weekly data from 2022-11-20 to 2022-11-26
  tests_completed_pt <- tests_completed_pt |>
    dplyr::filter(.data$region != "ON") # remove ON from main dataset
  on2 <- read_d("raw_data/reports/on/on_pho_testing.csv") |>
    report_pluck("tests_completed", "tests_completed_weekly", "value_daily", "pt") |>
    dplyr::filter(.data$date >= as.Date("2022-11-26"))
  on3 <- append_daily_d(on1, on2)
  # add ON back to main dataset
  tests_completed_pt <- dplyr::bind_rows(tests_completed_pt, on3)
  rm(on1, on2, on3) # clean up

  ## collate and process final dataset
  tests_completed_pt <- tests_completed_pt %>%
    dataset_format("pt")

  # vaccine_coverage dataset

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

  # vaccine_administration dataset

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

  # vaccine_distribution dataset

  ## collate and process final datasets
  vaccine_distribution_total_doses_pt <- dplyr::bind_rows(
    read_d("raw_data/ccodwg/can_vaccine_distribution_pt_ts.csv") |>
      dplyr::mutate(name = "vaccine_distribution_total_doses") |>
      dplyr::filter(.data$date <= as.Date("2021-01-01")),
    get_phac_d("vaccine_distribution_total_doses", "all") |>
      dplyr::filter(.data$region != "Federal allocation")) |>
    dataset_format("pt")

  ## Canadian dataset (NOT an aggregate of PT dataset)
  vaccine_distribution_total_doses_can <- dplyr::bind_rows(
    read_d("raw_data/ccodwg/can_vaccine_distribution_pt_ts.csv") |>
      dplyr::mutate(name = "vaccine_distribution_total_doses") |>
      dplyr::filter(.data$date <= as.Date("2021-01-01")) |>
      dplyr::mutate(region = "CAN") |>
      dplyr::group_by(.data$name, .data$region, .data$date) |>
      dplyr::summarize(value = sum(.data$value), .groups = "drop"),
    get_phac_d("vaccine_distribution_total_doses", "CAN")) |>
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
  tests_completed_can_completeness <- agg2can_completeness(tests_completed_pt)

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
  write_dataset(hosp_admissions_pt, "pt", "hosp_admissions_pt")
  # write_dataset(hosp_admissions_can, "can", "hosp_admissions_can")
  write_dataset(icu_admissions_pt, "pt", "icu_admissions_pt")
  # write_dataset(icu_admissions_can, "can", "icu_admissions_can")
  write_dataset(tests_completed_pt, "pt", "tests_completed_pt")
  write_dataset(tests_completed_can, "can", "tests_completed_can")
  write_dataset(tests_completed_can_completeness, "can", "tests_completed_can_completeness", ext = "json")
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
  write_dataset(vaccine_distribution_total_doses_pt, "pt", "vaccine_distribution_total_doses")
  write_dataset(vaccine_administration_total_doses_can, "can", "vaccine_distribution_total_doses")
}
