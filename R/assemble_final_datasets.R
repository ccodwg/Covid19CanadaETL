#' Assemble and write final datasets from raw datasets for CovidTimelineCanada
#'
#' @importFrom rlang .data
#' @importFrom rlang .env
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
  ab2 <- read_d("raw_data/static/ab/ab_cases_hr_ts_2.csv") %>% # begins 2020-04-01
    convert_hr_names()
  ab3 <- read_d("raw_data/static/ab/ab_cases_pt_ts.csv") %>%
    dplyr::filter(.data$date >= as.Date("2020-04-01"))
  # calculate time series for "unknown" health region based on diff between PT and agg HR
  ab4 <- ab2 %>%
    dplyr::select(-.data$sub_region_1) %>%
    dplyr::group_by(.data$name, .data$region, .data$date) %>%
    dplyr::summarize(value = sum(.data$value), .groups = "drop")
  ab5 <- ab3 %>%
    add_hr_col("9999")
  ab5$value <- ab5$value - ab4$value
  cases_ab <- dplyr::bind_rows(ab1, ab2, ab5)
  # 2023-09-02 and later
  ab6 <- read_d("raw_data/active_ts/ab/ab_cases_hr_ts.csv") |>
    dplyr::filter(.data$date >= as.Date("2023-09-02")) |>
    convert_hr_names()
  ab6_pt <- ab6 |>
    agg2pt(raw = TRUE)
  ab7 <- read_d("raw_data/active_ts/ab/ab_cases_pt_ts.csv")
  ab7$value_daily <- ab7$value_daily - ab6_pt$value_daily
  ab7 <- ab7 |>
    add_hr_col("9999")
  ab8 <- dplyr::bind_rows(ab6, ab7)
  cases_ab <- append_daily_d(cases_ab, ab8)
  rm(ab1, ab2, ab3, ab4, ab5, ab6, ab6_pt, ab7, ab8) # clean up
  # trim to max date
  cases_ab <- max_date(cases_ab, "2023-12-30")

  ## bc
  cases_bc  <- read_d("raw_data/reports/bc/bc_monthly_report_cumulative.csv") |>
    report_pluck("cases", "cases", "value", "hr") |>
    dplyr::mutate(sub_region_1 = ifelse(.data$sub_region_1 == "Out of Canada", "Unknown", .data$sub_region_1)) |>
    convert_hr_names() |>
    dplyr::filter(.data$date >= as.Date("2020-01-29")) # first case

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
  # trim to max date
  cases_nb <- max_date(cases_nb, "2023-12-30")

  ## nl
  cases_nl <- dplyr::bind_rows(
    get_ccodwg_d("cases", "NL", to = "2021-03-15", drop_not_reported = TRUE) %>%
      convert_hr_names(),
    read_d("raw_data/static/nl/nl_cases_hr_ts.csv") %>%
      convert_hr_names(),
    read_d("raw_data/static/nl/nl_cases_pt_ts.csv") %>%
      dplyr::filter(.data$date >= as.Date("2022-03-12")) %>%
      dplyr::transmute(
        name = .data$name,
        region = .data$region,
        sub_region_1 = "Unknown",
        date = .data$date,
        value = cumsum(.data$value_daily))
  ) %>%
    convert_hr_names()
  cases_nl <- append_daily_d(
    cases_nl,
    read_d("raw_data/active_ts/nl/nl_cases_pt_ts.csv") |>
      dplyr::filter(.data$date >= as.Date("2023-10-28")) |>
      add_hr_col("Unknown") |>
      convert_hr_names()
    )
  # trim to max date
  cases_nl <- max_date(cases_nl, "2023-12-30")

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
  # slight overlap between end of last report (2023-08-31) and beginning of this report (2023-08-27)
  ns7 <- read_d("raw_data/reports/ns/ns_respiratory_watch_report.csv") |>
    report_pluck("cases", "cases", "value_daily", "pt") |>
    add_hr_col("Unknown")
  cases_ns <- append_daily_d(cases_ns, ns7)
  rm(ns1, ns2, ns3, ns4, ns5, ns6, ns7) # cleanup

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
  cases_on <- read_d("raw_data/active_ts/on/on_cases_hr_ts.csv") |>
    convert_hr_names()

  ## pe
  cases_pe <- dplyr::bind_rows(
    get_phac_d("cases_daily", "PE", keep_up_to_date = TRUE) %>%
      add_hr_col("Prince Edward Island"),
    get_phac_d("cases", "PE", keep_up_to_date = TRUE) %>%
      add_hr_col("Prince Edward Island") %>%
      dplyr::filter(.data$date >= as.Date("2022-06-11"))
  )
  # trim to max date
  cases_pe <- max_date(cases_pe, "2023-12-30")

  ## qc
  tryCatch(
    {
      cases_qc <- read_d("raw_data/active_ts/qc/qc_cases_hr_ts.csv") %>%
        dplyr::mutate(
          sub_region_1 = ifelse(.data$sub_region_1 %in% c("Inconnu", "Hors Qu\u00E9bec"),
                                "Unknown", .data$sub_region_1)) %>%
        dplyr::group_by(.data$name, .data$region, .data$sub_region_1, .data$date) %>%
        dplyr::summarize(value = sum(.data$value), .groups = "drop") %>%
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
      sk3 <- get_phac_d("cases", "SK", keep_up_to_date = TRUE) %>%
        dplyr::mutate(value_daily = c(dplyr::slice_head(., n = 1) %>% dplyr::pull(.data$value), diff(.data$value))) |>
        dplyr::filter(.data$date >= as.Date("2022-07-02")) |>
        add_hr_col("Unknown")
      cases_sk <- append_daily_d(sk1, sk2) %>%
        dplyr::mutate(
          sub_region_1 = ifelse(.data$sub_region_1 == "Not Assigned", "Unknown", .data$sub_region_1))
      cases_sk <- append_daily_d(cases_sk, sk3)
      rm(sk1, sk2, sk3) # cleanup
      # trim to max date
      cases_sk <- max_date(cases_sk, "2023-12-30")
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
  ab1 <- get_ccodwg_d("deaths", "AB", to = "2020-06-22", drop_not_reported = FALSE) %>%
    convert_hr_names()
  ab2 <- dplyr::bind_rows(
    read_d("raw_data/static/ab/ab_deaths_hr_ts_1.csv") |> # 2020-06-23 to 2021-11-17
      dplyr::filter(.data$date <= as.Date("2021-11-17")) |>
      convert_hr_names(),
    read_d("raw_data/static/ab/ab_deaths_hr_ts_2.csv") |> # 2021-11-18 to 2023-07-24
      dplyr::filter(!is.na(.data$sub_region_1)) |>
      convert_hr_names()
  )
  ab2_max <- ab2 |>
    dplyr::filter(.data$date == as.Date("2023-07-24") & .data$sub_region_1 != "9999") |>
    dplyr::pull(.data$value) |>
    sum() # deaths on 2023-07-24, excluding unknown
  ab3 <- read_d("raw_data/static/ab/ab_deaths_pt_ts.csv") |> # ends 2023-08-28
    dplyr::filter(.data$date  >= as.Date("2023-07-25")) |>
    dplyr::mutate(value = .data$value - ab2_max) |>
    add_hr_col("9999") |> # add unknown column
    dplyr::filter(.data$value >= 0) # exclude negative values
  deaths_ab <- dplyr::bind_rows(ab1, ab2, ab3)
  ab_max <- deaths_ab |>
    dplyr::group_by(.data$name, .data$region, .data$sub_region_1) |>
    dplyr::filter(.data$date == max(.data$date)) |>
    dplyr::ungroup() |>
    dplyr::select("sub_region_1", "value2" = "value")
  # 2023-09-02 and later
  ab4 <- read_d("raw_data/active_cumul/ab/ab_deaths_hr_ab_2_ts.csv") |>
    dplyr::left_join(ab_max, by = c("sub_region_1")) |>
    dplyr::transmute(
      .data$name, .data$region, .data$sub_region_1, .data$date, value = .data$value + .data$value2)
  deaths_ab <- dplyr::bind_rows(deaths_ab, ab4)
  rm(ab1, ab2, ab2_max, ab3, ab_max, ab4) # clean up
  # trim to max date
  deaths_ab <- max_date(deaths_ab, "2023-12-30")

  ## bc
  deaths_bc  <- read_d("raw_data/reports/bc/bc_monthly_report_cumulative.csv") |>
    report_pluck("deaths", "deaths", "value", "hr") |>
    dplyr::mutate(sub_region_1 = ifelse(.data$sub_region_1 == "Out of Canada", "Unknown", .data$sub_region_1)) |>
    convert_hr_names() |>
    dplyr::filter(.data$date >= as.Date("2020-01-29")) # first case

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
  nl1 <- read_d("raw_data/reports/nl/nl_monthly_report.csv") %>%
    report_pluck("deaths", "deaths", "value_daily", "hr") %>%
    convert_hr_names()
  deaths_nl <- append_daily_d(deaths_nl, nl1)
  nl2 <- read_d("raw_data/reports/nl/nl_respiratory_report.csv") |>
    report_pluck("deaths", "deaths", "value_daily", "pt") |>
    dplyr::transmute(
      .data$name, .data$region, .data$date, value = cumsum(.data$value_daily)) |>
    add_hr_col("Unknown") |>
    convert_hr_names()
  deaths_nl <- dplyr::bind_rows(deaths_nl, nl2)
  rm(nl1, nl2) # clean up

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
      # slight overlap between end of last report (2023-08-31) and beginning of this report (2023-08-27)
      ns7 <- read_d("raw_data/reports/ns/ns_respiratory_watch_report.csv") |>
        report_pluck("deaths", "deaths", "value_daily", "pt") |>
        add_hr_col("Unknown")
      deaths_ns <- append_daily_d(deaths_ns, ns7)
      rm(ns1, ns2, ns3, ns4, ns5, ns6, ns7) # cleanup
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
  # trim to max date
  deaths_on <- max_date(deaths_on, "2023-12-30")

  ## pe
  deaths_pe <- dplyr::bind_rows(
    get_phac_d("deaths_daily", "PE", keep_up_to_date = TRUE) %>%
      add_hr_col("Prince Edward Island"),
    get_phac_d("deaths", "PE", keep_up_to_date = TRUE) %>%
      add_hr_col("Prince Edward Island") %>%
      dplyr::filter(.data$date >= as.Date("2022-06-11"))
  )
  # trim to max date
  deaths_pe <- max_date(deaths_pe, "2023-12-30")

  ## qc
  tryCatch(
    {
      deaths_qc <- read_d("raw_data/active_ts/qc/qc_deaths_hr_ts.csv") %>%
        dplyr::mutate(
          sub_region_1 = ifelse(.data$sub_region_1 %in% c("Inconnu", "Hors Qu\u00E9bec"),
                                "Unknown", .data$sub_region_1)) %>%
        dplyr::group_by(.data$name, .data$region, .data$sub_region_1, .data$date) %>%
        dplyr::summarize(value = sum(.data$value), .groups = "drop") %>%
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
      deaths_sk <- append_daily_d(sk1, sk2) %>%
        dplyr::mutate(
          sub_region_1 = ifelse(.data$sub_region_1 == "Not Assigned", "Unknown", .data$sub_region_1))
      sk_sum <- deaths_sk |>
        dplyr::filter(.data$sub_region_1 != "Unknown") |>
        dplyr::group_by(.data$sub_region_1) |>
        dplyr::filter(.data$date == max(.data$date)) |>
        dplyr::pull(.data$value) |>
        sum()
      sk3 <- get_phac_d("deaths", "SK", keep_up_to_date = TRUE) |>
        dplyr::filter(.data$date >= as.Date("2022-07-02")) |>
        dplyr::mutate(value = .data$value - sk_sum) |>
        add_hr_col("Unknown")
      deaths_sk <- dplyr::bind_rows(deaths_sk, sk3)
      rm(sk1, sk2, sk_sum, sk3) # cleanup
    },
    error = function(e) {
      print(e)
      cat("Error in processing pipeline", fill = TRUE)
    }
  )
  # trim to max date
  deaths_sk <- max_date(deaths_sk, "2023-12-30")

  ## yt
  deaths_yt <- dplyr::bind_rows(
    get_phac_d("deaths_daily", "YT", keep_up_to_date = TRUE) %>%
      add_hr_col("Yukon") %>%
      dplyr::filter(.data$date <= as.Date("2022-02-17")),
    read_d("raw_data/static/yt/yt_deaths_pt_ts.csv") |>
      add_hr_col("Yukon")
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
    read_d("raw_data/reports/bc/bc_daily_news_release.csv") |>
      report_pluck("hospitalizations", "active_hospitalizations", "value", "pt") |>
      dplyr::filter(.data$date <= as.Date("2020-03-12")),
    get_covid19tracker_d("hospitalizations", "BC", from = "2020-03-16", to = "2021-03-12"),
    read_d("raw_data/static/bc/bc_hospitalizations_hr_ts.csv") |>
      agg2pt(raw = TRUE),
    read_d("raw_data/reports/bc/bc_monthly_report.csv") |>
      report_pluck("hospitalizations", "active_hospitalizations", "value", "pt")
  )
  # remove seemingly erroneous data (unexplained spikes)
  hospitalizations_bc <- hospitalizations_bc[
    !hospitalizations_bc$date %in% as.Date(c("2021-04-01", "2021-05-05", "2022-04-28", "2022-08-18")), ]
  # trim to max date
  hospitalizations_bc <- max_date(hospitalizations_bc, "2023-12-21")

  ## mb
  hospitalizations_mb <- dplyr::bind_rows(
    read_d("raw_data/static/mb/mb_hospitalizations_pt_ts_1.csv") |>
      dplyr::filter(.data$date <= as.Date("2021-02-03")),
    read_d("raw_data/static/mb/mb_hospitalizations_pt_ts_2.csv")
  )

  ## nb
  hospitalizations_nb <- dplyr::bind_rows(
    read_d("raw_data/reports/nb/nb_daily_news_release.csv") |>
      report_pluck("hospitalizations", "active_hospitalizations", "value", "pt") |>
      dplyr::filter(.data$date <= as.Date("2020-06-29")),
    get_covid19tracker_d("hospitalizations", "NB", from = "2020-06-30", to = "2021-03-07"),
    read_d("raw_data/static/nb/nb_hospitalizations_pt_ts_1.csv"),
    read_d("raw_data/static/nb/nb_hospitalizations_pt_ts_2.csv") |>
      dplyr::filter(.data$date <= as.Date("2022-01-20")),
    read_d("raw_data/static/nb/nb_hospitalizations_pt_ts_3.csv"),
    read_d("raw_data/reports/nb/nb_weekly_report.csv") %>%
      report_pluck("hospitalizations", "active_hospitalizations", "value", "pt")
  )

  ## nl
  hospitalizations_nl <- dplyr::bind_rows(
    read_d("raw_data/static/nl/nl_hospitalizations_pt_ts.csv")
  )

  ## ns
  hospitalizations_ns <- dplyr::bind_rows(
    read_d("raw_data/reports/ns/ns_daily_news_release.csv") |>
      report_pluck("hospitalizations", "active_hospitalizations", "value", "pt") |>
      dplyr::filter(.data$date <= as.Date("2021-01-18")),
    read_d("raw_data/static/ns/ns_hospitalizations_pt_ts_1.csv"),
    read_d("raw_data/static/ns/ns_hospitalizations_pt_ts_2.csv"),
    read_d("raw_data/reports/ns/ns_weekly_report.csv") |>
      report_pluck("hospitalizations", "active_hospitalizations", "value", "pt")
  )

  ## on
  hospitalizations_on <- read_d("raw_data/active_ts/on/on_hospitalizations_pt_ts.csv")

  ## pe
  hospitalizations_pe <- read_d("raw_data/reports/pe/pe_daily_news_release.csv") |>
    report_pluck("hospitalizations", "active_hospitalizations", "value", "pt") |>
    # exclude dates with no data
    dplyr::filter(!(.data$date %in% as.Date(c("2022-02-05", "2022-02-19", "2022-02-20", "2022-02-21", "2022-02-23")))) |>
    # add 0 for period between 2021-04-19 and 2021-12-28 when no new hosp admissions were reported
    dplyr::filter(.data$date < as.Date("2021-04-19") | .data$date > as.Date("2021-12-28")) |>
    dplyr::bind_rows(
      data.frame(
        name = "hospitalizations",
        region = "PE",
        date = seq.Date(from = as.Date("2021-04-19"), to = as.Date("2021-12-28"), by = "day"),
        value = 0
      )
    )

  ## qc
  hospitalizations_qc <- dplyr::bind_rows(
    read_d("raw_data/static/qc/qc_hospitalizations_pt_ts_1.csv") |>
      dplyr::filter(.data$date <= as.Date("2020-04-09")),
    read_d("raw_data/static/qc/qc_hospitalizations_pt_ts_2.csv")) |>
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
    # daily news release has an ICU case on 2020-03-10, but this value is not regularly reported until later
    # so it is excluded here
    get_covid19tracker_d("icu", "BC", from = "2020-03-17", to = "2021-03-12"),
    read_d("raw_data/static/bc/bc_icu_hr_ts.csv") |>
      agg2pt(raw = TRUE),
    read_d("raw_data/reports/bc/bc_monthly_report.csv") |>
      report_pluck("icu", "active_icu", "value", "pt")
  )
  # remove seemingly erroneous data (unexplained spikes)
  icu_bc <- icu_bc[
    !icu_bc$date %in% as.Date(c("2021-04-01", "2021-05-05", "2022-04-28", "2022-08-18")), ]
  # trim to max date
  icu_bc <- max_date(icu_bc, "2023-12-21")

  ## mb
  icu_mb <- dplyr::bind_rows(
    read_d("raw_data/static/mb/mb_icu_pt_ts_1.csv") |>
      dplyr::filter(.data$date <= as.Date("2021-02-03")),
    read_d("raw_data/static/mb/mb_icu_pt_ts_2.csv")
  )

  ## nb
  icu_nb <- dplyr::bind_rows(
    read_d("raw_data/reports/nb/nb_daily_news_release.csv") |>
      report_pluck("icu", "active_icu", "value", "pt") |>
      dplyr::filter(.data$date <= as.Date("2020-06-29")),
    get_covid19tracker_d("icu", "NB", from = "2020-06-30", to = "2021-03-07"),
    read_d("raw_data/static/nb/nb_icu_pt_ts_1.csv"),
    read_d("raw_data/static/nb/nb_icu_pt_ts_2.csv") |>
      dplyr::filter(.data$date <= as.Date("2022-01-20")),
    read_d("raw_data/static/nb/nb_icu_pt_ts_3.csv"),
    read_d("raw_data/reports/nb/nb_weekly_report.csv") %>%
      report_pluck("icu", "active_icu", "value", "pt")
  )

  ## nl
  icu_nl <- dplyr::bind_rows(
    get_covid19tracker_d("icu", "NL", from = "2020-03-29", to = "2021-03-15"),
    read_d("raw_data/static/nl/nl_icu_pt_ts.csv")
  )

  ## ns
  icu_ns <- dplyr::bind_rows(
    read_d("raw_data/reports/ns/ns_daily_news_release.csv") |>
      report_pluck("icu", "active_icu", "value", "pt") |>
      dplyr::filter(.data$date <= as.Date("2021-01-18")),
    read_d("raw_data/static/ns/ns_icu_pt_ts_1.csv"),
    read_d("raw_data/static/ns/ns_icu_pt_ts_2.csv"),
    read_d("raw_data/reports/ns/ns_weekly_report.csv") |>
      report_pluck("icu", "active_icu", "value", "pt")
  )

  ## on
  icu_on <- read_d("raw_data/active_ts/on/on_icu_pt_ts.csv")

  ## pe
  icu_pe <- read_d("raw_data/reports/pe/pe_daily_news_release.csv") |>
    # exclude dates with no data
    dplyr::filter(!(.data$date %in% as.Date(c("2022-02-05", "2022-02-19", "2022-02-20", "2022-02-21", "2022-02-23")))) |>
    # handle implicit zeroes
    dplyr::transmute(
      name = "icu",
      region = "PE",
      date = .data$date,
      value = ifelse(is.na(.data$active_icu), 0, .data$active_icu) # assume if ICU is not mentioned, it is zero
    ) |>
    # add 0 for period between 2021-04-19 and 2021-12-28 when no new hosp admissions were reported
    dplyr::filter(.data$date < as.Date("2021-04-19") | .data$date > as.Date("2021-12-28")) |>
    dplyr::bind_rows(
      data.frame(
        name = "icu",
        region = "PE",
        date = seq.Date(from = as.Date("2021-04-19"), to = as.Date("2021-12-28"), by = "day"),
        value = 0
      )
    )

  ## qc
  icu_qc <- dplyr::bind_rows(
    read_d("raw_data/static/qc/qc_icu_pt_ts_1.csv") |>
      dplyr::filter(.data$date <= as.Date("2020-04-09")),
    read_d("raw_data/static/qc/qc_icu_pt_ts_2.csv")) |>
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

  ## ab
  hosp_admissions_ab <- read_d("raw_data/active_ts/ab/ab_hosp_admissions_pt_ts.csv") |>
    dplyr::transmute(
      .data$name,
      .data$region,
      .data$date,
      value = cumsum(.data$value_daily))
  # trim to max date
  hosp_admissions_ab <- max_date(hosp_admissions_ab, "2023-12-30")

  ## bc
  hosp_admissions_bc  <- read_d("raw_data/reports/bc/bc_monthly_report_cumulative.csv") |>
    report_pluck("hosp_admissions", "hosp_admissions", "value", "pt") |>
    dplyr::filter(.data$date >= as.Date("2020-01-03")) # first admission

  ## mb
  mb1 <- read_d("raw_data/static/mb/mb_hosp_admissions_pt_ts.csv") # up to 2022-03-19
  mb2 <- read_d("raw_data/reports/mb/mb_weekly_report.csv") %>%
    report_pluck("hosp_admissions", "cumulative_hospitalizations", "value", "pt") # up to 2022-11-05
  mb3 <- read_d("raw_data/reports/mb/mb_weekly_report_2.csv") %>%
    report_pluck("hosp_admissions", "cumulative_hospitalizations_diff", "value_daily", "pt") # from 2022-11-06
  hosp_admissions_mb <- dplyr::bind_rows(mb1, mb2)
  hosp_admissions_mb <- append_daily_d(hosp_admissions_mb, mb3)
  rm(mb1, mb2, mb3) # clean up

  ## nb
  hosp_admissions_nb <- read_d("raw_data/reports/nb/nb_weekly_report.csv") |>
    report_pluck("hosp_admissions", "cumulative_hosp_admissions", "value", "pt")
  hosp_admissions_nb <- append_daily_d(
    hosp_admissions_nb,
    read_d("raw_data/reports/nb/nb_weekly_report_2.csv") |>
      report_pluck("hosp_admissions", "new_hospitalizations", "value_daily", "pt")
  )
  hosp_admissions_nb <- append_daily_d(
    hosp_admissions_nb,
    read_d("raw_data/reports/nb/nb_weekly_report_3.csv") |>
      report_pluck("hosp_admissions", "new_hospitalizations", "value_daily", "pt")
  )

  ## ns
  ns1 <- read_d("raw_data/static/ns/ns_hosp_admissions_pt_ts.csv") |>
    dplyr::mutate(value = cumsum(.data$value_daily)) |>
    dplyr::select(-.data$value_daily) |>
    # overlaps with beginning of report
    dplyr::filter(date <= as.Date("2022-05-16"))
  ns2 <- read_d("raw_data/reports/ns/ns_weekly_report.csv") |>
    report_pluck("hosp_admissions", "new_hospitalizations", "value_daily", "pt") |>
    dplyr::filter(date >= as.Date("2022-05-23"))
  ns3 <- read_d("raw_data/reports/ns/ns_monthly_report.csv") |>
    report_pluck("hosp_admissions", "new_hospitalizations", "value_daily", "pt")
  hosp_admissions_ns <- append_daily_d(ns1, ns2)
  hosp_admissions_ns <- append_daily_d(hosp_admissions_ns, ns3)
  # slight overlap between end of last report (2023-08-31) and beginning of this report (2023-08-27)
  ns4 <- read_d("raw_data/reports/ns/ns_respiratory_watch_report.csv") |>
    report_pluck("hosp_admissions", "hosp_admissions", "value_daily", "pt")
  hosp_admissions_ns <- append_daily_d(hosp_admissions_ns, ns4)
  rm(ns1, ns2, ns3, ns4) # clean up

  ## nt
  hosp_admissions_nt <- read_d("raw_data/static/nt/nt_hosp_admissions_pt_ts.csv")

  ## on
  hosp_admissions_on <- read_d("raw_data/reports/on/on_pho_outcomes.csv") |>
    dplyr::filter(.data$outcome_weekly_type == "COVID-19 hospitalizations" & is.na(.data$sub_region_1)) |>
    report_pluck("hosp_admissions", "outcome_weekly_value", "value_daily", "pt") |>
    dplyr::transmute(
      .data$name,
      .data$region,
      .data$date,
      value = cumsum(.data$value_daily))
  # trim to max date
  hosp_admissions_on <- max_date(hosp_admissions_on, "2023-12-30")

  ## pe
  hosp_admissions_pe <- dplyr::bind_rows(
    read_d("raw_data/reports/pe/pe_daily_news_release.csv") |>
      report_pluck("hosp_admissions", "new_hospitalizations", "value_daily", "pt") |>
      dplyr::transmute(.data$name, .data$region, .data$date, value = cumsum(.data$value_daily)),
    read_d("raw_data/static/pe/pe_hosp_admissions_pt_ts.csv"),
    read_d("raw_data/reports/pe/pe_respiratory_illness_report.csv") |>
      report_pluck("hosp_admissions", "cumulative_hosp_admissions", "value", "pt")
  )

  ## sk
  hosp_admissions_sk <- append_daily_d(
    read_d("raw_data/reports/sk/sk_monthly_report.csv") |>
      report_pluck("hosp_admissions", "new_hospitalizations", "value_daily", "pt") |>
      report_recent() |>
      dplyr::transmute(
        .data$name,
        .data$region,
        .data$date,
        value = cumsum(.data$value_daily)),
    read_d("raw_data/reports/sk/sk_crisp_report.csv") |>
      report_pluck("hosp_admissions", "new_hospitalizations", "value_daily", "pt") |>
      report_recent()
    )
  # trim to max date
  hosp_admissions_sk <- max_date(hosp_admissions_sk, "2023-12-30")

  ## qc
  hosp_admissions_qc <- read_d("raw_data/static/qc/qc_hosp_admissions_pt_ts.csv") |>
    date_shift(1)

  ## yt
  hosp_admissions_yt <- read_d("raw_data/static/yt/yt_hosp_admissions_pt_ts.csv")

  ## collate and process final dataset
  suppressWarnings(rm(hosp_admissions_pt)) # if re-running manually
  hosp_admissions_pt <- collate_datasets("hosp_admissions") %>%
    dataset_format("pt")

  ## censor daily value for first date of several PTs: MB, NB, NT
  ## cumulative values are given but time series does not start at the beginning
  hosp_admissions_pt[
    hosp_admissions_pt$region == "MB" & hosp_admissions_pt$date == as.Date("2020-05-16"), "value_daily"] <- NA
  hosp_admissions_pt[
    hosp_admissions_pt$region == "NB" & hosp_admissions_pt$date == as.Date("2022-04-02"), "value_daily"] <- NA
  hosp_admissions_pt[
    hosp_admissions_pt$region == "NT" & hosp_admissions_pt$date == as.Date("2021-08-25"), "value_daily"] <- NA

  ## no Canadian dataset

  # icu_admissions dataset

  ## ab
  icu_admissions_ab <- read_d("raw_data/active_ts/ab/ab_icu_admissions_pt_ts.csv") |>
    dplyr::transmute(
      .data$name,
      .data$region,
      .data$date,
      value = cumsum(.data$value_daily))
  # trim to max date
  icu_admissions_ab <- max_date(icu_admissions_ab, "2023-12-30")

  ## bc
  icu_admissions_bc  <- read_d("raw_data/reports/bc/bc_monthly_report_cumulative.csv") |>
    report_pluck("icu_admissions", "icu_admissions", "value", "pt") |>
    dplyr::filter(.data$date >= as.Date("2020-01-03")) # first admission

  ## mb
  mb1 <- read_d("raw_data/static/mb/mb_icu_admissions_pt_ts.csv") # up to 2022-03-19
  mb2 <- read_d("raw_data/reports/mb/mb_weekly_report.csv") %>%
    report_pluck("icu_admissions", "cumulative_icu", "value", "pt") # up to 2022-11-05
  mb3 <- read_d("raw_data/reports/mb/mb_weekly_report_2.csv") %>%
    report_pluck("icu_admissions", "cumulative_icu_diff", "value_daily", "pt") # from 2022-11-06
  icu_admissions_mb <- dplyr::bind_rows(mb1, mb2)
  icu_admissions_mb <- append_daily_d(icu_admissions_mb, mb3)
  rm(mb1, mb2, mb3) # clean up

  ## nb
  icu_admissions_nb <- read_d("raw_data/reports/nb/nb_weekly_report.csv") |>
    report_pluck("icu_admissions", "cumulative_icu_admissions", "value", "pt")
  icu_admissions_nb <- append_daily_d(
    icu_admissions_nb,
    read_d("raw_data/reports/nb/nb_weekly_report_2.csv") |>
      report_pluck("icu_admissions", "new_icu", "value_daily", "pt")
  )
  icu_admissions_nb <- append_daily_d(
    icu_admissions_nb,
    read_d("raw_data/reports/nb/nb_weekly_report_3.csv") |>
      report_pluck("icu_admissions", "new_icu", "value_daily", "pt")
  )

  ## nt
  icu_admissions_nt <- read_d("raw_data/static/nt/nt_icu_admissions_pt_ts.csv")

  ## pe
  icu_admissions_pe <- dplyr::bind_rows(
    read_d("raw_data/reports/pe/pe_daily_news_release.csv") |>
      report_pluck("icu_admissions", "new_icu", "value_daily", "pt") |>
      dplyr::transmute(.data$name, .data$region, .data$date, value = cumsum(.data$value_daily)),
    read_d("raw_data/static/pe/pe_icu_admissions_pt_ts.csv"),
    read_d("raw_data/reports/pe/pe_respiratory_illness_report.csv") |>
      report_pluck("icu_admissions", "cumulative_icu_admissions", "value", "pt")
  )

  ## qc
  icu_admissions_qc <- read_d("raw_data/static/qc/qc_icu_admissions_pt_ts.csv") |>
    date_shift(1)

  ## sk
  icu_admissions_sk <- append_daily_d(
    read_d("raw_data/reports/sk/sk_monthly_report.csv") |>
      report_pluck("icu_admissions", "new_icu", "value_daily", "pt") |>
      report_recent() |>
      dplyr::transmute(
        .data$name,
        .data$region,
        .data$date,
        value = cumsum(.data$value_daily)),
    read_d("raw_data/reports/sk/sk_crisp_report.csv") |>
      report_pluck("icu_admissions", "new_icu", "value_daily", "pt") |>
      report_recent()
  )
  # trim to max date
  icu_admissions_sk <- max_date(icu_admissions_sk, "2023-12-30")

  ## collate and process final dataset
  suppressWarnings(rm(icu_admissions_pt)) # if re-running manually
  icu_admissions_pt <- collate_datasets("icu_admissions") %>%
    dataset_format("pt")

  ## censor daily value for first date of several PTs: MB, NB, NT
  ## cumulative values are given but time series does not start at the beginning
  icu_admissions_pt[
    icu_admissions_pt$region == "MB" & icu_admissions_pt$date == as.Date("2020-05-16"), "value_daily"] <- NA
  icu_admissions_pt[
    icu_admissions_pt$region == "NB" & icu_admissions_pt$date == as.Date("2022-04-02"), "value_daily"] <- NA
  icu_admissions_pt[
    icu_admissions_pt$region == "NT" & icu_admissions_pt$date == as.Date("2021-09-08"), "value_daily"] <- NA

  ## no Canadian dataset

  # tests_completed dataset

  ## all regions (up to 2022-11-12 or earlier, depending on PT)
  tests_completed_pt <- get_phac_d("tests_completed", "all", keep_up_to_date = TRUE)

  ## remove regions with extra/alternate data
  tests_completed_pt <- tests_completed_pt %>%
    dplyr::filter(!.data$region %in% c(
      "AB", "BC", "MB", "NB", "NS", "ON", "PE", "QC", "SK", "YT"))

  ## add data for provinces where RVDSS data are representative of all tests (MB, NS, SK)
  tests_completed_pt <- dplyr::bind_rows(
    tests_completed_pt,
    append_daily_d(
      get_phac_d("tests_completed", "MB", keep_up_to_date = TRUE) |>
        dplyr::filter(.data$date <= as.Date("2022-11-19")),
      get_phac_d("tests_completed_rvdss", "MB", keep_up_to_date = TRUE) |>
        dplyr::filter(.data$date >= as.Date("2022-11-26"))
    ),
    append_daily_d(
      get_phac_d("tests_completed", "NS", keep_up_to_date = TRUE) |>
        dplyr::filter(.data$date <= as.Date("2022-11-19")),
      get_phac_d("tests_completed_rvdss", "NS", keep_up_to_date = TRUE) |>
        dplyr::filter(.data$date >= as.Date("2022-11-26"))
    ),
    append_daily_d(
      get_phac_d("tests_completed", "SK", keep_up_to_date = TRUE) |>
        dplyr::filter(.data$date <= as.Date("2022-11-19")),
      get_phac_d("tests_completed_rvdss", "SK", keep_up_to_date = TRUE) |>
        dplyr::filter(.data$date >= as.Date("2022-11-26"))
    ))

  ## add AB data
  ab1 <- dplyr::bind_rows(
    # avoid overlaps
    read_d("raw_data/static/ab/ab_tests_completed_pt_ts_1.csv") |>
      dplyr::filter(.data$date <= as.Date("2020-03-05")),
    read_d("raw_data/static/ab/ab_tests_completed_pt_ts_2.csv") |>
      dplyr::filter(.data$date <= as.Date("2023-08-26")))
  ab2 <- read_d("raw_data/active_ts/ab/ab_tests_completed_pt_ts.csv") |>
    dplyr::filter(.data$date >= as.Date("2023-09-02"))
  ab3 <- append_daily_d(ab1, ab2)
  # add AB back to main dataset
  tests_completed_pt <- dplyr::bind_rows(tests_completed_pt, ab3)
  rm(ab1, ab2, ab3) # clean up

  ## add BC data (2022-11-26 and later)
  bc1 <- get_phac_d("tests_completed", "BC", keep_up_to_date = TRUE) |>
    # avoid overlap with new dataset
    dplyr::filter(.data$date <= as.Date("2022-11-19"))
  bc2 <- read_d("raw_data/reports/bc/bc_monthly_report_testing.csv") |>
    report_pluck("tests_completed", "tests_completed_weekly", "value_daily", "pt") |>
    dplyr::filter(.data$date >= as.Date("2022-11-26"))
  bc3 <- append_daily_d(bc1, bc2)
  # add BC back to main dataset
  tests_completed_pt <- dplyr::bind_rows(tests_completed_pt, bc3)
  rm(bc1, bc2, bc3) # clean up

  ## add NB data
  nb1 <- get_phac_d("tests_completed", "NB", keep_up_to_date = TRUE) |>
    # avoid overlap with new dataset
    dplyr::filter(.data$date <= as.Date("2022-11-19"))
  nb2 <- read_d("raw_data/reports/nb/nb_weekly_report.csv") |>
    report_pluck("tests_completed", "new_tests_completed", "value_daily", "pt") |>
    dplyr::filter(.data$date >= as.Date("2022-11-26"))
  nb3 <- read_d("raw_data/reports/nb/nb_weekly_report_2.csv") |>
    report_pluck("tests_completed", "tests_completed", "value_daily", "pt")
  nb4 <- append_daily_d(nb1, nb2)
  nb4 <- append_daily_d(nb4, nb3)
  tests_completed_pt <- dplyr::bind_rows(tests_completed_pt, nb4)
  rm(nb1, nb2, nb3, nb4) # clean up

  ## add ON data
  on1 <- read_d("raw_data/static/on/on_tests_completed_pt_ts.csv") |>
    dplyr::filter(.data$date <= as.Date("2023-04-01"))
  on2 <- read_d("raw_data/reports/on/on_pho_testing.csv") |>
    report_pluck("tests_completed", "tests_completed_weekly", "value_daily", "pt") |>
    dplyr::filter(.data$date >= as.Date("2023-04-08"))
  on3 <- append_daily_d(on1, on2)
  # add ON back to main dataset
  tests_completed_pt <- dplyr::bind_rows(tests_completed_pt, on3)
  rm(on1, on2, on3) # clean up
  # trim to max date
  tests_completed_pt <- max_date(tests_completed_pt, "2023-12-30")

  ## add PE data
  tests_completed_pt <- dplyr::bind_rows(
    tests_completed_pt,
    append_daily_d(
      get_phac_d("tests_completed", "PE", keep_up_to_date = TRUE) |>
        dplyr::filter(.data$date <= as.Date("2022-08-27")),
      get_phac_d("tests_completed_rvdss", "PE", keep_up_to_date = TRUE) |>
        dplyr::filter(.data$date >= as.Date("2022-09-03"))
  ))

  ## add QC data
  tests_completed_pt <- dplyr::bind_rows(
    tests_completed_pt,
    read_d("raw_data/active_ts/qc/qc_tests_completed_pt_ts.csv") |>
      date_shift(1)
  )

  ## add YT data
  tests_completed_pt <- dplyr::bind_rows(
    tests_completed_pt,
    read_d("raw_data/static/yt/yt_tests_completed_pt_ts.csv")
  )

  ## collate and process final dataset
  tests_completed_pt <- tests_completed_pt %>%
    dataset_format("pt")

  # vaccine_coverage dataset

  ## collate and process final datasets
  vaccine_coverage_dose_1_pt <- get_phac_d("vaccine_coverage_dose_1", "all") |>
    # censor Quebec after 2022-07-17
    dplyr::filter(!(.data$region == "QC" & date > as.Date("2022-07-17"))) |>
    dataset_format("pt", digits = 2)
  vaccine_coverage_dose_2_pt <- get_phac_d("vaccine_coverage_dose_2", "all") |>
    # censor Quebec after 2022-07-17
    dplyr::filter(!(.data$region == "QC" & date > as.Date("2022-07-17"))) |>
    dataset_format("pt", digits = 2)
  vaccine_coverage_dose_3_pt <- get_phac_d("vaccine_coverage_dose_3", "all") |>
    # censor Quebec after 2022-07-17
    dplyr::filter(!(.data$region == "QC" & date > as.Date("2022-07-17"))) |>
    dataset_format("pt", digits = 2)
  vaccine_coverage_dose_4_pt <- get_phac_d("vaccine_coverage_dose_4", "all") |>
    # censor Quebec after 2022-07-17
    dplyr::filter(!(.data$region == "QC" & date > as.Date("2022-07-17"))) |>
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

  ## dose 1
  vaccine_administration_dose_1_pt <- get_phac_d("vaccine_administration_dose_1", "all") |>
      # censor QC
      dplyr::filter(.data$region != "QC") |>
      # censor ON before 2021-01-16
      dplyr::filter(!(.data$region == "ON" & .data$date < as.Date("2021-01-16"))) |>
      # censor AB, NB, PE before 2020-12-26
      dplyr::filter(!(.data$region %in% c("AB", "NB", "PE") & .data$date < as.Date("2020-12-26"))) |>
      # only include MB data from 2022-05-22 onward due to bad data before this
      dplyr::filter(!(.data$region == "MB" & .data$date < as.Date("2022-05-22"))) |>
    dplyr::bind_rows(
      read_d("raw_data/static/on/on_vaccine_administration_dose_1_pt_ts.csv") |>
        # filter to ON before 2021-01-16
        dplyr::filter(.data$region == "ON" & .data$date < as.Date("2021-01-16"))) |>
    dplyr::bind_rows(
      read_d("raw_data/ccodwg/can_vaccine_administration_dose_1_pt_ts.csv") |>
        # filter to AB, NB, PE before 2020-12-26
        dplyr::filter(.data$region %in% c("AB", "NB", "PE") & .data$date < as.Date("2020-12-26"))) |>
    dplyr::bind_rows(
      # add old MB data
      read_d("raw_data/static/mb/mb_vaccine_administration_dose_1_pt_ts.csv") |>
        date_shift(1))
  # add QC data
  vaccine_administration_dose_1_pt <- vaccine_administration_dose_1_pt |>
    dplyr::bind_rows(
      read_d("raw_data/active_ts/qc/qc_vaccine_administration_dose_1_pt_ts.csv") |>
        # filter to max date of main dataset
        dplyr::filter(.data$date <= max(vaccine_administration_dose_1_pt$date))
    ) |>
    # filter to Saturday to match PHAC dataset
    dplyr::filter(lubridate::wday(.data$date) == 7) |>
    dataset_format("pt")

  ## dose 2
  vaccine_administration_dose_2_pt <- get_phac_d("vaccine_administration_dose_2", "all") |>
    # censor QC
    dplyr::filter(.data$region != "QC") |>
    # censor ON before 2021-01-16
    dplyr::filter(!(.data$region == "ON" & .data$date < as.Date("2021-01-16"))) |>
    # only include MB data from 2022-05-22 onward due to bad data before this
    dplyr::filter(!(.data$region == "MB" & .data$date < as.Date("2022-05-22"))) |>
    dplyr::bind_rows(
      read_d("raw_data/static/on/on_vaccine_administration_dose_2_pt_ts.csv") |>
        # filter to ON before 2021-01-16
        dplyr::filter(.data$region == "ON" & .data$date < as.Date("2021-01-16"))) |>
    dplyr::bind_rows(
      # add old MB data
      read_d("raw_data/static/mb/mb_vaccine_administration_dose_2_pt_ts.csv") |>
        date_shift(1))
  # add QC data
  vaccine_administration_dose_2_pt <- vaccine_administration_dose_2_pt |>
    dplyr::bind_rows(
      read_d("raw_data/active_ts/qc/qc_vaccine_administration_dose_2_pt_ts.csv") |>
        # filter to max date of main dataset
        dplyr::filter(.data$date <= max(vaccine_administration_dose_2_pt$date))
    ) |>
    # filter to Saturday to match PHAC dataset
    dplyr::filter(lubridate::wday(.data$date) == 7) |>
    dataset_format("pt")

  ## dose 3
  vaccine_administration_dose_3_pt <- get_phac_d("vaccine_administration_dose_3", "all") |>
    # censor QC
    dplyr::filter(.data$region != "QC") |>
    # only include MB data from 2022-05-22 onward due to bad data before this
    dplyr::filter(!(.data$region == "MB" & .data$date < as.Date("2022-05-22"))) |>
    dplyr::bind_rows(
      # add old MB data
      read_d("raw_data/static/mb/mb_vaccine_administration_dose_3_pt_ts.csv") |>
        date_shift(1))
  # add QC data
  vaccine_administration_dose_3_pt <- vaccine_administration_dose_3_pt |>
    dplyr::bind_rows(
      read_d("raw_data/active_ts/qc/qc_vaccine_administration_dose_3_pt_ts.csv") |>
        # filter to max date of main dataset
        dplyr::filter(.data$date <= max(vaccine_administration_dose_3_pt$date))
    ) |>
    # filter to Saturday to match PHAC dataset
    dplyr::filter(lubridate::wday(.data$date) == 7) |>
    dataset_format("pt")

  ## dose 4
  vaccine_administration_dose_4_pt <- get_phac_d("vaccine_administration_dose_4", "all") |>
    # censor QC
    dplyr::filter(.data$region != "QC") |>
    # only include MB data from 2022-05-22 onward due to bad data before this
    dplyr::filter(!(.data$region == "MB" & .data$date < as.Date("2022-05-22")))
  # add QC data
  vaccine_administration_dose_4_pt <- vaccine_administration_dose_4_pt |>
    dplyr::bind_rows(
      read_d("raw_data/active_ts/qc/qc_vaccine_administration_dose_4_pt_ts.csv") |>
        # filter to max date of main dataset
        dplyr::filter(.data$date <= max(vaccine_administration_dose_4_pt$date))
    ) |>
    # filter to Saturday to match PHAC dataset
    dplyr::filter(lubridate::wday(.data$date) == 7) |>
    dataset_format("pt")

  # dose 5+
  vaccine_administration_dose_5plus_pt <- get_phac_d("vaccine_administration_dose_5plus", "all") |>
    # censor QC
    dplyr::filter(.data$region != "QC") |>
    # only include MB data from 2022-05-22 onward due to bad data before this
    dplyr::filter(!(.data$region == "MB" & .data$date < as.Date("2022-05-22")))
    # add QC data
    vaccine_administration_dose_5plus_pt <- vaccine_administration_dose_5plus_pt |>
    dplyr::bind_rows(
      read_d("raw_data/active_ts/qc/qc_vaccine_administration_dose_5plus_pt_ts.csv") |>
        # filter to max date of main dataset
        dplyr::filter(.data$date <= max(vaccine_administration_dose_5plus_pt$date))
    ) |>
    # filter to Saturday to match PHAC dataset
    dplyr::filter(lubridate::wday(.data$date) == 7) |>
    dataset_format("pt")

  ## total doses
  vaccine_administration_total_doses_pt <- dplyr::left_join(
    vaccine_administration_dose_1_pt |>
      dplyr::transmute(.data$date, .data$region, dose_1 = .data$value),
    vaccine_administration_dose_2_pt |>
      dplyr::transmute(.data$date, .data$region, dose_2 = .data$value),
    by = c("date", "region")) |>
    dplyr::left_join(
      vaccine_administration_dose_3_pt |>
        dplyr::transmute(.data$date, .data$region, dose_3 = .data$value),
      by = c("date", "region")) |>
    dplyr::left_join(
      vaccine_administration_dose_4_pt |>
        dplyr::transmute(.data$date, .data$region, dose_4 = .data$value),
      by = c("date", "region")) |>
    dplyr::left_join(
      vaccine_administration_dose_5plus_pt |>
        dplyr::transmute(.data$date, .data$region, dose_5plus = .data$value),
      by = c("date", "region")) |>
    dplyr::rowwise() |>
    dplyr::transmute(
      name = "vaccine_administration_total_doses",
      .data$date,
      .data$region,
      value = sum(.data$dose_1, .data$dose_2, .data$dose_3, .data$dose_4, .data$dose_5plus, na.rm = TRUE)
    ) |>
    dplyr::ungroup() |>
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

  # replace PT-level case data for some regions

  ## NB
  cases_pt <- replace_hr_phac(
    cases_pt,
    "cases",
    "NB",
    "2022-12-17")

  ## SK
  cases_pt <- replace_hr_phac(
    cases_pt,
    "cases",
    "SK",
    "2022-02-12")

  # replace PT-level death data for some regions

  ## AB
  deaths_pt <- replace_hr(
    deaths_pt,
    dplyr::bind_rows(
      read_d("raw_data/static/ab/ab_deaths_pt_ts.csv") |>
        dplyr::filter(.data$date <= "2023-08-19"),
      get_phac_d("deaths", "AB", keep_up_to_date = TRUE) |>
        dplyr::filter(.data$date >= as.Date("2023-08-26"))
    ),
    "deaths",
    "AB",
    "2020-01-01") # entire time series

  ## ON
  deaths_pt <- replace_hr(
    deaths_pt,
    read_d("raw_data/active_ts/on/on_deaths_pt_ts.csv"),
    "deaths",
    "ON",
    "2020-01-01") # entire time series

  ## SK
  deaths_pt <- replace_hr_phac(
    deaths_pt,
    "deaths",
    "SK",
    "2022-02-12")

  # create aggregated datasets (PT -> CAN)
  cases_can <- agg2can(cases_pt)
  cases_can_completeness <- agg2can_completeness(cases_pt)
  deaths_can <- agg2can(deaths_pt)
  deaths_can_completeness <- agg2can_completeness(deaths_pt)
  tests_completed_can <- agg2can(tests_completed_pt)
  tests_completed_can_completeness <- agg2can_completeness(tests_completed_pt)
  vaccine_administration_dose_1_can <- agg2can(vaccine_administration_dose_1_pt)
  vaccine_administration_dose_2_can <- agg2can(vaccine_administration_dose_2_pt)
  vaccine_administration_dose_3_can <- agg2can(vaccine_administration_dose_3_pt)
  vaccine_administration_dose_4_can <- agg2can(vaccine_administration_dose_4_pt)
  vaccine_administration_dose_5plus_can <- agg2can(vaccine_administration_dose_5plus_pt)
  vaccine_administration_total_doses_can <- agg2can(vaccine_administration_total_doses_pt)

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
  write_dataset(vaccine_administration_dose_5plus_pt, "pt", "vaccine_administration_dose_5plus_pt")
  write_dataset(vaccine_administration_dose_5plus_can, "can", "vaccine_administration_dose_5plus_can")
  write_dataset(vaccine_administration_total_doses_pt, "pt", "vaccine_administration_total_doses_pt")
  write_dataset(vaccine_administration_total_doses_can, "can", "vaccine_administration_total_doses_can")
  write_dataset(vaccine_distribution_total_doses_pt, "pt", "vaccine_distribution_total_doses")
  write_dataset(vaccine_distribution_total_doses_can, "can", "vaccine_distribution_total_doses")
}
