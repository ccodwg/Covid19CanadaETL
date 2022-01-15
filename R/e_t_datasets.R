#' Extract and transform data for the daily Canadian COVID-19 data collection
#'
#' @importFrom rlang .data
#' @param mode Process which dataset? One of "main" or "phu".
#'
#'@export
e_t_datasets <- function(mode = c("main", "phu")) {

  # verify mode
  match.arg(mode, choices = c("main", "phu"), several.ok = FALSE)

  # define dates
  date_today <- as.character(lubridate::date(lubridate::with_tz(Sys.time(), "America/Toronto")))
  date_yesterday <- as.character(lubridate::date(lubridate::with_tz(Sys.time(), "America/Toronto")) - 1)

  # download datasets
  if (mode == "main") {
    ds_dir <- dl_datasets(mode = "main")
  } else {
    ds_dir <- dl_datasets(mode = "phu")
  }

  # load data from temporary directory
  load_ds <- function(ds_dir, ds_name, type = NULL) {
    if (!is.null(type) && type == "html") {
      xml2::read_html(paste0(ds_dir, "/", ds_name, ".html"))
    } else {
      suppressWarnings(readRDS(paste0(ds_dir, "/", ds_name, ".RData")))
    }
  }

  # open sink for error messages
  e <- tempfile()
  ef <- file(e, open = "wt")
  sink(file = ef, type = "message")

  # process datasets
  if (mode == "main") {

    # AB

    ## cases (hr)
    ab_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "24a572ea-0de3-4f83-b9b7-8764ea203eb6",
      val = "cases",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "24a572ea-0de3-4f83-b9b7-8764ea203eb6", "html")
    ) %>%
      process_hr_names("AB")

    ## mortality (hr)
    ab_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "d3b170a7-bb86-4bb0-b362-2adc5e6438c2",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "d3b170a7-bb86-4bb0-b362-2adc5e6438c2", "html")
    ) %>%
      process_hr_names("AB")

    ## recovered (prov)
    ab_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "ec1acea4-8b85-4c04-b905-f075de040493",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "ec1acea4-8b85-4c04-b905-f075de040493", "html")
    )

    ## testing (prov)
    ab_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "ec1acea4-8b85-4c04-b905-f075de040493",
      val = "testing",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "ec1acea4-8b85-4c04-b905-f075de040493", "html"),
      testing_type = "n_tests_completed"
    )

    ## vaccine_distribution (prov)
    ab_vaccine_distribution_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
      val = "vaccine_distribution",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "fa3f2917-6553-438c-9a6f-2af8d077f47f", "html"),
      prov = "AB"
    )

    ## vaccine_administration (prov)
    ab_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "24a572ea-0de3-4f83-b9b7-8764ea203eb6",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "24a572ea-0de3-4f83-b9b7-8764ea203eb6", "html")
    )

    ## vaccine_completion (prov)
    ab_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "24a572ea-0de3-4f83-b9b7-8764ea203eb6",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "24a572ea-0de3-4f83-b9b7-8764ea203eb6", "html")
    )

    ## vaccine_additional_doses (prov)
    ab_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "24a572ea-0de3-4f83-b9b7-8764ea203eb6",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "24a572ea-0de3-4f83-b9b7-8764ea203eb6", "html")
    )

    # BC

    ## cases (hr)
    bc_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "91367e1d-8b79-422c-b314-9b3441ba4f42",
      val = "cases",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "91367e1d-8b79-422c-b314-9b3441ba4f42")
    ) %>%
      process_hr_names("BC")

    ## mortality (hr)
    bc_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "91367e1d-8b79-422c-b314-9b3441ba4f42",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "91367e1d-8b79-422c-b314-9b3441ba4f42")
    ) %>%
      process_hr_names("BC")

    ## recovered (prov)
    bc_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "91367e1d-8b79-422c-b314-9b3441ba4f42",
      val = "recovered",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "91367e1d-8b79-422c-b314-9b3441ba4f42")
    ) %>%
      process_agg2prov()

    ## testing (prov)
    bc_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "f9a8dea5-1eed-447b-a9a0-be2a4b62d6a6",
      val = "testing",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "f9a8dea5-1eed-447b-a9a0-be2a4b62d6a6")
    )

    ## vaccine_distribution (prov)
    bc_vaccine_distribution_prov <- dplyr::bind_rows(
      Covid19CanadaDataProcess::process_dataset(
        uuid = "9d940861-0252-4d33-b6e8-23a2eeb105bf",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = load_ds(ds_dir, "9d940861-0252-4d33-b6e8-23a2eeb105bf")
      ),
      Covid19CanadaDataProcess::process_dataset(
        uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = load_ds(ds_dir, "fa3f2917-6553-438c-9a6f-2af8d077f47f", "html"),
        prov = "BC"
      )
    )
    bc_vaccine_distribution_prov <- tryCatch(
      {
        bc_vaccine_distribution_prov %>%
          dplyr::group_by(.data$name, .data$province, .data$date) %>%
          dplyr::summarize(value = max(.data$value), .groups = "drop")
      },
      error = function(e) {message(e); return(NA)})

    ## vaccine_administration (prov)
    bc_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "9d940861-0252-4d33-b6e8-23a2eeb105bf",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "9d940861-0252-4d33-b6e8-23a2eeb105bf")
    )

    ## vaccine_completion (prov)
    bc_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "9d940861-0252-4d33-b6e8-23a2eeb105bf",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "9d940861-0252-4d33-b6e8-23a2eeb105bf")
    )

    ## vaccine_additional_doses (prov)
    bc_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "9d940861-0252-4d33-b6e8-23a2eeb105bf",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "9d940861-0252-4d33-b6e8-23a2eeb105bf")
    )

    # MB

    ## cases (hr)
    mb_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0261e07b-85ce-4952-99d1-6e1e9a440291",
      val = "cases",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "0261e07b-85ce-4952-99d1-6e1e9a440291")
    ) %>%
      process_hr_names("MB")

    ## mortality (hr)
    mb_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0261e07b-85ce-4952-99d1-6e1e9a440291",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "0261e07b-85ce-4952-99d1-6e1e9a440291")
    ) %>%
      process_hr_names("MB")

    ## recovered (prov)
    mb_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0261e07b-85ce-4952-99d1-6e1e9a440291",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "0261e07b-85ce-4952-99d1-6e1e9a440291")
    )

    ## testing (prov)
    mb_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0261e07b-85ce-4952-99d1-6e1e9a440291",
      val = "testing",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "0261e07b-85ce-4952-99d1-6e1e9a440291")
    )

    ## vaccine_distribution (prov)
    mb_vaccine_distribution_prov <- dplyr::bind_rows(
      Covid19CanadaDataProcess::process_dataset(
        uuid = "1e9f40b2-853f-49d5-a9c4-ed04fee1bea2",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = load_ds(ds_dir, "1e9f40b2-853f-49d5-a9c4-ed04fee1bea2")
      ),
      Covid19CanadaDataProcess::process_dataset(
        uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = load_ds(ds_dir, "fa3f2917-6553-438c-9a6f-2af8d077f47f", "html"),
        prov = "MB"
      )
    )
    mb_vaccine_distribution_prov <- tryCatch(
      {
        mb_vaccine_distribution_prov %>%
          dplyr::group_by(.data$name, .data$province, .data$date) %>%
          dplyr::summarize(value = max(.data$value), .groups = "drop")
      },
      error = function(e) {message(e); return(NA)})

    ## vaccine_administration (prov)
    mb_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "a5801472-42ae-409e-aedd-9bf92831434a",
      val = "vaccine_administration",
      fmt = "prov_ts",
      ds = load_ds(ds_dir, "a5801472-42ae-409e-aedd-9bf92831434a")
    ) %>%
      process_cum_current()

    ## vaccine_completion (prov)
    mb_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "a5801472-42ae-409e-aedd-9bf92831434a",
      val = "vaccine_completion",
      fmt = "prov_ts",
      ds = load_ds(ds_dir, "a5801472-42ae-409e-aedd-9bf92831434a")
    ) %>%
      process_cum_current()

    ## vaccine_additional_doses (prov)
    mb_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "a5801472-42ae-409e-aedd-9bf92831434a",
      val = "vaccine_additional_doses",
      fmt = "prov_ts",
      ds = load_ds(ds_dir, "a5801472-42ae-409e-aedd-9bf92831434a")
    ) %>%
      process_cum_current()

    # NB

    ## cases (hr)
    nb_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4f194e3b-39fd-4fe0-b420-8cefa9001f7e",
      val = "cases",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "4f194e3b-39fd-4fe0-b420-8cefa9001f7e")
    ) %>%
      process_hr_names("NB")

    ## mortality (hr)
    nb_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4f194e3b-39fd-4fe0-b420-8cefa9001f7e",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "4f194e3b-39fd-4fe0-b420-8cefa9001f7e")
    ) %>%
      process_hr_names("NB")

    ## recovered (prov)
    nb_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4f194e3b-39fd-4fe0-b420-8cefa9001f7e",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "4f194e3b-39fd-4fe0-b420-8cefa9001f7e")
    )

    ## testing (prov)
    nb_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4f194e3b-39fd-4fe0-b420-8cefa9001f7e",
      val = "testing",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "4f194e3b-39fd-4fe0-b420-8cefa9001f7e")
    )

    ## vaccine_distribution (prov)
    nb_vaccine_distribution_prov <- dplyr::bind_rows(
      Covid19CanadaDataProcess::process_dataset(
        uuid = "6996a762-56cc-4a27-8e77-1c8734752793",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = load_ds(ds_dir, "6996a762-56cc-4a27-8e77-1c8734752793")
      ),
      Covid19CanadaDataProcess::process_dataset(
        uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = load_ds(ds_dir, "fa3f2917-6553-438c-9a6f-2af8d077f47f", "html"),
        prov = "NB"
      )
    )
    nb_vaccine_distribution_prov <- tryCatch(
      {
        nb_vaccine_distribution_prov %>%
          dplyr::group_by(.data$name, .data$province, .data$date) %>%
          dplyr::summarize(value = max(.data$value), .groups = "drop")
      },
      error = function(e) {message(e); return(NA)})

    ## vaccine_administration (prov)
    nb_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "6996a762-56cc-4a27-8e77-1c8734752793",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "6996a762-56cc-4a27-8e77-1c8734752793")
    )

    ## vaccine_completion (prov)
    nb_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "6996a762-56cc-4a27-8e77-1c8734752793",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "6996a762-56cc-4a27-8e77-1c8734752793")
    )

    ## vaccine_additional_doses (prov)
    nb_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "6996a762-56cc-4a27-8e77-1c8734752793",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "6996a762-56cc-4a27-8e77-1c8734752793")
    )

    # NL

    ## cases (hr)
    nl_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "f0e10f54-a4db-48d8-9c4e-8571e663ca28",
      val = "cases",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "f0e10f54-a4db-48d8-9c4e-8571e663ca28")
    ) %>%
      process_hr_names("NL")

    ## mortality (hr)
    nl_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "f0e10f54-a4db-48d8-9c4e-8571e663ca28",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "f0e10f54-a4db-48d8-9c4e-8571e663ca28")
    ) %>%
      process_hr_names("NL")

    ## recovered (prov)
    nl_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "8419f6f1-b80b-4247-84e5-6414ab0154d8",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "8419f6f1-b80b-4247-84e5-6414ab0154d8")
    )

    ## testing (prov)
    nl_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "8419f6f1-b80b-4247-84e5-6414ab0154d8",
      val = "testing",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "8419f6f1-b80b-4247-84e5-6414ab0154d8")
    )

    ## vaccine_distribution (prov)
    nl_vaccine_distribution_prov <- dplyr::bind_rows(
      Covid19CanadaDataProcess::process_dataset(
        uuid = "6d1b0f2e-0ac0-4ad8-a383-619065ec5a52",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = load_ds(ds_dir, "6d1b0f2e-0ac0-4ad8-a383-619065ec5a52")
      ),
      Covid19CanadaDataProcess::process_dataset(
        uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = load_ds(ds_dir, "fa3f2917-6553-438c-9a6f-2af8d077f47f", "html"),
        prov = "NL"
      )
    )
    nl_vaccine_distribution_prov <- tryCatch(
      {
        nl_vaccine_distribution_prov %>%
          dplyr::group_by(.data$name, .data$province, .data$date) %>%
          dplyr::summarize(value = max(.data$value), .groups = "drop")
      },
      error = function(e) {message(e); return(NA)})

    ## vaccine_administration (prov)
    nl_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "64d10e59-6b60-474f-9a4f-6c6a2c71b1a8",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "64d10e59-6b60-474f-9a4f-6c6a2c71b1a8")
    )

    ## vaccine_completion (prov)
    nl_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "64d10e59-6b60-474f-9a4f-6c6a2c71b1a8",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "64d10e59-6b60-474f-9a4f-6c6a2c71b1a8")
    )

    ## vaccine_additional_doses (prov)
    nl_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "64d10e59-6b60-474f-9a4f-6c6a2c71b1a8",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "64d10e59-6b60-474f-9a4f-6c6a2c71b1a8")
    )

    # NS

    ## cases (hr)
    ns_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "d0f05ef1-419f-4f4c-bc2d-17446c10059f",
      val = "cases",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "d0f05ef1-419f-4f4c-bc2d-17446c10059f")
    ) %>%
      process_hr_names("NS")

    ## mortality (hr)
    ns_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "d0f05ef1-419f-4f4c-bc2d-17446c10059f",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "d0f05ef1-419f-4f4c-bc2d-17446c10059f")
    ) %>%
      process_hr_names("NS")

    ## recovered (prov)
    ns_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "d0f05ef1-419f-4f4c-bc2d-17446c10059f",
      val = "recovered",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "d0f05ef1-419f-4f4c-bc2d-17446c10059f")
    ) %>%
      process_agg2prov()

    ## testing (prov)
    ns_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0e7a1f46-5d31-4267-be97-831172fa7081",
      val = "testing",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "0e7a1f46-5d31-4267-be97-831172fa7081")
    )

    ## vaccine_distribution (prov)
    ns_vaccine_distribution_prov <- dplyr::bind_rows(
      Covid19CanadaDataProcess::process_dataset(
        uuid = "70214276-8616-488c-b53a-b514608e3146",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = load_ds(ds_dir, "70214276-8616-488c-b53a-b514608e3146")
      ),
      Covid19CanadaDataProcess::process_dataset(
        uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = load_ds(ds_dir, "fa3f2917-6553-438c-9a6f-2af8d077f47f", "html"),
        prov = "NS"
      )
    )
    ns_vaccine_distribution_prov <- tryCatch(
      {
        ns_vaccine_distribution_prov %>%
          dplyr::group_by(.data$name, .data$province, .data$date) %>%
          dplyr::summarize(value = max(.data$value), .groups = "drop")
      },
      error = function(e) {message(e); return(NA)})

    ## vaccine_administration (prov)
    ns_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "70214276-8616-488c-b53a-b514608e3146",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "70214276-8616-488c-b53a-b514608e3146")
    )

    ## vaccine_completion (prov)
    ns_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "70214276-8616-488c-b53a-b514608e3146",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "70214276-8616-488c-b53a-b514608e3146")
    )

    ## vaccine_additional_doses (prov)
    ns_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "70214276-8616-488c-b53a-b514608e3146",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "70214276-8616-488c-b53a-b514608e3146")
    )

    # NT

    ## cases (hr)
    nt_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "e008ba6f-7b09-4af4-afc5-63ec3c3bbfbb",
      val = "cases",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "e008ba6f-7b09-4af4-afc5-63ec3c3bbfbb", "html")
    ) %>%
      process_prov2hr("NT")

    ## mortality (hr)
    nt_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "e008ba6f-7b09-4af4-afc5-63ec3c3bbfbb",
      val = "mortality",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "e008ba6f-7b09-4af4-afc5-63ec3c3bbfbb", "html")
    ) %>%
      process_prov2hr("NT")

    ## recovered (prov)
    nt_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "e008ba6f-7b09-4af4-afc5-63ec3c3bbfbb",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "e008ba6f-7b09-4af4-afc5-63ec3c3bbfbb", "html")
    )

    ## testing (prov)
    nt_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "66fbe91e-34c0-4f7f-aa94-cf6c14db0158",
      val = "testing",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "66fbe91e-34c0-4f7f-aa94-cf6c14db0158", "html")
    )

    ## vaccine_distribution (prov)
    nt_vaccine_distribution_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
      val = "vaccine_distribution",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "fa3f2917-6553-438c-9a6f-2af8d077f47f", "html"),
      prov = "NT"
    )

    ## vaccine_administration (prov)
    nt_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "454de458-f7b4-4814-96a6-5a426f8c8c60",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "454de458-f7b4-4814-96a6-5a426f8c8c60", "html")
    )

    ## vaccine_completion (prov)
    nt_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "454de458-f7b4-4814-96a6-5a426f8c8c60",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "454de458-f7b4-4814-96a6-5a426f8c8c60", "html")
    )

    ## vaccine_additional_doses (prov)
    nt_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "454de458-f7b4-4814-96a6-5a426f8c8c60",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "454de458-f7b4-4814-96a6-5a426f8c8c60", "html")
    )

    # NU

    ## cases (hr)
    nu_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "04ab3773-f535-42ad-8ee4-4d584ec23523",
      val = "cases",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "04ab3773-f535-42ad-8ee4-4d584ec23523", "html")
    ) %>%
      process_prov2hr("NU")

    ## mortality (hr)
    nu_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "04ab3773-f535-42ad-8ee4-4d584ec23523",
      val = "mortality",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "04ab3773-f535-42ad-8ee4-4d584ec23523", "html")
    ) %>%
      process_prov2hr("NU")

    ## recovered (prov)
    nu_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "04ab3773-f535-42ad-8ee4-4d584ec23523",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "04ab3773-f535-42ad-8ee4-4d584ec23523", "html")
    )

    ## testing (prov)
    nu_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "04ab3773-f535-42ad-8ee4-4d584ec23523",
      val = "testing",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "04ab3773-f535-42ad-8ee4-4d584ec23523", "html"),
      testing_type = "n_people_tested"
    )

    ## vaccine_distribution (prov)
    nu_vaccine_distribution_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
      val = "vaccine_distribution",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "fa3f2917-6553-438c-9a6f-2af8d077f47f", "html"),
      prov = "NU"
    )

    ## vaccine_administration (prov)
    nu_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "04ab3773-f535-42ad-8ee4-4d584ec23523",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "04ab3773-f535-42ad-8ee4-4d584ec23523", "html")
    )

    ## vaccine_completion (prov)
    nu_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "04ab3773-f535-42ad-8ee4-4d584ec23523",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "04ab3773-f535-42ad-8ee4-4d584ec23523", "html")
    )

    # ON

    # ## cases (hr)
    # on_cases_hr <- Covid19CanadaDataProcess::process_dataset(
    #   uuid = "73fffd44-fbad-4de8-8d32-00cc5ae180a6",
    #   val = "cases",
    #   fmt = "hr_ts",
    #   ds = load_ds(ds_dir, "73fffd44-fbad-4de8-8d32-00cc5ae180a6")
    # ) %>%
    #   process_hr_names("ON", opt = "moh") %>%
    #   process_cum_current()
    #
    # ## mortality (hr)
    # on_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
    #   uuid = "73fffd44-fbad-4de8-8d32-00cc5ae180a6",
    #   val = "mortality",
    #   fmt = "hr_ts",
    #   ds = load_ds(ds_dir, "73fffd44-fbad-4de8-8d32-00cc5ae180a6")
    # ) %>%
    #   process_hr_names("ON", opt = "moh") %>%
    #   process_cum_current()
    #
    # ## recovered (prov)
    # on_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
    #   uuid = "73fffd44-fbad-4de8-8d32-00cc5ae180a6",
    #   val = "recovered",
    #   fmt = "hr_ts",
    #   ds = load_ds(ds_dir, "73fffd44-fbad-4de8-8d32-00cc5ae180a6")
    # ) %>%
    #   process_agg2prov() %>%
    #   process_cum_current()

    ## testing (prov)
    on_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "a8b1be1a-561a-47f5-9456-c553ea5b2279",
      val = "testing",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "a8b1be1a-561a-47f5-9456-c553ea5b2279")
    )

    ## vaccine_distribution (prov)
    on_vaccine_distribution_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
      val = "vaccine_distribution",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "fa3f2917-6553-438c-9a6f-2af8d077f47f", "html"),
      prov = "ON"
    )

    ## vaccine_administration (prov)
    on_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "170057c4-3231-4f15-9438-2165c5438dda",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "170057c4-3231-4f15-9438-2165c5438dda")
    )

    ## vaccine_completion (prov)
    on_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "170057c4-3231-4f15-9438-2165c5438dda",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "170057c4-3231-4f15-9438-2165c5438dda")
    )

    ## vaccine_additional_doses (prov)
    on_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "170057c4-3231-4f15-9438-2165c5438dda",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "170057c4-3231-4f15-9438-2165c5438dda")
    )

    # PE

    ## cases (hr)
    pe_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455",
      val = "cases",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455", "html")
    ) %>%
      process_prov2hr("PE")

    ## mortality (hr)
    pe_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455",
      val = "mortality",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455", "html")
    ) %>%
      process_prov2hr("PE")

    ## recovered (prov)
    pe_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455", "html")
    )

    ## testing (prov)
    pe_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455",
      val = "testing",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455", "html")
    )

    # vaccine_distribution (prov)
    pe_vaccine_distribution_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
      val = "vaccine_distribution",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "fa3f2917-6553-438c-9a6f-2af8d077f47f", "html"),
      prov = "PE"
    )

    ## vaccine_administration (prov)
    pe_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "3ff94c42-8b12-4653-a6c9-0ddd8ff343d5",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "3ff94c42-8b12-4653-a6c9-0ddd8ff343d5-Total-Doses")
    )

    ## vaccine_completion (prov)
    pe_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "3ff94c42-8b12-4653-a6c9-0ddd8ff343d5",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "3ff94c42-8b12-4653-a6c9-0ddd8ff343d5-Fully-Immunized")
    )

    ## vaccine_additional_doses (prov)
    pe_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "3ff94c42-8b12-4653-a6c9-0ddd8ff343d5",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "3ff94c42-8b12-4653-a6c9-0ddd8ff343d5-Third-Doses")
    )

    # QC

    ## cases (hr)
    qc_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0c577d5e-999e-42c5-b4c1-66b3787c3a04",
      val = "cases",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "0c577d5e-999e-42c5-b4c1-66b3787c3a04")
    ) %>%
      process_hr_names("QC") %>%
      dplyr::group_by(dplyr::across(c(-.data$value))) %>%
      dplyr::summarize(value = sum(.data$value), .groups = "drop")

    ## mortality (hr)
    qc_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0c577d5e-999e-42c5-b4c1-66b3787c3a04",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "0c577d5e-999e-42c5-b4c1-66b3787c3a04")
    ) %>%
      process_hr_names("QC") %>%
      dplyr::group_by(dplyr::across(c(-.data$value))) %>%
      dplyr::summarize(value = sum(.data$value), .groups = "drop")

    ## recovered (prov)
    qc_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0c577d5e-999e-42c5-b4c1-66b3787c3a04",
      val = "recovered",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "0c577d5e-999e-42c5-b4c1-66b3787c3a04")
    ) %>%
      process_agg2prov()

    ## testing (prov)
    qc_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5",
      val = "testing",
      fmt = "hr_ts",
      ds = load_ds(ds_dir, "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5"),
      testing_type = "n_people_tested"
    ) %>%
      process_agg2prov() %>%
      process_cum_current()

    ## vaccine_distribution (prov)
    qc_vaccine_distribution_prov <- dplyr::bind_rows(
      Covid19CanadaDataProcess::process_dataset(
        uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = load_ds(ds_dir, "fa3f2917-6553-438c-9a6f-2af8d077f47f", "html"),
        prov = "QC"
      ),
      Covid19CanadaDataProcess::process_dataset(
        uuid = "aee3bd38-b782-4880-9033-db76f84cef5b",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = load_ds(ds_dir, "aee3bd38-b782-4880-9033-db76f84cef5b")
      )
    ) %>%
      dplyr::group_by(.data$name, .data$province, .data$date) %>%
      dplyr::summarize(value = max(.data$value), .groups = "drop")

    ## vaccine_administration (prov)
    qc_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4e04442d-f372-4357-ba15-3b64f4e03fbe",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "4e04442d-f372-4357-ba15-3b64f4e03fbe")
    )

    ## vaccine_completion (prov)
    qc_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4e04442d-f372-4357-ba15-3b64f4e03fbe",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "4e04442d-f372-4357-ba15-3b64f4e03fbe")
    )

    ## vaccine_additional_doses (prov)
    qc_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4e04442d-f372-4357-ba15-3b64f4e03fbe",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "4e04442d-f372-4357-ba15-3b64f4e03fbe")
    )

    # SK

    ## cases (hr)
    today <- Covid19CanadaDataProcess::process_dataset(
      uuid = "95de79d5-5e5c-45c2-bbab-41daf3dbee5d",
      val = "cases",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "95de79d5-5e5c-45c2-bbab-41daf3dbee5d")
    ) %>%
      process_sk_new2old()
    yesterday <- Covid19CanadaDataProcess::process_dataset(
      uuid = "95de79d5-5e5c-45c2-bbab-41daf3dbee5d",
      val = "cases",
      fmt = "hr_cum_current",
      ds = Covid19CanadaData::dl_archive("95de79d5-5e5c-45c2-bbab-41daf3dbee5d", date = "latest")[[1]]
    ) %>%
      process_sk_new2old()
    sk_cases_hr <- tryCatch({
      diff <- today$value - yesterday$value
      current <- googlesheets4::read_sheet(
        "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
        sheet = "cases_timeseries_hr") %>%
        dplyr::filter(.data$province == "Saskatchewan") %>%
        dplyr::pull(dplyr::all_of(date_yesterday)) %>%
        as.integer()
      today %>%
        dplyr::select(
          .data$name,
          .data$province,
          .data$sub_region_1,
          .data$date
        ) %>%
        dplyr::mutate(value = current + diff)
    },
    error = function(e) {message(e); return(NA)})
    rm(today, yesterday, diff, current) # clean up

    ## mortality (hr)
    today <- Covid19CanadaDataProcess::process_dataset(
      uuid = "95de79d5-5e5c-45c2-bbab-41daf3dbee5d",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "95de79d5-5e5c-45c2-bbab-41daf3dbee5d")
    ) %>%
      process_sk_new2old()
    yesterday <- Covid19CanadaDataProcess::process_dataset(
      uuid = "95de79d5-5e5c-45c2-bbab-41daf3dbee5d",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = Covid19CanadaData::dl_archive("95de79d5-5e5c-45c2-bbab-41daf3dbee5d", date = "latest")[[1]]
    ) %>%
      process_sk_new2old()
    sk_mortality_hr <- tryCatch({
      diff <- today$value - yesterday$value
      current <- googlesheets4::read_sheet(
        "1dTfl_3Zwf7HgRFfwqjsOlvHyDh-sCwgly2YDdHTKaSU",
        sheet = "mortality_timeseries_hr") %>%
        dplyr::filter(.data$province == "Saskatchewan") %>%
        dplyr::pull(dplyr::all_of(date_yesterday)) %>%
        as.integer()
      today %>%
        dplyr::select(
          .data$name,
          .data$province,
          .data$sub_region_1,
          .data$date
        ) %>%
        dplyr::mutate(value = current + diff)
    },
    error = function(e) {message(e); return(NA)})
    rm(today, yesterday, diff, current) # clean up

    ## recovered (prov)
    sk_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "95de79d5-5e5c-45c2-bbab-41daf3dbee5d",
      val = "recovered",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "95de79d5-5e5c-45c2-bbab-41daf3dbee5d")
    ) %>%
      process_agg2prov()

    ## testing (prov)
    sk_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "9736bff9-4bd3-4c04-b9d9-87f60b3d5eb5",
      val = "testing",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "9736bff9-4bd3-4c04-b9d9-87f60b3d5eb5"),
      testing_type = "n_people_tested"
    ) %>%
      process_agg2prov()

    ## vaccine_distribution (prov)
    sk_vaccine_distribution_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
      val = "vaccine_distribution",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "fa3f2917-6553-438c-9a6f-2af8d077f47f", "html"),
      prov = "SK"
    )

    ## vaccine_administration (prov)
    sk_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "15556169-0471-49ea-926e-20b5e8dbd25d",
      val = "vaccine_administration",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "15556169-0471-49ea-926e-20b5e8dbd25d")
    ) %>%
      process_agg2prov()

    ## vaccine_completion (prov)
    sk_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "15556169-0471-49ea-926e-20b5e8dbd25d",
      val = "vaccine_completion",
      fmt = "hr_cum_current",
      ds = load_ds(ds_dir, "15556169-0471-49ea-926e-20b5e8dbd25d")
    ) %>%
      process_agg2prov()

    ## vaccine_additional_doses (prov)
    sk_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "28d7f978-9a7b-4933-a520-41b073868d05",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "28d7f978-9a7b-4933-a520-41b073868d05", "html")
    )

    # YT

    ## cases (hr)
    yt_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e",
      val = "cases",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e", "html")
    ) %>%
      process_prov2hr("YT")

    ## mortality (hr)
    yt_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e",
      val = "mortality",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e", "html")
    ) %>%
      process_prov2hr("YT")

    ## recovered (prov)
    yt_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e", "html")
    )

    ## testing (prov)
    yt_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e",
      val = "testing",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e", "html"),
      testing_type = "n_people_tested"
    )

    ## vaccine_distribution (prov)
    yt_vaccine_distribution_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
      val = "vaccine_distribution",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "fa3f2917-6553-438c-9a6f-2af8d077f47f", "html"),
      prov = "YT"
    )

    ## vaccine_administration (prov)
    yt_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e", "html")
    )

    ## vaccine_completion (prov)
    yt_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e", "html")
    )

    ## vaccine_additional_doses (prov)
    yt_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = load_ds(ds_dir, "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e", "html")
    )

  } else {

    # define PHU names, abbreviations, UUIDs, sheet
    phu <- matrix(
      c(
        "Algoma", "ALG", "685df305-f6c7-4ac2-992b-ec707eb1f1cb", NA,
        "Brant", "BRN", "2e7a5549-92ae-473d-a97a-7b8e0c1ddbbc", NA,
        "Durham", "DUR", "ba7b0d74-5fe2-41d8-aadb-6320ff9acb21", NA,
        "Eastern", "EOH", "cd1db4e8-c4e5-4b24-86a5-2294281919c6", NA,
        "Grey Bruce", "GBH", "eac45a46-e5b5-4e75-9393-77995cd7e219", NA,
        "Haldimand-Norfolk", "HNH", "07fcf6b9-6e61-433e-b1a8-a951ee15b01d", NA,
        "Haliburton Kawartha Pineridge", "HKP", "c1cd96db-69c3-4970-9a4b-e7bcdc12d39b", NA,
        "Halton", "HAL", "8d4067a7-4828-4b09-8396-089231cf2e94", NA,
        "Hamilton", "HAM", "b8ef690e-d23f-4b7d-8cf8-bc4a0f3d0a84", NA,
        "Kingston Frontenac Lennox & Addington", "KFL", "83d1fa13-7fb3-4079-b3dc-5bc50c584fd3", NA,
        "Lambton", "LAM", "8d0cf226-b9b7-4fc3-8100-a4f56dec6792", NA,
        "Middlesex-London", "MSL", "b32a2f6b-7745-4bb1-9f9b-7ad0000d98a0", NA,
        "Niagara", "NIA", "e1887eb2-538f-4610-bc00-bcd7d929a375", NA,
        "North Bay Parry Sound", "NPS", "3178dd11-17af-4478-a72e-e1a35d7d1b2d", NA,
        "Northwestern", "NWR", "4c56a58b-0cb3-4d71-bafe-9fdb42e5c1d5", NA,
        "Ottawa", "OTT", "d8d4cbc6-d0a5-4544-ad3e-5a3c3060f973", NA,
        "Peel", "PEL", "34b7dda2-1843-47e1-9c24-0c2a7ab78431", NA,
        "Peterborough", "PET", "c3aa6a5e-2ff5-4158-83ab-dcde251bc365", NA,
        "Porcupine", "PQP", "00cc3ae2-7bf8-4074-81b7-8e06e91c947a", NA,
        "Renfrew", "REN", "688bf944-9be6-49c3-ae5d-848ae32bad92", NA,
        "Simcoe Muskoka", "SMD", "7106106a-2f43-4ed2-b2a2-a75a7046ff81", NA,
        "Sudbury", "SUD", "4b9c88a2-9487-4632-adc5-cfd4a2fddb3f", NA,
        "Thunder Bay", "THB", "942e48c4-1148-46e1-a5d3-e25aa9bede05", NA,
        "Timiskaming", "TSK", "9c7bbba4-33ba-493a-8ea1-4eedd5149bc0", NA,
        "Toronto", "TOR", "ebad185e-9706-44f4-921e-fc89d5cfa334", "Status",
        "Wellington Dufferin Guelph", "WDG", "e00e2148-b0ea-458b-9f00-3533e0c5ae8e", NA,
        "Windsor-Essex", "WEK", "01574538-062f-4a41-9dd5-8fdb72a0fe03", NA,
        "York", "YRK", "3821cc66-f88d-4f12-99ca-d36d368872cd",  NA
      ),
      ncol = 4,
      byrow = TRUE
    )

    # process data
    for (i in 1:nrow(phu)) {
      for (val in c("cases", "mortality", "recovered")) {
        uuid <- phu[i, 3]
        sheet <- phu[i, 4]
        ds_name <- if (is.na(sheet)) {
          uuid
        } else {
          paste(uuid, gsub(" ", "-", sheet), sep = "-")
        }
        if (ds_name %in% c(
          # Ottawa
          "d8d4cbc6-d0a5-4544-ad3e-5a3c3060f973",
          # Toronto
          "ebad185e-9706-44f4-921e-fc89d5cfa334-Status",
          # York
          "3821cc66-f88d-4f12-99ca-d36d368872cd"
        )) {
          type <- NULL
        } else {
          type <- "html"
        }
        assign(paste(tolower(phu[i, 2]), sep = "_", val, "hr"), Covid19CanadaDataProcess::process_dataset(
          uuid = uuid,
          val = val,
          fmt = "hr_cum_current",
          hr = phu[i, 1],
          ds = load_ds(ds_dir, ds_name, type)
        ))}}

    # add data from # Ontario Ministry of Health Time Series by PHU
    on_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "73fffd44-fbad-4de8-8d32-00cc5ae180a6",
      val = "cases",
      fmt = "hr_ts",
      ds = load_ds(ds_dir, "73fffd44-fbad-4de8-8d32-00cc5ae180a6")) %>%
      process_hr_names("ON", opt = "moh") %>%
      process_cum_current()
    on_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "73fffd44-fbad-4de8-8d32-00cc5ae180a6",
      val = "mortality",
      fmt = "hr_ts",
      ds = load_ds(ds_dir, "73fffd44-fbad-4de8-8d32-00cc5ae180a6")) %>%
      process_hr_names("ON", opt = "moh") %>%
      process_cum_current()
    on_recovered_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "73fffd44-fbad-4de8-8d32-00cc5ae180a6",
      val = "recovered",
      fmt = "hr_ts",
      ds = load_ds(ds_dir, "73fffd44-fbad-4de8-8d32-00cc5ae180a6")) %>%
      process_hr_names("ON", opt = "moh") %>%
      process_cum_current()

    # Chatham-Kent (CKH)
    tryCatch(
      {
        ckh_cases_hr <- on_cases_hr %>%
          dplyr::filter(.data$sub_region_1 == "Chatham-Kent")
      },
      error = function(e) {message(e); return(NA)}
    )
    tryCatch(
      {
        ckh_mortality_hr <- on_mortality_hr %>%
          dplyr::filter(.data$sub_region_1 == "Chatham-Kent")
      },
      error = function(e) {message(e); return(NA)}
    )
    tryCatch(
      {
        ckh_recovered_hr <- on_recovered_hr %>%
          dplyr::filter(.data$sub_region_1 == "Chatham-Kent")
      },
      error = function(e) {message(e); return(NA)}
    )

    # Leeds Grenville and Lanark (LGL)
    tryCatch(
      {
        lgl_cases_hr <- on_cases_hr %>%
          dplyr::filter(.data$sub_region_1 == "Leeds Grenville and Lanark")
      },
      error = function(e) {message(e); return(NA)}
    )
    tryCatch(
      {
        lgl_mortality_hr <- on_mortality_hr %>%
          dplyr::filter(.data$sub_region_1 == "Leeds Grenville and Lanark")
      },
      error = function(e) {message(e); return(NA)}
    )
    tryCatch(
      {
        lgl_recovered_hr <- on_recovered_hr %>%
          dplyr::filter(.data$sub_region_1 == "Leeds Grenville and Lanark")
      },
      error = function(e) {message(e); return(NA)}
    )

    # clean up
    rm(on_cases_hr, on_mortality_hr, on_recovered_hr)

    # add ON Not Reported
    onnr_cases_hr <- data.frame(
      name = "cases",
      province = "ON",
      sub_region_1 = "Not Reported",
      date = lubridate::date(lubridate::with_tz(Sys.time(), "America/Toronto")),
      value = 0
    )
    onnr_mortality_hr <- onnr_cases_hr %>%
      dplyr::mutate(name = "mortality")
    onnr_recovered_hr <- onnr_cases_hr %>%
      dplyr::mutate(name = "recovered")

    # remove objects no longer needed
    rm(phu)

  }

  # close sink
  sink(NULL, type = "message")
  close(ef)
  # process errors
  readLines(e) %>%
    # filter messages from googlesheets4 (begin with a checkmark, auth token)
    grep("^\u2713|^\u2714|^Auto-refreshing stale OAuth token\\.$", ., invert = TRUE, value = TRUE) %>%
    paste(collapse = "\n") %>%
    # if there are errors, write to error log
    log_error()

  # return all data frames created by the function as a list
  Filter(function(x) is.data.frame(x), mget(ls()))

}
