#' Extract and transform data for the daily Canadian COVID-19 data collection
#'
#' @importFrom rlang .data
#' @param ds Datasets from \link{dl_datasets}. Optional, will be downloaded if not
#'supplied to the function.
#' @param mode Process which dataset? One of "main" or "phu".
#'
#'@export
e_t_datasets <- function(ds = NULL, mode = c("main", "phu")) {

  # verify mode
  match.arg(mode, choices = c("main", "phu"), several.ok = FALSE)

  # if datasets are not supplied, download them
  if (is.null(ds)) {
    if (mode == "main") {
      ds <- dl_datasets(mode = "main")
    } else {
      ds <- dl_datasets(mode = "phu")
    }
  }

  # process datasets
  if (mode == "main") {

    # AB

    ## cases (hr)
    ab_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "59da1de8-3b4e-429a-9e18-b67ba3834002",
      val = "cases",
      fmt = "hr_ts",
      ds = ds[["59da1de8-3b4e-429a-9e18-b67ba3834002"]]
    ) %>%
      process_hr_names("AB") %>%
      process_cum_current()

    ## mortality (hr)
    ab_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "59da1de8-3b4e-429a-9e18-b67ba3834002",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = ds[["59da1de8-3b4e-429a-9e18-b67ba3834002"]]
    ) %>%
      process_hr_names("AB")

    ## recovered (prov)
    ab_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "ec1acea4-8b85-4c04-b905-f075de040493",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = ds[["ec1acea4-8b85-4c04-b905-f075de040493"]]
    )

    ## testing (prov)
    ab_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "ec1acea4-8b85-4c04-b905-f075de040493",
      val = "testing",
      fmt = "prov_cum_current",
      ds = ds[["ec1acea4-8b85-4c04-b905-f075de040493"]],
      testing_type = "n_tests_completed"
    )

    ## vaccine_distribution (prov)
    ab_vaccine_distribution_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
      val = "vaccine_distribution",
      fmt = "prov_cum_current",
      ds = ds[["fa3f2917-6553-438c-9a6f-2af8d077f47f"]],
      prov = "AB"
    )

    ## vaccine_administration (prov)
    ab_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "24a572ea-0de3-4f83-b9b7-8764ea203eb6",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = ds[["24a572ea-0de3-4f83-b9b7-8764ea203eb6"]]
    )

    ## vaccine_completion (prov)
    ab_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "24a572ea-0de3-4f83-b9b7-8764ea203eb6",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = ds[["24a572ea-0de3-4f83-b9b7-8764ea203eb6"]]
    )

    ## vaccine_additional_doses (prov)
    ab_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "24a572ea-0de3-4f83-b9b7-8764ea203eb6",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = ds[["24a572ea-0de3-4f83-b9b7-8764ea203eb6"]]
    )

    # BC

    ## cases (hr)
    bc_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "91367e1d-8b79-422c-b314-9b3441ba4f42",
      val = "cases",
      fmt = "hr_cum_current",
      ds = ds[["91367e1d-8b79-422c-b314-9b3441ba4f42"]]
    ) %>%
      process_hr_names("BC")

    ## mortality (hr)
    bc_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "91367e1d-8b79-422c-b314-9b3441ba4f42",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = ds[["91367e1d-8b79-422c-b314-9b3441ba4f42"]]
    ) %>%
      process_hr_names("BC")

    ## recovered (prov)
    bc_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "91367e1d-8b79-422c-b314-9b3441ba4f42",
      val = "recovered",
      fmt = "hr_cum_current",
      ds = ds[["91367e1d-8b79-422c-b314-9b3441ba4f42"]]
    ) %>%
      process_agg2prov

    ## testing (prov)
    bc_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "f9a8dea5-1eed-447b-a9a0-be2a4b62d6a6",
      val = "testing",
      fmt = "prov_cum_current",
      ds = ds[["f9a8dea5-1eed-447b-a9a0-be2a4b62d6a6"]]
    )

    ## vaccine_distribution (prov)
    bc_vaccine_distribution_prov <- dplyr::bind_rows(
      Covid19CanadaDataProcess::process_dataset(
        uuid = "9d940861-0252-4d33-b6e8-23a2eeb105bf",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = ds[["9d940861-0252-4d33-b6e8-23a2eeb105bf"]]
      ),
      Covid19CanadaDataProcess::process_dataset(
        uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = ds[["fa3f2917-6553-438c-9a6f-2af8d077f47f"]],
        prov = "BC"
      )
    ) %>%
      dplyr::group_by(.data$name, .data$province, .data$date) %>%
      dplyr::summarize(value = max(.data$value)) %>%
      dplyr::ungroup()

    ## vaccine_administration (prov)
    bc_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "9d940861-0252-4d33-b6e8-23a2eeb105bf",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = ds[["9d940861-0252-4d33-b6e8-23a2eeb105bf"]]
    )

    ## vaccine_completion (prov)
    bc_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "9d940861-0252-4d33-b6e8-23a2eeb105bf",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = ds[["9d940861-0252-4d33-b6e8-23a2eeb105bf"]]
    )

    ## vaccine_additional_doses (prov)
    bc_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "9d940861-0252-4d33-b6e8-23a2eeb105bf",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = ds[["9d940861-0252-4d33-b6e8-23a2eeb105bf"]]
    )

    # MB

    ## cases (hr)
    mb_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0261e07b-85ce-4952-99d1-6e1e9a440291",
      val = "cases",
      fmt = "hr_cum_current",
      ds = ds[["0261e07b-85ce-4952-99d1-6e1e9a440291"]]
    ) %>%
      process_hr_names("MB")

    ## mortality (hr)
    mb_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0261e07b-85ce-4952-99d1-6e1e9a440291",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = ds[["0261e07b-85ce-4952-99d1-6e1e9a440291"]]
    ) %>%
      process_hr_names("MB")

    ## recovered (prov)
    mb_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0261e07b-85ce-4952-99d1-6e1e9a440291",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = ds[["0261e07b-85ce-4952-99d1-6e1e9a440291"]]
    )

    ## testing (prov)
    mb_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0261e07b-85ce-4952-99d1-6e1e9a440291",
      val = "testing",
      fmt = "prov_cum_current",
      ds = ds[["0261e07b-85ce-4952-99d1-6e1e9a440291"]]
    )

    ## vaccine_distribution (prov)
    mb_vaccine_distribution_prov <- dplyr::bind_rows(
      Covid19CanadaDataProcess::process_dataset(
        uuid = "1e9f40b2-853f-49d5-a9c4-ed04fee1bea2",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = ds[["1e9f40b2-853f-49d5-a9c4-ed04fee1bea2"]]
      ),
      Covid19CanadaDataProcess::process_dataset(
        uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = ds[["fa3f2917-6553-438c-9a6f-2af8d077f47f"]],
        prov = "MB"
      )
    ) %>%
      dplyr::group_by(.data$name, .data$province, .data$date) %>%
      dplyr::summarize(value = max(.data$value)) %>%
      dplyr::ungroup()

    ## vaccine_administration (prov)
    # note that this "topline" provincial total may exceed the sum of the HR totals
    # if the "topline" dashboard is updated one day but the sub-dashboards are not
    # the second information is only available in the sub-dashboards so in this case
    # the share of doses administered that are second doses will be underestimated
    mb_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "7a5ef226-2244-47e0-b964-28c2dc06d5ae",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = ds[["7a5ef226-2244-47e0-b964-28c2dc06d5ae"]]
    )

    ## vaccine_completion (prov)
    mb_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "a57dc10d-7139-4164-9042-eb2242716585",
      val = "vaccine_completion",
      fmt = "hr_cum_current",
      ds = ds[["a57dc10d-7139-4164-9042-eb2242716585"]]
    ) %>%
      process_agg2prov

    # NB

    ## cases (hr)
    nb_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4f194e3b-39fd-4fe0-b420-8cefa9001f7e",
      val = "cases",
      fmt = "hr_cum_current",
      ds = ds[["4f194e3b-39fd-4fe0-b420-8cefa9001f7e"]]
    ) %>%
      process_hr_names("NB")

    ## mortality (hr)
    nb_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4f194e3b-39fd-4fe0-b420-8cefa9001f7e",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = ds[["4f194e3b-39fd-4fe0-b420-8cefa9001f7e"]]
    ) %>%
      process_hr_names("NB")

    ## recovered (prov)
    nb_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4f194e3b-39fd-4fe0-b420-8cefa9001f7e",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = ds[["4f194e3b-39fd-4fe0-b420-8cefa9001f7e"]]
    )

    ## testing (prov)
    nb_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4f194e3b-39fd-4fe0-b420-8cefa9001f7e",
      val = "testing",
      fmt = "prov_cum_current",
      ds = ds[["4f194e3b-39fd-4fe0-b420-8cefa9001f7e"]]
    )

    ## vaccine_distribution (prov)
    nb_vaccine_distribution_prov <- dplyr::bind_rows(
      Covid19CanadaDataProcess::process_dataset(
        uuid = "18fd4390-8e47-43b4-b1c1-c48218a7859b",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = ds[["18fd4390-8e47-43b4-b1c1-c48218a7859b"]]
      ),
      Covid19CanadaDataProcess::process_dataset(
        uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = ds[["fa3f2917-6553-438c-9a6f-2af8d077f47f"]],
        prov = "NB"
      )
    ) %>%
      dplyr::group_by(.data$name, .data$province, .data$date) %>%
      dplyr::summarize(value = max(.data$value)) %>%
      dplyr::ungroup()

    ## vaccine_administration (prov)
    nb_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "18fd4390-8e47-43b4-b1c1-c48218a7859b",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = ds[["18fd4390-8e47-43b4-b1c1-c48218a7859b"]]
    )

    ## vaccine_completion (prov)
    nb_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "18fd4390-8e47-43b4-b1c1-c48218a7859b",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = ds[["18fd4390-8e47-43b4-b1c1-c48218a7859b"]]
    )

    # NL

    ## cases (hr)
    nl_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "f0e10f54-a4db-48d8-9c4e-8571e663ca28",
      val = "cases",
      fmt = "hr_cum_current",
      ds = ds[["f0e10f54-a4db-48d8-9c4e-8571e663ca28"]]
    ) %>%
      process_hr_names("NL")

    ## mortality (hr)
    nl_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "f0e10f54-a4db-48d8-9c4e-8571e663ca28",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = ds[["f0e10f54-a4db-48d8-9c4e-8571e663ca28"]]
    ) %>%
      process_hr_names("NL")

    ## recovered (prov)
    nl_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "8419f6f1-b80b-4247-84e5-6414ab0154d8",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = ds[["8419f6f1-b80b-4247-84e5-6414ab0154d8"]]
    )

    ## testing (prov)
    nl_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "8419f6f1-b80b-4247-84e5-6414ab0154d8",
      val = "testing",
      fmt = "prov_cum_current",
      ds = ds[["8419f6f1-b80b-4247-84e5-6414ab0154d8"]]
    )

    ## vaccine_distribution (prov)
    nl_vaccine_distribution_prov <- dplyr::bind_rows(
      Covid19CanadaDataProcess::process_dataset(
        uuid = "6d1b0f2e-0ac0-4ad8-a383-619065ec5a52",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = ds[["6d1b0f2e-0ac0-4ad8-a383-619065ec5a52"]]
      ),
      Covid19CanadaDataProcess::process_dataset(
        uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = ds[["fa3f2917-6553-438c-9a6f-2af8d077f47f"]],
        prov = "NL"
      )
    ) %>%
      dplyr::group_by(.data$name, .data$province, .data$date) %>%
      dplyr::summarize(value = max(.data$value)) %>%
      dplyr::ungroup()

    ## vaccine_administration (prov)
    nl_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "25ca2057-978c-49b8-a9e6-1a1f70732659",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = ds[["25ca2057-978c-49b8-a9e6-1a1f70732659"]]
    )

    ## vaccine_completion (prov)
    nl_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "25ca2057-978c-49b8-a9e6-1a1f70732659",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = ds[["25ca2057-978c-49b8-a9e6-1a1f70732659"]]
    )

    # NS

    ## cases (hr)
    ns_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "d0f05ef1-419f-4f4c-bc2d-17446c10059f",
      val = "cases",
      fmt = "hr_cum_current",
      ds = ds[["d0f05ef1-419f-4f4c-bc2d-17446c10059f"]]
    ) %>%
      process_hr_names("NS")

    ## mortality (hr)
    ns_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "d0f05ef1-419f-4f4c-bc2d-17446c10059f",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = ds[["d0f05ef1-419f-4f4c-bc2d-17446c10059f"]]
    ) %>%
      process_hr_names("NS")

    ## recovered (prov)
    ns_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "d0f05ef1-419f-4f4c-bc2d-17446c10059f",
      val = "recovered",
      fmt = "hr_cum_current",
      ds = ds[["d0f05ef1-419f-4f4c-bc2d-17446c10059f"]]
    ) %>%
      process_agg2prov

    ## testing (prov)
    ns_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0e7a1f46-5d31-4267-be97-831172fa7081",
      val = "testing",
      fmt = "prov_cum_current",
      ds = ds[["0e7a1f46-5d31-4267-be97-831172fa7081"]]
    )

    ## vaccine_distribution (prov)
    ns_vaccine_distribution_prov <- dplyr::bind_rows(
      Covid19CanadaDataProcess::process_dataset(
        uuid = "70214276-8616-488c-b53a-b514608e3146",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = ds[["70214276-8616-488c-b53a-b514608e3146"]]
      ),
      Covid19CanadaDataProcess::process_dataset(
        uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = ds[["fa3f2917-6553-438c-9a6f-2af8d077f47f"]],
        prov = "NS"
      )
    ) %>%
      dplyr::group_by(.data$name, .data$province, .data$date) %>%
      dplyr::summarize(value = max(.data$value)) %>%
      dplyr::ungroup()

    ## vaccine_administration (prov)
    ns_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "70214276-8616-488c-b53a-b514608e3146",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = ds[["70214276-8616-488c-b53a-b514608e3146"]]
    )

    ## vaccine_completion (prov)
    ns_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "7b7be246-cd65-4f35-b354-faa705cacecc",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = ds[["7b7be246-cd65-4f35-b354-faa705cacecc"]]
    )

    ## vaccine_additional_doses (prov)
    ns_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "7b7be246-cd65-4f35-b354-faa705cacecc",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = ds[["7b7be246-cd65-4f35-b354-faa705cacecc"]]
    )

    # NT

    ## cases (hr)
    nt_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "e008ba6f-7b09-4af4-afc5-63ec3c3bbfbb",
      val = "cases",
      fmt = "prov_cum_current",
      ds = ds[["e008ba6f-7b09-4af4-afc5-63ec3c3bbfbb"]]
    ) %>%
      process_prov2hr("NT")

    ## mortality (hr)
    nt_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "e008ba6f-7b09-4af4-afc5-63ec3c3bbfbb",
      val = "mortality",
      fmt = "prov_cum_current",
      ds = ds[["e008ba6f-7b09-4af4-afc5-63ec3c3bbfbb"]]
    ) %>%
      process_prov2hr("NT")

    ## recovered (prov)
    nt_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "e008ba6f-7b09-4af4-afc5-63ec3c3bbfbb",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = ds[["e008ba6f-7b09-4af4-afc5-63ec3c3bbfbb"]]
    )

    ## testing (prov)
    nt_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "66fbe91e-34c0-4f7f-aa94-cf6c14db0158",
      val = "testing",
      fmt = "prov_cum_current",
      ds = ds[["66fbe91e-34c0-4f7f-aa94-cf6c14db0158"]]
    )

    ## vaccine_distribution (prov)
    nt_vaccine_distribution_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
      val = "vaccine_distribution",
      fmt = "prov_cum_current",
      ds = ds[["fa3f2917-6553-438c-9a6f-2af8d077f47f"]],
      prov = "NT"
    )

    ## vaccine_administration (prov)
    nt_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "454de458-f7b4-4814-96a6-5a426f8c8c60",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = ds[["454de458-f7b4-4814-96a6-5a426f8c8c60"]]
    )

    ## vaccine_completion (prov)
    nt_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "454de458-f7b4-4814-96a6-5a426f8c8c60",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = ds[["454de458-f7b4-4814-96a6-5a426f8c8c60"]]
    )

    # NU

    ## cases (hr)
    nu_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "04ab3773-f535-42ad-8ee4-4d584ec23523",
      val = "cases",
      fmt = "prov_cum_current",
      ds = ds[["04ab3773-f535-42ad-8ee4-4d584ec23523"]]
    ) %>%
      process_prov2hr("NU")

    ## mortality (hr)
    nu_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "04ab3773-f535-42ad-8ee4-4d584ec23523",
      val = "mortality",
      fmt = "prov_cum_current",
      ds = ds[["04ab3773-f535-42ad-8ee4-4d584ec23523"]]
    ) %>%
      process_prov2hr("NU")

    ## recovered (prov)
    nu_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "04ab3773-f535-42ad-8ee4-4d584ec23523",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = ds[["04ab3773-f535-42ad-8ee4-4d584ec23523"]]
    )

    ## testing (prov)
    nu_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "04ab3773-f535-42ad-8ee4-4d584ec23523",
      val = "testing",
      fmt = "prov_cum_current",
      ds = ds[["04ab3773-f535-42ad-8ee4-4d584ec23523"]],
      testing_type = "n_people_tested"
    )

    ## vaccine_distribution (prov)
    nu_vaccine_distribution_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
      val = "vaccine_distribution",
      fmt = "prov_cum_current",
      ds = ds[["fa3f2917-6553-438c-9a6f-2af8d077f47f"]],
      prov = "NU"
    )

    ## vaccine_administration (prov)
    nu_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "04ab3773-f535-42ad-8ee4-4d584ec23523",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = ds[["04ab3773-f535-42ad-8ee4-4d584ec23523"]]
    )

    ## vaccine_completion (prov)
    nu_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "04ab3773-f535-42ad-8ee4-4d584ec23523",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = ds[["04ab3773-f535-42ad-8ee4-4d584ec23523"]]
    )

    # ON

    # ## cases (hr)
    # on_cases_hr <- Covid19CanadaDataProcess::process_dataset(
    #   uuid = "73fffd44-fbad-4de8-8d32-00cc5ae180a6",
    #   val = "cases",
    #   fmt = "hr_ts",
    #   ds = ds[["73fffd44-fbad-4de8-8d32-00cc5ae180a6"]]
    # ) %>%
    #   process_hr_names("ON", opt = "moh") %>%
    #   process_cum_current()
    #
    # ## mortality (hr)
    # on_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
    #   uuid = "73fffd44-fbad-4de8-8d32-00cc5ae180a6",
    #   val = "mortality",
    #   fmt = "hr_ts",
    #   ds = ds[["73fffd44-fbad-4de8-8d32-00cc5ae180a6"]]
    # ) %>%
    #   process_hr_names("ON", opt = "moh") %>%
    #   process_cum_current()
    #
    # ## recovered (prov)
    # on_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
    #   uuid = "73fffd44-fbad-4de8-8d32-00cc5ae180a6",
    #   val = "recovered",
    #   fmt = "hr_ts",
    #   ds = ds[["73fffd44-fbad-4de8-8d32-00cc5ae180a6"]]
    # ) %>%
    #   process_agg2prov() %>%
    #   process_cum_current()

    ## testing (prov)
    on_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "a8b1be1a-561a-47f5-9456-c553ea5b2279",
      val = "testing",
      fmt = "prov_cum_current",
      ds = ds[["a8b1be1a-561a-47f5-9456-c553ea5b2279"]]
    )

    ## vaccine_distribution (prov)
    on_vaccine_distribution_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
      val = "vaccine_distribution",
      fmt = "prov_cum_current",
      ds = ds[["fa3f2917-6553-438c-9a6f-2af8d077f47f"]],
      prov = "ON"
    )

    ## vaccine_administration (prov)
    on_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "170057c4-3231-4f15-9438-2165c5438dda",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = ds[["170057c4-3231-4f15-9438-2165c5438dda"]]
    )

    ## vaccine_completion (prov)
    on_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "170057c4-3231-4f15-9438-2165c5438dda",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = ds[["170057c4-3231-4f15-9438-2165c5438dda"]]
    )

    # PE

    ## cases (hr)
    pe_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455",
      val = "cases",
      fmt = "prov_cum_current",
      ds = ds[["68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455"]]
    ) %>%
      process_prov2hr("PE")

    ## mortality (hr)
    pe_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455",
      val = "mortality",
      fmt = "prov_cum_current",
      ds = ds[["68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455"]]
    ) %>%
      process_prov2hr("PE")

    ## recovered (prov)
    pe_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = ds[["68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455"]]
    )

    ## testing (prov)
    pe_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455",
      val = "testing",
      fmt = "prov_cum_current",
      ds = ds[["68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455"]]
    )

    # vaccine_distribution (prov)
    pe_vaccine_distribution_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
      val = "vaccine_distribution",
      fmt = "prov_cum_current",
      ds = ds[["fa3f2917-6553-438c-9a6f-2af8d077f47f"]],
      prov = "PE"
    )

    ## vaccine_administration (prov)
    pe_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "3ff94c42-8b12-4653-a6c9-0ddd8ff343d5",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = ds[["3ff94c42-8b12-4653-a6c9-0ddd8ff343d5-Total-Doses"]]
    )

    ## vaccine_completion (prov)
    pe_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "3ff94c42-8b12-4653-a6c9-0ddd8ff343d5",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = ds[["3ff94c42-8b12-4653-a6c9-0ddd8ff343d5-Fully-Immunized"]]
    )

    # QC

    ## cases (hr)
    qc_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0c577d5e-999e-42c5-b4c1-66b3787c3a04",
      val = "cases",
      fmt = "hr_cum_current",
      ds = ds[["0c577d5e-999e-42c5-b4c1-66b3787c3a04"]]
    ) %>%
      process_hr_names("QC") %>%
      dplyr::group_by(dplyr::across(c(-.data$value))) %>%
      dplyr::summarize(value = sum(.data$value), .groups = "drop")

    ## mortality (hr)
    qc_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0c577d5e-999e-42c5-b4c1-66b3787c3a04",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = ds[["0c577d5e-999e-42c5-b4c1-66b3787c3a04"]]
    ) %>%
      process_hr_names("QC") %>%
      dplyr::group_by(dplyr::across(c(-.data$value))) %>%
      dplyr::summarize(value = sum(.data$value), .groups = "drop")

    ## recovered (prov)
    qc_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "0c577d5e-999e-42c5-b4c1-66b3787c3a04",
      val = "recovered",
      fmt = "hr_cum_current",
      ds = ds[["0c577d5e-999e-42c5-b4c1-66b3787c3a04"]]
    ) %>%
      process_agg2prov

    ## testing (prov)
    qc_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5",
      val = "testing",
      fmt = "hr_ts",
      ds = ds[["3b93b663-4b3f-43b4-a23d-cbf6d149d2c5"]],
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
        ds = ds[["fa3f2917-6553-438c-9a6f-2af8d077f47f"]],
        prov = "QC"
      ),
      Covid19CanadaDataProcess::process_dataset(
        uuid = "aee3bd38-b782-4880-9033-db76f84cef5b",
        val = "vaccine_distribution",
        fmt = "prov_cum_current",
        ds = Covid19CanadaData::dl_dataset("aee3bd38-b782-4880-9033-db76f84cef5b", sep = ";")
      )
    ) %>%
      dplyr::group_by(.data$name, .data$province, .data$date) %>%
      dplyr::summarize(value = max(.data$value)) %>%
      dplyr::ungroup()

    ## vaccine_administration (prov)
    qc_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4e04442d-f372-4357-ba15-3b64f4e03fbe",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = ds[["4e04442d-f372-4357-ba15-3b64f4e03fbe"]]
    )

    ## vaccine_completion (prov)
    qc_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4e04442d-f372-4357-ba15-3b64f4e03fbe",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = ds[["4e04442d-f372-4357-ba15-3b64f4e03fbe"]]
    )

    ## vaccine_additional_doses (prov)
    qc_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4e04442d-f372-4357-ba15-3b64f4e03fbe",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = ds[["4e04442d-f372-4357-ba15-3b64f4e03fbe"]]
    )

    # SK

    ## cases (hr)
    sk_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "95de79d5-5e5c-45c2-bbab-41daf3dbee5d",
      val = "cases",
      fmt = "hr_cum_current",
      ds = ds[["95de79d5-5e5c-45c2-bbab-41daf3dbee5d"]]
    ) %>%
      process_hr_names("SK") %>%
      dplyr::group_by(dplyr::across(c(-.data$value))) %>%
      dplyr::summarize(value = sum(.data$value), .groups = "drop")

    ## mortality (hr)
    sk_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "95de79d5-5e5c-45c2-bbab-41daf3dbee5d",
      val = "mortality",
      fmt = "hr_cum_current",
      ds = ds[["95de79d5-5e5c-45c2-bbab-41daf3dbee5d"]]
    ) %>%
      process_hr_names("SK") %>%
      dplyr::group_by(dplyr::across(c(-.data$value))) %>%
      dplyr::summarize(value = sum(.data$value), .groups = "drop")

    ## recovered (prov)
    sk_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "95de79d5-5e5c-45c2-bbab-41daf3dbee5d",
      val = "recovered",
      fmt = "hr_cum_current",
      ds = ds[["95de79d5-5e5c-45c2-bbab-41daf3dbee5d"]]
    ) %>%
      process_agg2prov

    ## testing (prov)
    sk_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "9736bff9-4bd3-4c04-b9d9-87f60b3d5eb5",
      val = "testing",
      fmt = "hr_cum_current",
      ds = ds[["9736bff9-4bd3-4c04-b9d9-87f60b3d5eb5"]],
      testing_type = "n_people_tested"
    ) %>%
      process_agg2prov

    ## vaccine_distribution (prov)
    sk_vaccine_distribution_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
      val = "vaccine_distribution",
      fmt = "prov_cum_current",
      ds = ds[["fa3f2917-6553-438c-9a6f-2af8d077f47f"]],
      prov = "SK"
    )

    ## vaccine_administration (prov)
    sk_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "15556169-0471-49ea-926e-20b5e8dbd25d",
      val = "vaccine_administration",
      fmt = "hr_cum_current",
      ds = ds[["15556169-0471-49ea-926e-20b5e8dbd25d"]]
    ) %>%
      process_agg2prov

    ## vaccine_completion (prov)
    sk_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "15556169-0471-49ea-926e-20b5e8dbd25d",
      val = "vaccine_completion",
      fmt = "hr_cum_current",
      ds = ds[["15556169-0471-49ea-926e-20b5e8dbd25d"]]
    ) %>%
      process_agg2prov

    ## vaccine_additional_doses (prov)
    sk_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "28d7f978-9a7b-4933-a520-41b073868d05",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = ds[["28d7f978-9a7b-4933-a520-41b073868d05"]]
    )

    # YT

    ## cases (hr)
    yt_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e",
      val = "cases",
      fmt = "prov_cum_current",
      ds = ds[["4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e"]]
    ) %>%
      process_prov2hr("YT")

    ## mortality (hr)
    yt_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e",
      val = "mortality",
      fmt = "prov_cum_current",
      ds = ds[["4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e"]]
    ) %>%
      process_prov2hr("YT")

    ## recovered (prov)
    yt_recovered_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e",
      val = "recovered",
      fmt = "prov_cum_current",
      ds = ds[["4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e"]]
    )

    ## testing (prov)
    yt_testing_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e",
      val = "testing",
      fmt = "prov_cum_current",
      ds = ds[["4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e"]],
      testing_type = "n_people_tested"
    )

    ## vaccine_distribution (prov)
    yt_vaccine_distribution_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "fa3f2917-6553-438c-9a6f-2af8d077f47f",
      val = "vaccine_distribution",
      fmt = "prov_cum_current",
      ds = ds[["fa3f2917-6553-438c-9a6f-2af8d077f47f"]],
      prov = "YT"
    )

    ## vaccine_administration (prov)
    yt_vaccine_administration_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e",
      val = "vaccine_administration",
      fmt = "prov_cum_current",
      ds = ds[["4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e"]]
    )

    ## vaccine_completion (prov)
    yt_vaccine_completion_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e",
      val = "vaccine_completion",
      fmt = "prov_cum_current",
      ds = ds[["4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e"]]
    )

    ## vaccine_additional_doses (prov)
    yt_vaccine_additional_doses_prov <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e",
      val = "vaccine_additional_doses",
      fmt = "prov_cum_current",
      ds = ds[["4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e"]]
    )

  } else {

    # Algoma (ALG)

    ## cases (hr)
    alg_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "685df305-f6c7-4ac2-992b-ec707eb1f1cb",
      val = "cases",
      fmt = "hr_cum_current",
      hr = "Algoma",
      ds = ds[["685df305-f6c7-4ac2-992b-ec707eb1f1cb"]]
    )

    ## mortality (hr)
    alg_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "685df305-f6c7-4ac2-992b-ec707eb1f1cb",
      val = "mortality",
      fmt = "hr_cum_current",
      hr = "Algoma",
      ds = ds[["685df305-f6c7-4ac2-992b-ec707eb1f1cb"]]
    )

    ## recovered (hr)
    alg_recovered_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "685df305-f6c7-4ac2-992b-ec707eb1f1cb",
      val = "recovered",
      fmt = "hr_cum_current",
      hr = "Algoma",
      ds = ds[["685df305-f6c7-4ac2-992b-ec707eb1f1cb"]]
    )

    # Eastern (EOH)

    ## cases (hr)
    eoh_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "cd1db4e8-c4e5-4b24-86a5-2294281919c6",
      val = "cases",
      fmt = "hr_cum_current",
      hr = "Eastern",
      ds = ds[["cd1db4e8-c4e5-4b24-86a5-2294281919c6"]]
    )

    ## mortality (hr)
    eoh_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "cd1db4e8-c4e5-4b24-86a5-2294281919c6",
      val = "mortality",
      fmt = "hr_cum_current",
      hr = "Eastern",
      ds = ds[["cd1db4e8-c4e5-4b24-86a5-2294281919c6"]]
    )

    ## recovered (hr)
    eoh_recovered_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "cd1db4e8-c4e5-4b24-86a5-2294281919c6",
      val = "recovered",
      fmt = "hr_cum_current",
      hr = "Eastern",
      ds = ds[["cd1db4e8-c4e5-4b24-86a5-2294281919c6"]]
    )

    # Grey Bruce (GBH)

    ## cases (hr)
    gbh_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "eac45a46-e5b5-4e75-9393-77995cd7e219",
      val = "cases",
      fmt = "hr_cum_current",
      hr = "Grey Bruce",
      ds = ds[["eac45a46-e5b5-4e75-9393-77995cd7e219"]]
    )

    ## mortality (hr)
    gbh_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "eac45a46-e5b5-4e75-9393-77995cd7e219",
      val = "mortality",
      fmt = "hr_cum_current",
      hr = "Grey Bruce",
      ds = ds[["eac45a46-e5b5-4e75-9393-77995cd7e219"]]
    )

    ## recovered (hr)
    gbh_recovered_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "eac45a46-e5b5-4e75-9393-77995cd7e219",
      val = "recovered",
      fmt = "hr_cum_current",
      hr = "Grey Bruce",
      ds = ds[["eac45a46-e5b5-4e75-9393-77995cd7e219"]]
    )

    # Northwestern (NWR)

    ## cases (hr)
    nwr_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4c56a58b-0cb3-4d71-bafe-9fdb42e5c1d5",
      val = "cases",
      fmt = "hr_cum_current",
      hr = "Northwestern",
      ds = ds[["4c56a58b-0cb3-4d71-bafe-9fdb42e5c1d5"]]
    )

    ## mortality (hr)
    nwr_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4c56a58b-0cb3-4d71-bafe-9fdb42e5c1d5",
      val = "mortality",
      fmt = "hr_cum_current",
      hr = "Northwestern",
      ds = ds[["4c56a58b-0cb3-4d71-bafe-9fdb42e5c1d5"]]
    )

    ## recovered (hr)
    nwr_recovered_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4c56a58b-0cb3-4d71-bafe-9fdb42e5c1d5",
      val = "recovered",
      fmt = "hr_cum_current",
      hr = "Northwestern",
      ds = ds[["4c56a58b-0cb3-4d71-bafe-9fdb42e5c1d5"]]
    )

    # Ottawa (OTT)

    ## cases (hr)
    ott_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "d8d4cbc6-d0a5-4544-ad3e-5a3c3060f973",
      val = "cases",
      fmt = "hr_cum_current",
      hr = "Ottawa",
      ds = ds[["d8d4cbc6-d0a5-4544-ad3e-5a3c3060f973"]]
    )

    ## mortality (hr)
    ott_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "d8d4cbc6-d0a5-4544-ad3e-5a3c3060f973",
      val = "mortality",
      fmt = "hr_cum_current",
      hr = "Ottawa",
      ds = ds[["d8d4cbc6-d0a5-4544-ad3e-5a3c3060f973"]]
    )

    ## recovered (hr)
    ott_recovered_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "d8d4cbc6-d0a5-4544-ad3e-5a3c3060f973",
      val = "recovered",
      fmt = "hr_cum_current",
      hr = "Ottawa",
      ds = ds[["d8d4cbc6-d0a5-4544-ad3e-5a3c3060f973"]]
    )

    # Peterborough (PET)

    ## cases (hr)
    pet_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "c3aa6a5e-2ff5-4158-83ab-dcde251bc365",
      val = "cases",
      fmt = "hr_cum_current",
      hr = "Peterborough",
      ds = ds[["c3aa6a5e-2ff5-4158-83ab-dcde251bc365"]]
    )

    ## mortality (hr)
    pet_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "c3aa6a5e-2ff5-4158-83ab-dcde251bc365",
      val = "mortality",
      fmt = "hr_cum_current",
      hr = "Peterborough",
      ds = ds[["c3aa6a5e-2ff5-4158-83ab-dcde251bc365"]]
    )

    ## recovered (hr)
    pet_recovered_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "c3aa6a5e-2ff5-4158-83ab-dcde251bc365",
      val = "recovered",
      fmt = "hr_cum_current",
      hr = "Peterborough",
      ds = ds[["c3aa6a5e-2ff5-4158-83ab-dcde251bc365"]]
    )

    # Porcupine (PQP)

    ## cases (hr)
    pqp_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "00cc3ae2-7bf8-4074-81b7-8e06e91c947a",
      val = "cases",
      fmt = "hr_cum_current",
      hr = "Porcupine",
      ds = ds[["00cc3ae2-7bf8-4074-81b7-8e06e91c947a"]]
    )

    ## mortality (hr)
    pqp_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "00cc3ae2-7bf8-4074-81b7-8e06e91c947a",
      val = "mortality",
      fmt = "hr_cum_current",
      hr = "Porcupine",
      ds = ds[["00cc3ae2-7bf8-4074-81b7-8e06e91c947a"]]
    )

    ## recovered (hr)
    pqp_recovered_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "00cc3ae2-7bf8-4074-81b7-8e06e91c947a",
      val = "recovered",
      fmt = "hr_cum_current",
      hr = "Porcupine",
      ds = ds[["00cc3ae2-7bf8-4074-81b7-8e06e91c947a"]]
    )

    # Renfrew (REN)

    ## cases (hr)
    ren_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "688bf944-9be6-49c3-ae5d-848ae32bad92",
      val = "cases",
      fmt = "hr_cum_current",
      hr = "Renfrew",
      ds = ds[["688bf944-9be6-49c3-ae5d-848ae32bad92"]]
    )

    ## mortality (hr)
    ren_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "688bf944-9be6-49c3-ae5d-848ae32bad92",
      val = "mortality",
      fmt = "hr_cum_current",
      hr = "Renfrew",
      ds = ds[["688bf944-9be6-49c3-ae5d-848ae32bad92"]]
    )

    ## recovered (hr)
    ren_recovered_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "688bf944-9be6-49c3-ae5d-848ae32bad92",
      val = "recovered",
      fmt = "hr_cum_current",
      hr = "Renfrew",
      ds = ds[["688bf944-9be6-49c3-ae5d-848ae32bad92"]]
    )

    # Sudbury (SUD)

    ## cases (hr)
    sud_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4b9c88a2-9487-4632-adc5-cfd4a2fddb3f",
      val = "cases",
      fmt = "hr_cum_current",
      hr = "Sudbury",
      ds = ds[["4b9c88a2-9487-4632-adc5-cfd4a2fddb3f"]]
    )

    ## mortality (hr)
    sud_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4b9c88a2-9487-4632-adc5-cfd4a2fddb3f",
      val = "mortality",
      fmt = "hr_cum_current",
      hr = "Sudbury",
      ds = ds[["4b9c88a2-9487-4632-adc5-cfd4a2fddb3f"]]
    )

    ## recovered (hr)
    sud_recovered_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "4b9c88a2-9487-4632-adc5-cfd4a2fddb3f",
      val = "recovered",
      fmt = "hr_cum_current",
      hr = "Sudbury",
      ds = ds[["4b9c88a2-9487-4632-adc5-cfd4a2fddb3f"]]
    )

    # Timiskaming (TSK)

    ## cases (hr)
    tsk_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "9c7bbba4-33ba-493a-8ea1-4eedd5149bc0",
      val = "cases",
      fmt = "hr_cum_current",
      hr = "Timiskaming",
      ds = ds[["9c7bbba4-33ba-493a-8ea1-4eedd5149bc0"]]
    )

    ## mortality (hr)
    tsk_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "9c7bbba4-33ba-493a-8ea1-4eedd5149bc0",
      val = "mortality",
      fmt = "hr_cum_current",
      hr = "Timiskaming",
      ds = ds[["9c7bbba4-33ba-493a-8ea1-4eedd5149bc0"]]
    )

    ## recovered (hr)
    tsk_recovered_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "9c7bbba4-33ba-493a-8ea1-4eedd5149bc0",
      val = "recovered",
      fmt = "hr_cum_current",
      hr = "Timiskaming",
      ds = ds[["9c7bbba4-33ba-493a-8ea1-4eedd5149bc0"]]
    )

    # Toronto (TOR)

    ## cases (hr)
    tor_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "ebad185e-9706-44f4-921e-fc89d5cfa334",
      val = "cases",
      fmt = "hr_cum_current",
      hr = "Toronto",
      ds = ds[["ebad185e-9706-44f4-921e-fc89d5cfa334-Status"]]
    )

    ## mortality (hr)
    tor_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "ebad185e-9706-44f4-921e-fc89d5cfa334",
      val = "mortality",
      fmt = "hr_cum_current",
      hr = "Toronto",
      ds = ds[["ebad185e-9706-44f4-921e-fc89d5cfa334-Status"]]
    )

    ## recovered (hr)
    tor_recovered_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "ebad185e-9706-44f4-921e-fc89d5cfa334",
      val = "recovered",
      fmt = "hr_cum_current",
      hr = "Toronto",
      ds = ds[["ebad185e-9706-44f4-921e-fc89d5cfa334-Status"]]
    )

    # Windsor-Essex (WEK)

    ## cases (hr)
    wek_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "01574538-062f-4a41-9dd5-8fdb72a0fe03",
      val = "cases",
      fmt = "hr_cum_current",
      hr = "Windsor-Essex",
      ds = ds[["01574538-062f-4a41-9dd5-8fdb72a0fe03"]]
    )

    ## mortality (hr)
    wek_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "01574538-062f-4a41-9dd5-8fdb72a0fe03",
      val = "mortality",
      fmt = "hr_cum_current",
      hr = "Windsor-Essex",
      ds = ds[["01574538-062f-4a41-9dd5-8fdb72a0fe03"]]
    )

    ## recovered (hr)
    wek_recovered_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "01574538-062f-4a41-9dd5-8fdb72a0fe03",
      val = "recovered",
      fmt = "hr_cum_current",
      hr = "Windsor-Essex",
      ds = ds[["01574538-062f-4a41-9dd5-8fdb72a0fe03"]]
    )

    # York (YRK)

    ## cases (hr)
    yrk_cases_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "3821cc66-f88d-4f12-99ca-d36d368872cd",
      val = "cases",
      fmt = "hr_cum_current",
      hr = "York",
      ds = ds[["3821cc66-f88d-4f12-99ca-d36d368872cd"]]
    )

    ## mortality (hr)
    yrk_mortality_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "3821cc66-f88d-4f12-99ca-d36d368872cd",
      val = "mortality",
      fmt = "hr_cum_current",
      hr = "York",
      ds = ds[["3821cc66-f88d-4f12-99ca-d36d368872cd"]]
    )

    ## recovered (hr)
    yrk_recovered_hr <- Covid19CanadaDataProcess::process_dataset(
      uuid = "3821cc66-f88d-4f12-99ca-d36d368872cd",
      val = "recovered",
      fmt = "hr_cum_current",
      hr = "York",
      ds = ds[["3821cc66-f88d-4f12-99ca-d36d368872cd"]]
    )

    # Not Reported

    ## cases (hr)
    onnr_cases_hr <- data.frame(
      name = "cases",
      province = "ON",
      sub_region_1 = "Not Reported",
      date = Sys.Date(),
      value = 0
    )

    ## mortality (hr)
    onnr_mortality_hr <- onnr_cases_hr %>%
      dplyr::mutate(name = "mortality")

    ## recovered (hr)
    onnr_recovered_hr <- onnr_cases_hr %>%
      dplyr::mutate(name = "recovered")

  }

  # return all objects created by the function as a list
  rm(ds) # no longer needed
  values <- as.list(environment())
  values <- values[setdiff(names(values), names(formals()))]
  values <- rev(values) # alphabetical order
  return(values)

}
