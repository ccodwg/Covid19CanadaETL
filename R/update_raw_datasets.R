#' Load data from temporary directory
#' @param ds The list of datasets returned by \code{\link[Covid19CanadaETL]{dl_datasets}}.
#' @param ds_name The UUID of the dataset to load.
#' @param d The object already loaded into memory. If provided, all other arguments will be ignored.
#' @param skip_raw_update If TRUE, skip updating raw datasets and just assemble final datasets. Default: FALSE.
#'
#' @export
load_ds <- function(ds, ds_name, d) {
  tryCatch(
    {
      # check if name of object in memory has been provided
      if (!missing(d)) {
        return(d)
      } else {
        # check file
        f_name <- list.files(path = ds, pattern = ds_name)
        if (length(f_name) == 0) {
          stop("Dataset not found, returning NULL.")
        } else if (length(f_name) > 1) {
          stop("Dataset name matches multiple files, returning NULL.")
        }
        if (grepl(".*\\.html$", f_name)) {
          # if .html, use read_xml()
          xml2::read_html(file.path(ds, paste0(ds_name, ".html")))
        } else {
          # else, try readRDS
          suppressWarnings(readRDS(file.path(ds, paste0(ds_name, ".RData"))))
        }
      }
    },
    error = function(e) {
      print(e)
      cat("Error in load_ds:", ds_name, fill = TRUE)
      return(NULL)
    }
  )
}

#' Update active_ts datasets
#' @param ds The list of datasets returned by \code{\link[Covid19CanadaETL]{dl_datasets}}.
#' @export
update_active_ts <- function(ds) {
  cat("Updating active_ts datasets...", fill = TRUE)

  # active_ts - case data
  cat("Updating active_ts: case data", fill = TRUE)

  ## ab
  Covid19CanadaDataProcess::process_dataset(
    uuid = "3e4fd9ff-48f9-4de1-a48a-97fd33b68337",
    val = "cases",
    fmt = "hr_ts",
    ds = load_ds(ds, "3e4fd9ff-48f9-4de1-a48a-97fd33b68337")) |>
    write_ts("active_ts", "ab", "cases")
  Covid19CanadaDataProcess::process_dataset(
    uuid = "2a11bbcc-7b43-47d1-952d-437cdc9b2ffb",
    val = "cases",
    fmt = "prov_ts",
    ds = load_ds(ds, "2a11bbcc-7b43-47d1-952d-437cdc9b2ffb")) |>
    write_ts("active_ts", "ab", "cases")

  ## can
  Covid19CanadaDataProcess::process_dataset(
    uuid = "314c507d-7e48-476e-937b-965499f51e8e",
    val = "cases",
    fmt = "prov_ts",
    ds = load_ds(ds, "314c507d-7e48-476e-937b-965499f51e8e")) %>%
    write_ts("active_ts", "can", "cases")

  ## nl
  Covid19CanadaDataProcess::process_dataset(
    uuid = "b19daaca-6b32-47f5-944f-c69eebd63c07",
    val = "cases",
    fmt = "prov_ts",
    ds = load_ds(ds, "b19daaca-6b32-47f5-944f-c69eebd63c07")) %>%
    write_ts("active_ts", "nl", "cases")

  ## on
  Covid19CanadaDataProcess::process_dataset(
    uuid = "921649fa-c6c0-43af-a112-23760da4d622",
    val = "cases",
    fmt = "hr_ts",
    ds = load_ds(ds, "921649fa-c6c0-43af-a112-23760da4d622")) %>%
    write_ts("active_ts", "on", "cases")

  ## qc
  Covid19CanadaDataProcess::process_dataset(
    uuid = "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5",
    val = "cases",
    fmt = "hr_ts",
    ds = load_ds(ds, "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5")) %>%
    write_ts("active_ts", "qc", "cases")

  # active_ts - death data
  cat("Updating active_ts: death data", fill = TRUE)

  ## ab
  Covid19CanadaDataProcess::process_dataset(
    uuid = "e477791b-bced-4b20-b40b-f8d7629c9b69",
    val = "mortality",
    fmt = "prov_ts",
    ds = load_ds(ds, "e477791b-bced-4b20-b40b-f8d7629c9b69")) %>%
    add_name_col("deaths") %>%
    write_ts("active_ts", "ab", "deaths")

  ## can
  Covid19CanadaDataProcess::process_dataset(
    uuid = "314c507d-7e48-476e-937b-965499f51e8e",
    val = "mortality",
    fmt = "prov_ts",
    ds = load_ds(ds, "314c507d-7e48-476e-937b-965499f51e8e")) %>%
    add_name_col("deaths")  %>%
    write_ts("active_ts", "can", "deaths")

  ## on
  Covid19CanadaDataProcess::process_dataset(
    uuid = "75dd0545-f728-4af5-8185-4269596785ef",
    val = "mortality",
    fmt = "prov_ts",
    ds = load_ds(ds, "75dd0545-f728-4af5-8185-4269596785ef")) %>%
    add_name_col("deaths") %>%
    write_ts("active_ts", "on", "deaths")

  ## qc
  Covid19CanadaDataProcess::process_dataset(
    uuid = "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5",
    val = "mortality",
    fmt = "hr_ts",
    ds = load_ds(ds, "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5")) %>%
    add_name_col("deaths") %>%
    write_ts("active_ts", "qc", "deaths")

  # active_ts - hospitalization data
  cat("Updating active_ts: hospitalization data", fill = TRUE)

  ## can
  Covid19CanadaDataProcess::process_dataset(
    uuid = "f1e1a857-fab8-4c25-a132-f474fab93622",
    val = "hospitalizations",
    fmt = "can_ts",
    ds = load_ds(ds, "f1e1a857-fab8-4c25-a132-f474fab93622")) %>%
    write_ts("active_ts", "can", "hospitalizations")

  ## on
  Covid19CanadaDataProcess::process_dataset(
    uuid = "4b214c24-8542-4d26-a850-b58fc4ef6a30",
    val = "hospitalizations",
    fmt = "prov_ts",
    ds = load_ds(ds, "4b214c24-8542-4d26-a850-b58fc4ef6a30")) %>%
    write_ts("active_ts", "on", "hospitalizations")

  ## qc
  Covid19CanadaDataProcess::process_dataset(
    uuid = "f0c25e20-2a6c-4f9a-adc3-61b28ab97245",
    val = "hospitalizations",
    fmt = "prov_ts",
    ds = load_ds(ds, "f0c25e20-2a6c-4f9a-adc3-61b28ab97245")) %>%
    write_ts("active_ts", "qc", "hospitalizations")

  # active_ts - icu data
  cat("Updating active_ts: icu data", fill = TRUE)

  ## can
  Covid19CanadaDataProcess::process_dataset(
    uuid = "f1e1a857-fab8-4c25-a132-f474fab93622",
    val = "icu",
    fmt = "can_ts",
    ds = load_ds(ds, "f1e1a857-fab8-4c25-a132-f474fab93622")) %>%
    write_ts("active_ts", "can", "icu")

  ## on
  Covid19CanadaDataProcess::process_dataset(
    uuid = "4b214c24-8542-4d26-a850-b58fc4ef6a30",
    val = "icu",
    fmt = "prov_ts",
    ds = load_ds(ds, "4b214c24-8542-4d26-a850-b58fc4ef6a30")) %>%
    write_ts("active_ts", "on", "icu")

  ## qc
  Covid19CanadaDataProcess::process_dataset(
    uuid = "f0c25e20-2a6c-4f9a-adc3-61b28ab97245",
    val = "icu",
    fmt = "prov_ts",
    ds = load_ds(ds, "f0c25e20-2a6c-4f9a-adc3-61b28ab97245")) %>%
    write_ts("active_ts", "qc", "icu")

  # active_ts - hosp_admissions data
  cat("Updating active_ts: hosp admissions data", fill = TRUE)

  ## ab
  Covid19CanadaDataProcess::process_dataset(
    uuid = "e477791b-bced-4b20-b40b-f8d7629c9b69",
    val = "hosp_admissions",
    fmt = "prov_ts",
    ds = load_ds(ds, "e477791b-bced-4b20-b40b-f8d7629c9b69")) %>%
    write_ts("active_ts", "ab", "hosp_admissions")

  ## qc
  Covid19CanadaDataProcess::process_dataset(
    uuid = "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5",
    val = "hosp_admissions",
    fmt = "prov_ts",
    ds = load_ds(ds, "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5")) %>%
    write_ts("active_ts", "qc", "hosp_admissions")

  # active_ts - icu_admissions data
  cat("Updating active_ts: icu admissions data", fill = TRUE)

  ## ab
  Covid19CanadaDataProcess::process_dataset(
    uuid = "e477791b-bced-4b20-b40b-f8d7629c9b69",
    val = "icu_admissions",
    fmt = "prov_ts",
    ds = load_ds(ds, "e477791b-bced-4b20-b40b-f8d7629c9b69")) %>%
    write_ts("active_ts", "ab", "icu_admissions")

  ## qc
  Covid19CanadaDataProcess::process_dataset(
    uuid = "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5",
    val = "icu_admissions",
    fmt = "prov_ts",
    ds = load_ds(ds, "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5")) %>%
    write_ts("active_ts", "qc", "icu_admissions")

  # active_ts - testing data
  cat("Updating active_ts: testing data", fill = TRUE)

  ## ab
  Covid19CanadaDataProcess::process_dataset(
    uuid = "7724f4fe-6759-4c9c-be6f-91b29a76631d",
    val = "testing",
    fmt = "prov_ts",
    ds = load_ds(ds, "7724f4fe-6759-4c9c-be6f-91b29a76631d")) %>%
    add_name_col("tests_completed") %>%
    write_ts("active_ts", "ab", "tests_completed")

  ## can
  Covid19CanadaDataProcess::process_dataset(
    uuid = "e41c63ec-ac54-47c9-8cf3-da2e1146aa75",
    val = "testing",
    fmt = "prov_ts",
    ds = load_ds(ds, "e41c63ec-ac54-47c9-8cf3-da2e1146aa75")) %>%
    add_name_col("tests_completed") %>%
    write_ts("active_ts", "can", "tests_completed")

  # vaccine coverage data
  cat("Updating active_ts: vaccine coverage data", fill = TRUE)
  for (dose in 1:4) {
    Covid19CanadaDataProcess::process_dataset(
      uuid = "d0bfcd85-9552-47a5-a699-aa6fe4815e00",
      val = paste0("vaccine_coverage_dose_", dose),
      fmt = "prov_ts",
      ds = load_ds(ds, "d0bfcd85-9552-47a5-a699-aa6fe4815e00")) %>%
      write_ts("active_ts", "can", paste0("vaccine_coverage_dose_", dose))
  }

  # vaccine administration data
  cat("Updating active_ts: vaccine administration data", fill = TRUE)

  ## define doses
  doses <- c("total_doses", "dose_1", "dose_2", "dose_3", "dose_4", "dose_5plus")

  ## can
  for (dose in doses) {
    Covid19CanadaDataProcess::process_dataset(
      uuid = "194a0002-5ad1-4016-8788-e7a216216a92",
      val = paste0("vaccine_administration_", dose),
      fmt = "prov_ts",
      ds = load_ds(ds, "194a0002-5ad1-4016-8788-e7a216216a92")) %>%
      write_ts("active_ts", "can", paste0("vaccine_administration_", dose))
  }

  ## qc
  for (dose in doses) {
    Covid19CanadaDataProcess::process_dataset(
      uuid = "4e04442d-f372-4357-ba15-3b64f4e03fbe",
      val = paste0("vaccine_administration_", dose),
      fmt = "prov_ts",
      ds = load_ds(ds, "4e04442d-f372-4357-ba15-3b64f4e03fbe")) %>%
      write_ts("active_ts", "qc", paste0("vaccine_administration_", dose))
  }

  # wastewater data

  ## can
  Covid19CanadaDataProcess::process_dataset(
    uuid = "ea3718c1-83f1-46a1-8b21-e25aebd1ebee",
    val = "wastewater_copies_per_ml",
    fmt = "subhr_ts",
    ds = load_ds(ds, "ea3718c1-83f1-46a1-8b21-e25aebd1ebee")) %>%
    write_ts("active_ts", "can", "wastewater_copies_per_ml")
}

#' Update reports datasets
#' @export
update_reports <- function() {
  cat("Updating reports datasets...", fill = TRUE)

  # actively updated reports
  sync_report("bc_monthly_report", "bc", "hr")
  sync_report("bc_monthly_report_testing", "bc", "hr")
  sync_report("bc_monthly_report_cumulative", "bc", "hr")
  sync_report("mb_weekly_report_2", "mb", "hr")
  sync_report("nb_weekly_report_3", "nb", "hr")
  sync_report("ns_monthly_report", "ns", "hr")
  sync_report("sk_crisp_report", "sk", "hr")
  sync_report("on_pho_cases", "on", "hr")
  sync_report("on_pho_outcomes", "on", "hr")
  sync_report("on_pho_testing", "on", "hr")
  sync_report("on_pho_vaccine_doses", "on", "hr")
  sync_report("on_pho_vaccine_coverage", "on", "hr")

  # no longer updated reports
  # sync_report("mb_weekly_report", "mb", "hr")
  # sync_report("nb_weekly_report", "nb", "hr")
  # sync_report("nb_weekly_report_2", "nb", "hr")
  # sync_report("ns_daily_news_release", "ns", "hr")
  # sync_report("ns_weekly_report", "ns", "hr")
  # sync_report("pe_daily_news_release", "pe", "hr")
  # sync_report("sk_monthly_report", "sk", "hr")
  # sync_report("sk_weekly_report", "sk", "hr")
}

#' Update covid19tracker.ca datasets
#' @export
update_covid19tracker <- function() {
  cat("Updating covid19tracker.ca datasets...", fill = TRUE)
  update_covid19tracker_dataset("hospitalizations")
  update_covid19tracker_dataset("icu")
}

#' Update active_cumul datasets
#' @param ds The list of datasets returned by \code{\link[Covid19CanadaETL]{dl_datasets}}.
#' @export
update_active_cumul <- function(ds) {
  cat("Updating active_cumul datasets...", fill = TRUE)

  # load Google Drive folder for COVID-19 data
  files <- googledrive::drive_ls(googledrive::as_id("10uX8h0GHcf3tleH-9i0E9mX0e8hXEOl5"))

  # active_cumul - death data
  cat("Updating active_cumul: death data", fill = TRUE)

  # active_cumul - death data - ab
  ac_deaths_hr_ab <- Covid19CanadaDataProcess::process_dataset(
    uuid = "2a11bbcc-7b43-47d1-952d-437cdc9b2ffb",
    val = "mortality",
    fmt = "hr_cum_current",
    ds = load_ds(ds, "2a11bbcc-7b43-47d1-952d-437cdc9b2ffb")) %>%
    convert_hr_names() %>%
    add_as_of_date(
      as_of_date = load_ds(ds, "2a11bbcc-7b43-47d1-952d-437cdc9b2ffb") |>
        rvest::html_elements(".goa-callout") %>%
        .[2] %>%
        rvest::html_text2() |>
        stringr::str_extract("(?<=up-to-date as(?: of)? )((January|February|March|April|May|June|July|August|September|October|November|December) \\d{1,2}, \\d{4})") |>
        as.Date(format = "%B %d, %Y"))
  upload_active_cumul(ac_deaths_hr_ab, files, "covid19_cumul", "deaths_hr_ab_2")
  sync_active_cumul("deaths_hr_ab_2", "deaths", "AB", as_of_date = TRUE)
}

#' Update raw datasets for CovidTimelineCanada
#'
#' @importFrom rlang .data
#'
#' @export
update_raw_datasets <- function(skip_raw_update = FALSE) {

  # announce start
  if (skip_raw_update) {
    cat("Skipping update of raw datasets...", fill = TRUE)
    return(invisible(NULL))
  } else {
    cat("Updating raw datasets...", fill = TRUE)
  }

  # define constants

  ## define provinces/territories
  pt <- c(
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT")

  # download datasets
  ds <- dl_datasets()

  # open sink for error messages
  e <- tempfile()
  ef <- file(e, open = "wt")
  sink(file = ef, type = "message")

  # update active_ts datasets
  update_active_ts(ds)

  # update reports datasets
  update_reports()

  # update covid19tracker.ca datasets
  update_covid19tracker()

  # update active_cumul datasets
  update_active_cumul(ds)

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

}
