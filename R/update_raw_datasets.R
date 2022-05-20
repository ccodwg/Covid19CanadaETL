#' Update raw datasets for CovidTimelineCanada
#'
#' @importFrom rlang .data
#'
#' @export
update_raw_datasets <- function() {

  # announce start
  cat("Updating raw datasets...", fill = TRUE)

  # define constants

  ## define provinces/territories
  pt <- c(
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT")

  # load Google Drive folder for COVID-19 data
  files <- googledrive::drive_ls(googledrive::as_id("10uX8h0GHcf3tleH-9i0E9mX0e8hXEOl5"))

  # download datasets
  ds <- dl_datasets()

  # function: load data from temporary directory
  load_ds <- function(ds, ds_name) {
    tryCatch(
      {
        # check file
        f_name <- list.files(path = ds, pattern = ds_name)
        if (length(f_name) == 0) {
          stop("Dataset not found")
        } else if (length(f_name) > 1) {
          stop("Dataset name matches multiple files")
        }
        if (grepl(".*\\.html$", f_name)) {
          # if .html, use read_xml()
          xml2::read_html(file.path(ds, paste0(ds_name, ".html")))
        } else {
          # else, try readRDS
          suppressWarnings(readRDS(file.path(ds, paste0(ds_name, ".RData"))))
        }
      },
      error = function(e) {
        print(e)
        cat("Error in load_ds:", ds_name, fill = TRUE)
      }
    )
  }

  # open sink for error messages
  e <- tempfile()
  ef <- file(e, open = "wt")
  sink(file = ef, type = "message")

  # update active_ts datasets
  cat("Updating active_ts datasets...", fill = TRUE)

  # active_ts - case data
  cat("Updating active_ts: case data", fill = TRUE)

  ## ab
  Covid19CanadaDataProcess::process_dataset(
    uuid = "59da1de8-3b4e-429a-9e18-b67ba3834002",
    val = "cases",
    fmt = "hr_ts",
    ds = load_ds(ds, "59da1de8-3b4e-429a-9e18-b67ba3834002")) %>%
    write_ts("active_ts", "ab", "cases")

  ## bc
  Covid19CanadaDataProcess::process_dataset(
    uuid = "ab6abe51-c9b1-4093-b625-93de1ddb6302",
    val = "cases",
    fmt = "hr_ts",
    ds = load_ds(ds, "ab6abe51-c9b1-4093-b625-93de1ddb6302")) %>%
    write_ts("active_ts", "bc", "cases")

  ## can
  Covid19CanadaDataProcess::process_dataset(
    uuid = "f7db31d0-6504-4a55-86f7-608664517bdb",
    val = "cases",
    fmt = "prov_ts",
    ds = load_ds(ds, "f7db31d0-6504-4a55-86f7-608664517bdb")) %>%
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
    uuid = "73fffd44-fbad-4de8-8d32-00cc5ae180a6",
    val = "cases",
    fmt = "hr_ts",
    ds = load_ds(ds, "73fffd44-fbad-4de8-8d32-00cc5ae180a6")) %>%
    write_ts("active_ts", "on", "cases")

  ## qc
  Covid19CanadaDataProcess::process_dataset(
    uuid = "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5",
    val = "cases",
    fmt = "hr_ts",
    ds = load_ds(ds, "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5")) %>%
    write_ts("active_ts", "qc", "cases")

  ## yt
  Covid19CanadaDataProcess::process_dataset(
    uuid = "8eb9a22f-a2c0-4bdb-8f6c-ef8134901efe",
    val = "cases",
    fmt = "prov_ts",
    ds = load_ds(ds, "8eb9a22f-a2c0-4bdb-8f6c-ef8134901efe")) %>%
    write_ts("active_ts", "yt", "cases")

  # active_ts - death data
  cat("Updating active_ts: death data", fill = TRUE)

  ## can
  Covid19CanadaDataProcess::process_dataset(
    uuid = "f7db31d0-6504-4a55-86f7-608664517bdb",
    val = "mortality",
    fmt = "prov_ts",
    ds = load_ds(ds, "f7db31d0-6504-4a55-86f7-608664517bdb")) %>%
    add_name_col("deaths")  %>%
    write_ts("active_ts", "can", "deaths")

  ## on
  on1 <- Covid19CanadaDataProcess::process_dataset(
    uuid = "73fffd44-fbad-4de8-8d32-00cc5ae180a6",
    val = "mortality",
    fmt = "hr_ts",
    ds = load_ds(ds, "73fffd44-fbad-4de8-8d32-00cc5ae180a6")) %>%
    add_name_col("deaths")
  # fix obvious error
  tryCatch(
    {
      on1[on1$sub_region_1 == "TORONTO" &
            on1$date == as.Date("2021-02-02"), "value"] <- on1[
              on1$sub_region_1 == "TORONTO" &
                on1$date == as.Date("2021-02-01"), "value"]
    },
    error = function(e) {
      print(e)
      cat("Error in processing pipeline", fill = TRUE)
    }
  )
  write_ts(on1, "active_ts", "on", "deaths")
  rm(on1)

  ## qc
  Covid19CanadaDataProcess::process_dataset(
    uuid = "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5",
    val = "mortality",
    fmt = "hr_ts",
    ds = load_ds(ds, "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5")) %>%
    add_name_col("deaths")  %>%
    write_ts("active_ts", "qc", "deaths")

  # active_ts - hospitalization data
  cat("Updating active_ts: hospitalization data", fill = TRUE)

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

  # testing data
  cat("Updating active_ts: testing data", fill = TRUE)
  Covid19CanadaDataProcess::process_dataset(
    uuid = "f7db31d0-6504-4a55-86f7-608664517bdb",
    val = "testing",
    fmt = "prov_ts",
    testing_type = "n_tests_completed",
    ds = load_ds(ds, "f7db31d0-6504-4a55-86f7-608664517bdb")) %>%
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
  for (dose in 1:3) {
    Covid19CanadaDataProcess::process_dataset(
      uuid = "d0bfcd85-9552-47a5-a699-aa6fe4815e00",
      val = paste0("vaccine_administration_dose_", dose),
      fmt = "prov_ts",
      ds = load_ds(ds, "d0bfcd85-9552-47a5-a699-aa6fe4815e00")) %>%
      write_ts("active_ts", "can", paste0("vaccine_administration_dose_", dose))
  }
  Covid19CanadaDataProcess::process_dataset(
    uuid = "d0bfcd85-9552-47a5-a699-aa6fe4815e00",
    val = "vaccine_administration_total_doses",
    fmt = "prov_ts",
    ds = load_ds(ds, "d0bfcd85-9552-47a5-a699-aa6fe4815e00")) %>%
    write_ts("active_ts", "can", "vaccine_administration_total_doses")

  # update reports datasets
  cat("Updating reports datasets...", fill = TRUE)

  ## actively updated reports
  sync_report("mb_weekly_report", "mb", "hr")
  sync_report("nb_weekly_report", "nb", "hr")
  sync_report("ns_weekly_report", "ns", "hr")
  sync_report("sk_weekly_report", "sk", "hr")

  ## no longer updated reports
  # sync_report("ns_daily_news_release", "ns", "hr")
  # sync_report("pe_daily_news_release", "pe", "hr")

  # update covid19tracker.ca datasets
  cat("Updating covid19tracker.ca datasets...", fill = TRUE)
  update_covid19tracker("hospitalizations")
  update_covid19tracker("icu")

  # update active_cumul datasets
  cat("Updating active_cumul datasets...", fill = TRUE)

  # active_cumul - death data
  cat("Updating active_cumul: death data", fill = TRUE)
  ac_deaths_hr <- list()

  ## ab
  ac_deaths_hr[["ab"]] <- Covid19CanadaDataProcess::process_dataset(
    uuid = "d3b170a7-bb86-4bb0-b362-2adc5e6438c2",
    val = "mortality",
    fmt = "hr_cum_current",
    ds = load_ds(ds, "d3b170a7-bb86-4bb0-b362-2adc5e6438c2")) %>%
    convert_hr_names()

  ## bc
  ac_deaths_hr[["bc"]] <- Covid19CanadaDataProcess::process_dataset(
    uuid = "91367e1d-8b79-422c-b314-9b3441ba4f42",
    val = "mortality",
    fmt = "hr_cum_current",
    ds = load_ds(ds, "91367e1d-8b79-422c-b314-9b3441ba4f42")) %>%
    drop_sub_regions("Out of Canada") %>%
    convert_hr_names()

  ## nl
  ac_deaths_hr[["nl"]] <- Covid19CanadaDataProcess::process_dataset(
    uuid = "34f45670-34ed-415c-86a6-e14d77fcf6db",
    val = "mortality",
    fmt = "hr_cum_current",
    ds = load_ds(ds, "34f45670-34ed-415c-86a6-e14d77fcf6db")) %>%
    convert_hr_names()

  ## upload
  upload_active_cumul(ac_deaths_hr, files, "covid19_cumul", "deaths_hr")

  ## sync and write
  sync_active_cumul("deaths_hr", "deaths", c("AB", "BC", "NL"))

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
