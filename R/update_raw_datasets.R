#' Load data from temporary directory
#' @param ds The list of datasets returned by \code{\link[Covid19CanadaETL]{dl_datasets}}.
#' @param ds_name The UUID of the dataset to load.
#' @param d The object already loaded into memory. If provided, all other arguments will be ignored.
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

  ## download and process data from PHO's Ontario COVID-19 Data Tool
  tryCatch(
    {
      pho_data <- Covid19CanadaDataProcess::process_pho_data_tool()
    },
    error = function(e) {
      print(e)
      cat(
        "Error in downloading and processing data from Ontario COVID-19 Data Tool",
        fill = TRUE)
    }
  )

  ## ab
  Covid19CanadaDataProcess::process_dataset(
    uuid = "59da1de8-3b4e-429a-9e18-b67ba3834002",
    val = "cases",
    fmt = "hr_ts",
    ds = load_ds(ds, "59da1de8-3b4e-429a-9e18-b67ba3834002")) %>%
    write_ts("active_ts", "ab", "cases")

  ## bc
  lapply(c("a8637b6c-babf-48cd-aeab-2f38c713f596", # up to 2022-10-08
           "878c0e41-ec21-4bd5-87e8-3b4a5969de84", # 2022-10-09 and later
           "f7cd5492-f23b-45a5-9d9b-118ac2b47529", # up to 2022-10-08
           "29c5a1e0-2f4d-409d-b10a-d6a62caad835", # 2022-10-09 and later
           "1ad7ef1b-1b02-4d5c-aec2-4923ea100e97", # up to 2022-10-08
           "635e4440-a4a3-457b-ac0f-7511f567afda", # 2022-10-09 and later
           "89b48da6-bed9-4cd4-824c-8b6d82ffba24", # up to 2022-10-08
           "2111db2e-f894-40ad-b7ad-aeea0c851a51", # 2022-10-09 and later
           "def3aca2-3595-4d70-a5d2-d51f78912dda", # up to 2022-10-08
           "b8aa2bec-cad8-45cf-901b-79f9f9aad545", # 2022-10-09 and later
           "c0ab9514-92ea-4dda-b714-bab9985e58be", # up to 2022-10-08
           "f056e795-1502-43f2-b87d-603aac0edf05" # 2022-10-09 and later
           ),
         function(uuid) {
           Covid19CanadaDataProcess::process_dataset(
             uuid = uuid,
             val = "cases",
             fmt = "hr_ts",
             ds = load_ds(ds, uuid))
           }) %>%
    dplyr::bind_rows() %>%
    dplyr::arrange(.data$name, .data$date, .data$sub_region_1) %>%
    dplyr::group_by(.data$name, .data$region, .data$sub_region_1) %>%
    dplyr::mutate(value = cumsum(.data$value)) %>%
    dplyr::ungroup() %>%
    write_ts("active_ts", "bc", "cases")

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

  ## ns
  Covid19CanadaDataProcess::process_dataset(
    uuid = "369d8d34-c8f7-4594-8e5c-7c4ccc00a7a4",
    val = "cases",
    fmt = "prov_ts",
    ds = load_ds(ds, "369d8d34-c8f7-4594-8e5c-7c4ccc00a7a4")) %>%
    write_ts("active_ts", "ns", "cases")

  ## on
  load_ds(d = pho_data[["cases_on"]]) %>%
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

  ## can
  Covid19CanadaDataProcess::process_dataset(
    uuid = "314c507d-7e48-476e-937b-965499f51e8e",
    val = "mortality",
    fmt = "prov_ts",
    ds = load_ds(ds, "314c507d-7e48-476e-937b-965499f51e8e")) %>%
    add_name_col("deaths")  %>%
    write_ts("active_ts", "can", "deaths")

  ## on
  load_ds(d = pho_data[["deaths_on"]]) %>%
    write_ts("active_ts", "on", "deaths")

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

  # active_ts - testing data
  cat("Updating active_ts: testing data", fill = TRUE)

  ## ab
  Covid19CanadaDataProcess::process_dataset(
    uuid = "24a572ea-0de3-4f83-b9b7-8764ea203eb6",
    val = "testing",
    fmt = "prov_ts",
    ds = load_ds(ds, "24a572ea-0de3-4f83-b9b7-8764ea203eb6")) %>%
    add_name_col("tests_completed") %>%
    write_ts("active_ts", "ab", "tests_completed")

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
  for (dose in 1:4) {
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
  sync_report("mb_weekly_report_2", "mb", "hr")
  sync_report("nb_weekly_report_2", "nb", "hr")
  sync_report("ns_weekly_report", "ns", "hr")
  sync_report("sk_crisp_report", "sk", "hr")

  # no longer updated reports
  # sync_report("mb_weekly_report", "mb", "hr")
  # sync_report("nb_weekly_report", "nb", "hr")
  # sync_report("ns_daily_news_release", "ns", "hr")
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
    uuid = "59da1de8-3b4e-429a-9e18-b67ba3834002",
    val = "mortality",
    fmt = "hr_cum_current",
    ds = load_ds(ds, "59da1de8-3b4e-429a-9e18-b67ba3834002")) %>%
    convert_hr_names() %>%
    add_as_of_date(
      as_of_date = max(as.Date(load_ds(ds, "59da1de8-3b4e-429a-9e18-b67ba3834002")[["Date.reported"]]), na.rm = TRUE))
  upload_active_cumul(ac_deaths_hr_ab, files, "covid19_cumul", "deaths_hr_ab")
  sync_active_cumul("deaths_hr_ab", "deaths", "AB", as_of_date = TRUE)

  # active_cumul - death data - bc
  ac_deaths_hr_bc <- Covid19CanadaDataProcess::process_dataset(
    uuid = "91367e1d-8b79-422c-b314-9b3441ba4f42",
    val = "mortality",
    fmt = "hr_cum_current",
    ds = load_ds(ds, "91367e1d-8b79-422c-b314-9b3441ba4f42")) %>%
    drop_sub_regions("Out of Canada") %>%
    convert_hr_names() %>%
    add_as_of_date(
      as_of_date = as.Date(as.POSIXct(load_ds(ds, "4f9dc8b7-7b42-450e-a741-a0f6a621d2af")$features$attributes[1, 1] / 1000, origin = "1970-01-01", tz = "America/Vancouver")))
  upload_active_cumul(ac_deaths_hr_bc, files, "covid19_cumul", "deaths_hr_bc")
  sync_active_cumul("deaths_hr_bc", "deaths", "BC", as_of_date = TRUE)

  # active_cumul - death data - nl
  ac_deaths_hr_nl <- Covid19CanadaDataProcess::process_dataset(
    uuid = "34f45670-34ed-415c-86a6-e14d77fcf6db",
    val = "mortality",
    fmt = "hr_cum_current",
    ds = load_ds(ds, "34f45670-34ed-415c-86a6-e14d77fcf6db")) %>%
    convert_hr_names() %>%
    add_as_of_date(
      as_of_date = lubridate::date(lubridate::with_tz(as.POSIXct(load_ds(ds, "34f45670-34ed-415c-86a6-e14d77fcf6db")$features$attributes$EditDate / 1000, origin = "1970-01-01")[1], tz = "America/St_Johns")))
  upload_active_cumul(ac_deaths_hr_nl, files, "covid19_cumul", "deaths_hr_nl")
  sync_active_cumul("deaths_hr_nl", "deaths", "NL", as_of_date = TRUE)
}

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
