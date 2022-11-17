#' Download datasets required to update raw datasets for CovidTimelineCanada
#'
#' The live version of each required dataset is downloaded from a URL using the
#' function \code{\link[Covid19CanadaData]{dl_dataset}}. Datasets are saved as
#' .RData files and HTML files to a temporary directory. Datasets are referred
#' to by their UUID given in the datasets.json (\url{https://github.com/ccodwg/Covid19CanadaArchive/blob/master/datasets.json})
#' file from "Covid19CanadaArchive" (\url{https://github.com/ccodwg/Covid19CanadaArchive}).
#'
#' @return A character vector containing the path of the temporary directory
#' containing the datasets required for \code{\link{update_raw_datasets}}. Names correspond to
#' the UUIDs assigned to the dataset in datasets.json.
#'
#' @export
dl_datasets <- function() {

  # define temporary directory for downloaded datasets
  ds_dir <- tempdir()

  # list datasets
  covid_datasets <- matrix(
    c(
      # ab
      "59da1de8-3b4e-429a-9e18-b67ba3834002", NA, NA, # c/d
      "24a572ea-0de3-4f83-b9b7-8764ea203eb6", NA, NA, # tests_completed
      # bc
      "a8637b6c-babf-48cd-aeab-2f38c713f596", NA, NA, # c (Interior, up to 2022-10-08)
      "878c0e41-ec21-4bd5-87e8-3b4a5969de84", NA, NA, # c (Interior, 2022-10-09 and later)
      "f7cd5492-f23b-45a5-9d9b-118ac2b47529", NA, NA, # c (Fraser, up to 2022-10-08)
      "29c5a1e0-2f4d-409d-b10a-d6a62caad835", NA, NA, # c (Fraser, 2022-10-09 and later)
      "1ad7ef1b-1b02-4d5c-aec2-4923ea100e97", NA, NA, # c (Vancouver Coastal, up to 2022-10-08)
      "635e4440-a4a3-457b-ac0f-7511f567afda", NA, NA, # c (Vancouver Coastal, 2022-10-09 and later)
      "89b48da6-bed9-4cd4-824c-8b6d82ffba24", NA, NA, # c (Vancouver Island, up to 2022-10-08)
      "2111db2e-f894-40ad-b7ad-aeea0c851a51", NA, NA, # c (Vancouver Island, 2022-10-09 and later)
      "def3aca2-3595-4d70-a5d2-d51f78912dda", NA, NA, # c (Northern, up to 2022-10-08)
      "b8aa2bec-cad8-45cf-901b-79f9f9aad545", NA, NA, # c (Northern, 2022-10-09 and later)
      "c0ab9514-92ea-4dda-b714-bab9985e58be", NA, NA, # c (Out of Canada, up to 2022-10-08)
      "f056e795-1502-43f2-b87d-603aac0edf05", NA, NA, # c (Out of Canada, 2022-10-09 and later)
      "91367e1d-8b79-422c-b314-9b3441ba4f42", NA, NA, # d
      "4f9dc8b7-7b42-450e-a741-a0f6a621d2af", NA, NA, # d (as of date)
      # can
      "314c507d-7e48-476e-937b-965499f51e8e", NA, NA, # c/d
      "366ce221-c1c9-4f41-a917-8ff4648f6a40", NA, NA, # tests_completed
      "d0bfcd85-9552-47a5-a699-aa6fe4815e00", NA, NA, # vaccine_coverage, vaccine_administration
      "ea3718c1-83f1-46a1-8b21-e25aebd1ebee", NA, NA, # wastewater_copies_per_ml
      # mb
      # nb
      # nl
      "b19daaca-6b32-47f5-944f-c69eebd63c07", NA, NA, # c
      "34f45670-34ed-415c-86a6-e14d77fcf6db", NA, NA, # d
      # ns
      # nt
      # nu
      # on
      "73fffd44-fbad-4de8-8d32-00cc5ae180a6", NA, NA, # c/d
      "4b214c24-8542-4d26-a850-b58fc4ef6a30", NA, NA, # h/i
      # pe
      # qc
      "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5", NA, NA, # c/d
      "f0c25e20-2a6c-4f9a-adc3-61b28ab97245", NA, NA # h/i
      # sk
      # yt
    ),
    ncol = 3,
    byrow = TRUE)

  # calculate dataset names
  covid_datasets <- cbind(covid_datasets, apply(covid_datasets, MARGIN = 1, FUN = function(x) {
    uuid <- x[1]
    sheet <- x[2]
    ds_name <- if (is.na(sheet)) {
      uuid
    } else {
      paste(uuid, gsub(" ", "-", sheet), sep = "-")
    }}))

  # read in datasets
  cat("Downloading datasets...", fill = TRUE)
  ds_failed <- apply(covid_datasets, MARGIN = 1, FUN = function(x) {
    uuid <- x[1]
    sheet <- x[2]
    sep <- x[3]
    ds_name <- x[4]
    tryCatch(
      {
        if (!is.na(sheet)) {
          d <- Covid19CanadaData::dl_dataset(uuid, sheet = sheet)
        } else if (!is.na(sep)) {
          d <- Covid19CanadaData::dl_dataset(uuid, sep = sep)
        } else {
          d <- Covid19CanadaData::dl_dataset(uuid)
        }
        cat(ds_name, fill = TRUE)
        if ("xml_document" %in% class(d)) {
          xml2::write_html(d, file = paste0(ds_dir, "/", ds_name, ".html"))
        } else {
          saveRDS(d, file = paste0(ds_dir, "/", ds_name, ".RData"))
        }
        return(1)
      },
      error = function(e) {
        print(e)
        cat("FAILED:", ds_name, fill = TRUE)
        return(NA)
      })})

  # name datasets
  names(ds_failed) <- covid_datasets[, 4]

  # retry failed datasets
  ds_failed <- names(ds_failed[unlist(lapply(ds_failed, function(x) all(is.na(x))))])
  if (length(ds_failed) > 0) {
    for (i in 1:length(ds_failed)) {
      # wait 10 seconds
      cat("WAITING 10 SECONDS BEFORE RETRY...", fill = TRUE)
      Sys.sleep(10)
      # get value of uuid, sheet, sep
      ds_name <- ds_failed[i]
      f <- covid_datasets[covid_datasets[, 4] == ds_name, 1:3]
      uuid <- f[1]
      sheet <- f[2]
      sep <- f[3]
      # retry dataset download
      tryCatch(
        {
          if (!is.na(sheet)) {
            d <- Covid19CanadaData::dl_dataset(uuid, sheet = sheet)
          } else if (!is.na(sep)) {
            d <- Covid19CanadaData::dl_dataset(uuid, sep = sep)
          } else {
            d <- Covid19CanadaData::dl_dataset(uuid)
          }
          cat("RETRY SUCCESSFUL: ", ds_name, fill = TRUE)
          if ("xml_document" %in% class(d)) {
            xml2::write_html(d, file = paste0(ds_dir, "/", ds_name, ".html"))
          } else {
            saveRDS(d, file = paste0(ds_dir, "/", ds_name, ".RData"))
          }
        },
        error = function(e) {
          print(e)
          cat("RETRY FAILED:", ds_name, fill = TRUE)
        }
      )
    }
  }

  # return temporary directory for downloaded datasets
  return(ds_dir)
}
