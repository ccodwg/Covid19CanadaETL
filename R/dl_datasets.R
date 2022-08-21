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
      "ab6abe51-c9b1-4093-b625-93de1ddb6302", NA, NA, # c
      "91367e1d-8b79-422c-b314-9b3441ba4f42", NA, NA, # d
      "4f9dc8b7-7b42-450e-a741-a0f6a621d2af", NA, NA, # d (as of date)
      # can
      "314c507d-7e48-476e-937b-965499f51e8e", NA, NA, # c/d
      "366ce221-c1c9-4f41-a917-8ff4648f6a40", NA, NA, # tests_completed
      "d0bfcd85-9552-47a5-a699-aa6fe4815e00", NA, NA, # vaccine_coverage
      "c4631a95-cb44-4d21-ae39-f1ad54daf814", NA, NA, # vaccine_administration
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
      "f0c25e20-2a6c-4f9a-adc3-61b28ab97245", NA, NA, # h/i
      # sk
      # yt
      "8eb9a22f-a2c0-4bdb-8f6c-ef8134901efe", NA, NA, # c
      "4a33ab2c-32a4-4630-8f6b-2bac2b1ce7ca", NA, NA # tests_completed
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
