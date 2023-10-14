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
      "3e4fd9ff-48f9-4de1-a48a-97fd33b68337", NA, NA, # c
      "2a11bbcc-7b43-47d1-952d-437cdc9b2ffb", NA, NA, # c, d
      "e477791b-bced-4b20-b40b-f8d7629c9b69", NA, NA, # h_a, i_a
      # bc
      # can
      "314c507d-7e48-476e-937b-965499f51e8e", NA, NA, # c, d
      "f1e1a857-fab8-4c25-a132-f474fab93622", NA, NA, # h, i
      "e41c63ec-ac54-47c9-8cf3-da2e1146aa75", NA, NA, # t
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
      "921649fa-c6c0-43af-a112-23760da4d622", NA, NA, # c
      "75dd0545-f728-4af5-8185-4269596785ef", NA, NA, # d (pt)
      "4b214c24-8542-4d26-a850-b58fc4ef6a30", NA, NA, # h, i
      # pe
      # qc
      "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5", NA, NA, # c, d
      "f0c25e20-2a6c-4f9a-adc3-61b28ab97245", NA, NA # h, i
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
