#' Download datasets required for daily Canadian COVID-19 data collection
#'
#' The live version of each required dataset is downloaded from a URL using the
#' function \code{\link[Covid19CanadaData]{dl_dataset}}. Datasets are saved as
#' .RData files and HTML files to a temporary directory. Datasets are referred
#' to by their UUID given in the datasets.json (\url{https://github.com/ccodwg/Covid19CanadaArchive/blob/master/datasets.json})
#' file from "Covid19CanadaArchive" (\url{https://github.com/ccodwg/Covid19CanadaArchive}).
#'
#' @param mode Download which dataset? One of "main" or "phu".
#' @return A character vector containing the path of the temporary directory
#' containing the datasets required for \link{e_t_datasets}. Names correspond to
#' the UUIDs assigned to the dataset in datasets.json.
#'
#' @export
dl_datasets <- function(mode = c("main", "phu")) {

  # verify mode
  match.arg(mode, choices = c("main", "phu"), several.ok = FALSE)

  # create temporary directory for datasets
  ds_dir <- tempdir()
  cat("Creating temporary directory for datasets:", ds_dir, fill = TRUE)

  # list datasets
  covid_datasets <- if (mode == "main") {
    matrix(
      c(
        # ab
        "d3b170a7-bb86-4bb0-b362-2adc5e6438c2", NA, NA,
        "24a572ea-0de3-4f83-b9b7-8764ea203eb6", NA, NA,
        "ec1acea4-8b85-4c04-b905-f075de040493", NA, NA,
        # bc
        "91367e1d-8b79-422c-b314-9b3441ba4f42", NA, NA,
        "f9a8dea5-1eed-447b-a9a0-be2a4b62d6a6", NA, NA,
        "9d940861-0252-4d33-b6e8-23a2eeb105bf", NA, NA,
        # can
        "fa3f2917-6553-438c-9a6f-2af8d077f47f", NA, NA,
        "f7db31d0-6504-4a55-86f7-608664517bdb", NA, NA,
        # mb
        "8cb83971-19f0-4dfc-b832-69efc1036ddd", NA, NA,
        # nb
        "4f194e3b-39fd-4fe0-b420-8cefa9001f7e", NA, NA,
        "719bbb12-d493-4427-8896-e823c2a9833a", NA, NA,
        # nl
        "34f45670-34ed-415c-86a6-e14d77fcf6db", NA, NA,
        # ns
        # nt
        "9ed0f5cd-2c45-40a1-94c9-25b0c9df8f48", NA, NA,
        "454de458-f7b4-4814-96a6-5a426f8c8c60", NA, NA,
        # nu
        "04ab3773-f535-42ad-8ee4-4d584ec23523", NA, NA,
        # on
        "73fffd44-fbad-4de8-8d32-00cc5ae180a6", NA, NA,
        "a8b1be1a-561a-47f5-9456-c553ea5b2279", NA, NA,
        "170057c4-3231-4f15-9438-2165c5438dda", NA, NA,
        # pe
        "68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455", NA, NA,
        "3ff94c42-8b12-4653-a6c9-0ddd8ff343d5", "Total Doses", NA,
        "3ff94c42-8b12-4653-a6c9-0ddd8ff343d5", "Fully Immunized", NA,
        "3ff94c42-8b12-4653-a6c9-0ddd8ff343d5", "Third Doses", NA,
        # qc
        "0c577d5e-999e-42c5-b4c1-66b3787c3a04", NA, NA,
        "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5", NA, NA,
        "aee3bd38-b782-4880-9033-db76f84cef5b", NA, ";",
        "4e04442d-f372-4357-ba15-3b64f4e03fbe", NA, NA,
        # sk
        # yt
        "9c29c29f-fd38-45d5-a471-9bfb95abb683", NA, NA,
        "4a33ab2c-32a4-4630-8f6b-2bac2b1ce7ca", NA, NA,
        "387473c7-bcb9-4712-82fb-cd0355793cdc", NA, NA
      ),
      ncol = 3,
      byrow = TRUE
    )
  } else {
    matrix(
      c(
        # Algoma (ALG)
        "685df305-f6c7-4ac2-992b-ec707eb1f1cb", NA, NA,
        # Brant (BRN)
        "2e7a5549-92ae-473d-a97a-7b8e0c1ddbbc", NA, NA,
        # Hamilton (HAM)
        "b8ef690e-d23f-4b7d-8cf8-bc4a0f3d0a84", NA, NA,
        # Middlesex-London (MSL)
        "b32a2f6b-7745-4bb1-9f9b-7ad0000d98a0", NA, NA,
        # Niagara (NIA)
        "e1887eb2-538f-4610-bc00-bcd7d929a375", NA, NA,
        # North Bay Parry Sound (NPS)
        "3178dd11-17af-4478-a72e-e1a35d7d1b2d", NA, NA,
        # Northwestern (NWR)
        "4c56a58b-0cb3-4d71-bafe-9fdb42e5c1d5", NA, NA,
        # Ottawa (OTT)
        "d8d4cbc6-d0a5-4544-ad3e-5a3c3060f973", NA, NA,
        # Peel (PEL)
        "34b7dda2-1843-47e1-9c24-0c2a7ab78431", NA, NA,
        # Peterborough (PET)
        "821645cf-acbb-49d1-ae28-0e65037c61bf", NA, NA,
        # Porcupine (PQP)
        "00cc3ae2-7bf8-4074-81b7-8e06e91c947a", NA, NA,
        # Renfrew (REN)
        "688bf944-9be6-49c3-ae5d-848ae32bad92", NA, NA,
        # Simcoe Muskoka (SMD)
        "7106106a-2f43-4ed2-b2a2-a75a7046ff81", NA, NA,
        # Sudbury (SUD)
        "4b9c88a2-9487-4632-adc5-cfd4a2fddb3f", NA, NA,
        # Thunder Bay (THB)
        "942e48c4-1148-46e1-a5d3-e25aa9bede05", NA, NA,
        # Timiskaming (TSK)
        "9c7bbba4-33ba-493a-8ea1-4eedd5149bc0", NA, NA,
        # Toronto (TOR)
        "ebad185e-9706-44f4-921e-fc89d5cfa334", "Status", NA,
        # Wellington Dufferin Guelph
        "e00e2148-b0ea-458b-9f00-3533e0c5ae8e", NA, NA,
        # Windsor-Essex (WEK)
        "fb6ccf1c-498f-40d0-b70c-8fce37603be1", NA, NA,
        # York (YRK)
        "3821cc66-f88d-4f12-99ca-d36d368872cd", NA, NA,
        # Ontario Ministry of Health Time Series by PHU
        # used for:
        # Chatham-Kent (CKH)
        # Eastern (EOH)
        # Haldimand-Norfolk (HNH)
        # Huron Perth (HPH)
        # Leeds Grenville and Lanark (LGL)
        "73fffd44-fbad-4de8-8d32-00cc5ae180a6", NA, NA
      ),
      ncol = 3,
      byrow = TRUE
    )
  }

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

  # return temporary directory for datasets
  return(ds_dir)
}
