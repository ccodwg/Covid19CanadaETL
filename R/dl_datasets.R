#' Download datasets required for daily Canadian COVID-19 data collection
#'
#' The live version of each required dataset is downloaded from a URL using the
#' function \link[Covid19CanadaData]{dl_dataset} in "Covid19CanadaData". Datasets are referred to by
#' their UUID given in the datasets.json (\url{https://github.com/ccodwg/Covid19CanadaArchive/blob/master/datasets.json})
#' file from "Covid19CanadaArchive" (\url{https://github.com/ccodwg/Covid19CanadaArchive}).
#'
#' @param mode Download which dataset? One of "main" or "phu".
#' @return A named list with required datasets for \link{e_t_datasets}. Names
#' correspond to the UUIDs assigned to the dataset in datasets.json.
#'
#' @export
dl_datasets <- function(mode = c("main", "phu")) {

  # verify mode
  match.arg(mode, choices = c("main", "phu"), several.ok = FALSE)

  # list datasets
  covid_datasets <- if (mode == "main") {
    matrix(
      c(
        # ab
        "59da1de8-3b4e-429a-9e18-b67ba3834002", NA,
        "ec1acea4-8b85-4c04-b905-f075de040493", NA,
        "24a572ea-0de3-4f83-b9b7-8764ea203eb6", NA,
        # bc
        "91367e1d-8b79-422c-b314-9b3441ba4f42", NA,
        "f9a8dea5-1eed-447b-a9a0-be2a4b62d6a6", NA,
        "9d940861-0252-4d33-b6e8-23a2eeb105bf", NA,
        # can
        "fa3f2917-6553-438c-9a6f-2af8d077f47f", NA,
        # mb
        "0261e07b-85ce-4952-99d1-6e1e9a440291", NA,
        "1e9f40b2-853f-49d5-a9c4-ed04fee1bea2", NA,
        "a57dc10d-7139-4164-9042-eb2242716585", NA,
        "7a5ef226-2244-47e0-b964-28c2dc06d5ae", NA,
        # nb
        "4f194e3b-39fd-4fe0-b420-8cefa9001f7e", NA,
        "18fd4390-8e47-43b4-b1c1-c48218a7859b", NA,
        # nl
        "f0e10f54-a4db-48d8-9c4e-8571e663ca28", NA,
        "8419f6f1-b80b-4247-84e5-6414ab0154d8", NA,
        "6d1b0f2e-0ac0-4ad8-a383-619065ec5a52", NA,
        "25ca2057-978c-49b8-a9e6-1a1f70732659", NA,
        # ns
        "d0f05ef1-419f-4f4c-bc2d-17446c10059f", NA,
        "0e7a1f46-5d31-4267-be97-831172fa7081", NA,
        "70214276-8616-488c-b53a-b514608e3146", NA,
        "7b7be246-cd65-4f35-b354-faa705cacecc", NA,
        # nt
        "e008ba6f-7b09-4af4-afc5-63ec3c3bbfbb", NA,
        "66fbe91e-34c0-4f7f-aa94-cf6c14db0158", NA,
        "454de458-f7b4-4814-96a6-5a426f8c8c60", NA,
        # nu
        "04ab3773-f535-42ad-8ee4-4d584ec23523", NA,
        # on
        "73fffd44-fbad-4de8-8d32-00cc5ae180a6", NA,
        "a8b1be1a-561a-47f5-9456-c553ea5b2279", NA,
        "170057c4-3231-4f15-9438-2165c5438dda", NA,
        # pe
        "68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455", NA,
        "3ff94c42-8b12-4653-a6c9-0ddd8ff343d5", "Total Doses",
        "3ff94c42-8b12-4653-a6c9-0ddd8ff343d5", "Fully Immunized",
        # qc
        "0c577d5e-999e-42c5-b4c1-66b3787c3a04", NA,
        "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5", NA,
        "4e04442d-f372-4357-ba15-3b64f4e03fbe", NA,
        # sk
        "95de79d5-5e5c-45c2-bbab-41daf3dbee5d", NA,
        "9736bff9-4bd3-4c04-b9d9-87f60b3d5eb5", NA,
        "15556169-0471-49ea-926e-20b5e8dbd25d", NA,
        "28d7f978-9a7b-4933-a520-41b073868d05", NA,
        # yt
        "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e", NA
      ),
      ncol = 2,
      byrow = TRUE
    )
  } else {
    matrix(
      c(
        # Algoma (ALG)
        "685df305-f6c7-4ac2-992b-ec707eb1f1cb", NA,
        # Brant (BRN)
        "2e7a5549-92ae-473d-a97a-7b8e0c1ddbbc", NA,
        # Chatham-Kent (CKH)
        "fe08035c-2c03-4960-a642-bde1fe18c857", NA,
        # Durham (DUR)
        "ba7b0d74-5fe2-41d8-aadb-6320ff9acb21", NA,
        # Eastern (EOH)
        "cd1db4e8-c4e5-4b24-86a5-2294281919c6", NA,
        # Grey Bruce (GBH)
        "eac45a46-e5b5-4e75-9393-77995cd7e219", NA,
        # Haldimand-Norfolk (HNH)
        "07fcf6b9-6e61-433e-b1a8-a951ee15b01d", NA,
        # Haliburton Kawartha Pineridge (HKP)
        "c1cd96db-69c3-4970-9a4b-e7bcdc12d39b", NA,
        # Halton (HAL)
        "8d4067a7-4828-4b09-8396-089231cf2e94", NA,
        # Hamilton (HAM)
        "b8ef690e-d23f-4b7d-8cf8-bc4a0f3d0a84", NA,
        # Kingston Frontenac Lennox & Addington (KFL)
        "83d1fa13-7fb3-4079-b3dc-5bc50c584fd3", NA,
        # Lambton (LAM)
        "8d0cf226-b9b7-4fc3-8100-a4f56dec6792", NA,
        # Middlesex-London (MSL)
        "b32a2f6b-7745-4bb1-9f9b-7ad0000d98a0", NA,
        # Niagara (NIA)
        "e1887eb2-538f-4610-bc00-bcd7d929a375", NA,
        # North Bay Parry Sound (NPS)
        "3178dd11-17af-4478-a72e-e1a35d7d1b2d", NA,
        # Northwestern (NWR)
        "4c56a58b-0cb3-4d71-bafe-9fdb42e5c1d5", NA,
        # Ottawa (OTT)
        "d8d4cbc6-d0a5-4544-ad3e-5a3c3060f973", NA,
        # Peel (PEL)
        "34b7dda2-1843-47e1-9c24-0c2a7ab78431", NA,
        # Peterborough (PET)
        "c3aa6a5e-2ff5-4158-83ab-dcde251bc365", NA,
        # Porcupine (PQP)
        "00cc3ae2-7bf8-4074-81b7-8e06e91c947a", NA,
        # Renfrew (REN)
        "688bf944-9be6-49c3-ae5d-848ae32bad92", NA,
        # Simcoe Muskoka (SMD)
        "7106106a-2f43-4ed2-b2a2-a75a7046ff81", NA,
        # Sudbury (SUD)
        "4b9c88a2-9487-4632-adc5-cfd4a2fddb3f", NA,
        # Thunder Bay (THB)
        "942e48c4-1148-46e1-a5d3-e25aa9bede05", NA,
        # Timiskaming (TSK)
        "9c7bbba4-33ba-493a-8ea1-4eedd5149bc0", NA,
        # Toronto (TOR)
        "ebad185e-9706-44f4-921e-fc89d5cfa334", "Status",
        # Wellington Dufferin Guelph
        "e00e2148-b0ea-458b-9f00-3533e0c5ae8e", NA,
        # Windsor-Essex (WEK)
        "01574538-062f-4a41-9dd5-8fdb72a0fe03", NA,
        # York (YRK)
        "3821cc66-f88d-4f12-99ca-d36d368872cd", NA
      ),
      ncol = 2,
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
  ds <- apply(covid_datasets, MARGIN = 1, FUN = function(x) {
    uuid <- x[1]
    sheet <- x[2]
    ds_name <- x[3]
    tryCatch(
      {
        if (is.na(sheet)) {
          d <- Covid19CanadaData::dl_dataset(uuid)
        } else {
          d <- Covid19CanadaData::dl_dataset(uuid, sheet = sheet)
        }
        cat(ds_name, fill = TRUE)
        return(d)
      },
      error = function(e) {
        print(e)
        cat("FAILED:", ds_name, fill = TRUE)
        d <- NA
        return(d)
      })})

  # name datasets
  names(ds) <- covid_datasets[, 3]

  # retry failed datasets
  ds_failed <- names(ds[unlist(lapply(ds, function(x) all(is.na(x))))])
  if (length(ds_failed) > 0) {
    for (i in 1:length(ds_failed)) {
      # wait 10 seconds
      cat("WAITING 10 SECONDS BEFORE RETRY...", fill = TRUE)
      Sys.sleep(10)
      # get value of uuid and sheet
      ds_name <- ds_failed[i]
      f <- covid_datasets[covid_datasets[, 3] == ds_name, 1:2]
      uuid <- f[1]
      sheet <- f[2]
      # retry dataset download
      ds[[ds_name]] <- tryCatch(
        {
          if (is.na(sheet)) {
            d <- Covid19CanadaData::dl_dataset(uuid)
          } else {
            d <- Covid19CanadaData::dl_dataset(uuid, sheet = sheet)
          }
          cat("RETRY SUCCESSFUL: ", ds_name, fill = TRUE)
          d
        },
        error = function(e) {
          print(e)
          cat("RETRY FAILED:", ds_name, fill = TRUE)
          d <- NA
        }
      )
    }
  }

  # filter out failed data
  ds <- ds[unlist(lapply(ds, function(x) !all(is.na(x))))]

  # return list of datasets
  return(ds)
}
