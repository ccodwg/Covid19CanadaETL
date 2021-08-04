#' Download datasets required for daily Canadian COVID-19 data collection
#'
#' The live version of each required dataset is downloaded from a URL using the
#' function \link[Covid19CanadaData]{dl_dataset} in "Covid19CanadaData". Datasets are referred to by
#' their UUID given in the datasets.json (\url{https://github.com/ccodwg/Covid19CanadaArchive/blob/master/datasets.json})
#' file from "Covid19CanadaArchive" (\url{https://github.com/ccodwg/Covid19CanadaArchive}).
#'
#' Some datasets requiring additional options (e.g., Excel sheet number) must
#' presently be supplied outside of this function.
#'
#' @return A named list with required datasets for \link{e_t_datasets}. Names
#' correspond to the UUIDs assigned to the dataset in datasets.json.
#'
#' @export
dl_datasets <- function() {
  # list datasets
  covid_datasets <- list(
    # ab
    "59da1de8-3b4e-429a-9e18-b67ba3834002",
    "ec1acea4-8b85-4c04-b905-f075de040493",
    "24a572ea-0de3-4f83-b9b7-8764ea203eb6",
    # bc
    "91367e1d-8b79-422c-b314-9b3441ba4f42",
    "f9a8dea5-1eed-447b-a9a0-be2a4b62d6a6",
    "9d940861-0252-4d33-b6e8-23a2eeb105bf",
    # can
    "fa3f2917-6553-438c-9a6f-2af8d077f47f",
    # mb
    "0261e07b-85ce-4952-99d1-6e1e9a440291",
    "1e9f40b2-853f-49d5-a9c4-ed04fee1bea2",
    "a57dc10d-7139-4164-9042-eb2242716585",
    "7a5ef226-2244-47e0-b964-28c2dc06d5ae",
    # nb
    "4f194e3b-39fd-4fe0-b420-8cefa9001f7e",
    "18fd4390-8e47-43b4-b1c1-c48218a7859b",
    # nl
    "f0e10f54-a4db-48d8-9c4e-8571e663ca28",
    "8419f6f1-b80b-4247-84e5-6414ab0154d8",
    "6d1b0f2e-0ac0-4ad8-a383-619065ec5a52",
    "3c0bcd2e-e563-4426-b2d8-3928cbce9f37",
    # ns
    "d0f05ef1-419f-4f4c-bc2d-17446c10059f",
    "0e7a1f46-5d31-4267-be97-831172fa7081",
    "70214276-8616-488c-b53a-b514608e3146",
    # nt
    "e008ba6f-7b09-4af4-afc5-63ec3c3bbfbb",
    # nu
    "04ab3773-f535-42ad-8ee4-4d584ec23523",
    "bd18a4e4-bc22-47c6-b601-1aae39667a03",
    # on
    "921649fa-c6c0-43af-a112-23760da4d622",
    "a8b1be1a-561a-47f5-9456-c553ea5b2279",
    "170057c4-3231-4f15-9438-2165c5438dda",
    # pe
    "68e5cbb9-0dcc-4a4f-ade0-58a0b06b1455",
    # qc
    "0c577d5e-999e-42c5-b4c1-66b3787c3a04",
    "3b93b663-4b3f-43b4-a23d-cbf6d149d2c5",
    "aee3bd38-b782-4880-9033-db76f84cef5b",
    "4e04442d-f372-4357-ba15-3b64f4e03fbe",
    # sk
    "95de79d5-5e5c-45c2-bbab-41daf3dbee5d",
    "9736bff9-4bd3-4c04-b9d9-87f60b3d5eb5",
    "15556169-0471-49ea-926e-20b5e8dbd25d",
    # yt
    "4cdeff57-3cbd-4d58-b0a9-c66d8c0c197e"
  )
  # read in datasets
  ds <- lapply(covid_datasets, FUN = function(x) {
    d <- Covid19CanadaData::dl_dataset(x)
    cat(x, fill = TRUE)
    return(d)}
  )
  # name datasets according to UUID
  names(ds) <- covid_datasets

  # add datasets requiring additional options
  ds["3ff94c42-8b12-4653-a6c9-0ddd8ff343d5-Total-Doses"] <- Covid19CanadaData::dl_dataset(
    "3ff94c42-8b12-4653-a6c9-0ddd8ff343d5", sheet = "Total Doses")
  cat("3ff94c42-8b12-4653-a6c9-0ddd8ff343d5-Total-Doses", fill = TRUE)
  ds["3ff94c42-8b12-4653-a6c9-0ddd8ff343d5-Fully-Immunized"] <- Covid19CanadaData::dl_dataset(
    "3ff94c42-8b12-4653-a6c9-0ddd8ff343d5", sheet = "Fully Immunized")
  cat("3ff94c42-8b12-4653-a6c9-0ddd8ff343d5-Fully-Immunized", fill = TRUE)

  # return list of datasets
  return(ds)
}
