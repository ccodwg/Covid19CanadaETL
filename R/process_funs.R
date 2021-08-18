#' Functions to further process COVID-19 data
#'
#' Functions to further process data downloaded via the `process_dataset`
#' function from `Covid19CanadaDataProcess`. Examples include normalizing
#' province and health region names and aggregating health region-level data up
#' to the province level.
#'
#' @importFrom rlang .data
#' @param d The dataset to process.
#' @param prov Two-letter province abbreviation (e.g., "ON").
#' @param loc Either "prov" or "hr".
#' @param pattern The pattern of values to collate (e.g., "_cases_").
#' @param date_today Today's date.
#' @param id The ID of the Google Sheet document to load.
#' @param sheet Optional. The specific sheet within the Google Sheet document to load.
#' @param opt Optional. Other options passed to the function.
#' @param ... Other arguments.
#'
#' @name process_funs
NULL

#' process_funs: Normalize province names to comport with CCODWG dataset
#'
#' @rdname process_funs
#'
#' @export
process_prov_names <- function(d) {
  # get prov names
  provs <- get_prov_data() %>%
    dplyr::select(.data$province, .data$province_short)
  provs <- d %>%
    dplyr::left_join(
      provs,
      by = c("province" = "province_short")
    )
  # convert prov names
  d %>%
    dplyr::mutate(
      province = provs$province.y
    )
}

#' process_funs: Normalize health region names to comport with CCODWG dataset
#'
#' @rdname process_funs
#'
#' @export
process_hr_names <- function(d, prov, date_today = Sys.Date(), opt = NULL) {

  # fix HR names
  if (!prov %in% c("BC", "MB", "NB", "ON", "QC", "SK")) {
    # get HR names
    hr <- get_hr_data(sknew = TRUE) %>%
      dplyr::filter(.data$province_short == prov) %>%
      dplyr::select(.data$health_region, .data$health_region_esri)
    hr[hr$health_region == "Not Reported", "health_region_esri"] <- "Not Reported"
  } else {
    switch(
      prov,
      "BC" = {
        hr <- data.frame(
          health_region = c("Interior", "Fraser", "Vancouver Coastal",
                            "Island", "Northern", "Not Reported"),
          health_region_esri = c("Interior", "Fraser", "Vancouver Coastal",
                                 "Vancouver Island", "Northern", "Not Reported")
        )
      },
      "MB" = {
        hr <- data.frame(
          health_region = c("Interlake-Eastern", "Northern", "Prairie Mountain",
                            "Southern Health", "Winnipeg", "Not Reported"),
          health_region_esri = c("Interlake-Eastern", "Northern", "Prairie Mountain Health",
                                 "Southern Health-Sant\u00E9 Sud", "Winnipeg", "Not Reported")
        )
      },
      "NB" = {
        hr <- data.frame(
          health_region = c("Zone 1 (Moncton area)", "Zone 2 (Saint John area)",
                            "Zone 3 (Fredericton area)", "Zone 4 (Edmundston area)",
                            "Zone 5 (Campbellton area)", "Zone 6 (Bathurst area)",
                            "Zone 7 (Miramichi area)", "Not Reported"),
          health_region_esri = c("Zone 1: South East Region (Moncton)", "Zone 2: South Central Region (Saint John)",
                                 "Zone 3: Central West Region (Fredericton)", "Zone 4: North West Region (Edmundston)",
                                 "Zone 5: North Central Region (Campbellton)", "Zone 6: North East Region (Bathurst)",
                                 "Zone 7: Central East Region (Miramichi)", "Not Reported")
        )
      },
      "ON" = {
        match.arg(opt, choices = c("moh", "esri"), several.ok = FALSE)
        if (opt == "moh") {
          hr <- data.frame(
            health_region = c("Algoma", "Brant", "Chatham-Kent",
                              "Durham", "Eastern", "Grey Bruce",
                              "Haldimand-Norfolk", "Haliburton Kawartha Pineridge", "Halton",
                              "Hamilton", "Hastings Prince Edward", "Huron Perth",
                              "Kingston Frontenac Lennox & Addington", "Lambton", "Leeds Grenville and Lanark",
                              "Middlesex-London", "Niagara", "North Bay Parry Sound",
                              "Northwestern", "Ottawa", "Peel",
                              "Peterborough", "Porcupine", "Renfrew",
                              "Simcoe Muskoka", "Southwestern", "Sudbury",
                              "Thunder Bay", "Timiskaming", "Toronto",
                              "Waterloo", "Wellington Dufferin Guelph", "Windsor-Essex",
                              "York", "Not Reported"),
            health_region_esri = c("ALGOMA DISTRICT", "BRANT COUNTY", "CHATHAM-KENT",
                                   "DURHAM REGION", "EASTERN ONTARIO", "GREY BRUCE",
                                   "HALDIMAND-NORFOLK", "HALIBURTON, KAWARTHA, PINE RIDGE", "HALTON REGION",
                                   "CITY OF HAMILTON", "HASTINGS & PRINCE EDWARD COUNTIES", "HURON PERTH",
                                   "KINGSTON, FRONTENAC, LENNOX & ADDINGTON", "LAMBTON COUNTY", "LEEDS, GRENVILLE AND LANARK DISTRICT",
                                   "MIDDLESEX-LONDON", "NIAGARA REGION", "NORTH BAY PARRY SOUND DISTRICT",
                                   "NORTHWESTERN", "CITY OF OTTAWA", "PEEL REGION",
                                   "PETERBOROUGH COUNTY-CITY", "PORCUPINE", "RENFREW COUNTY AND DISTRICT",
                                   "SIMCOE MUSKOKA DISTRICT", "OXFORD ELGIN-ST.THOMAS", "SUDBURY AND DISTRICT",
                                   "THUNDER BAY DISTRICT", "TIMISKAMING", "TORONTO",
                                   "WATERLOO REGION", "WELLINGTON-DUFFERIN-GUELPH", "WINDSOR-ESSEX COUNTY",
                                   "YORK REGION", "Not Reported"
            )
          )
        } else if (opt == "esri") {
          hr <- data.frame(
            health_region = c("Algoma", "Brant", "Chatham-Kent",
                              "Durham", "Eastern", "Grey Bruce",
                              "Haldimand-Norfolk", "Haliburton Kawartha Pineridge", "Halton",
                              "Hamilton", "Hastings Prince Edward", "Huron Perth",
                              "Kingston Frontenac Lennox & Addington", "Lambton", "Leeds Grenville and Lanark",
                              "Middlesex-London", "Niagara", "North Bay Parry Sound",
                              "Northwestern", "Ottawa", "Peel",
                              "Peterborough", "Porcupine", "Renfrew",
                              "Simcoe Muskoka", "Southwestern", "Sudbury",
                              "Thunder Bay", "Timiskaming", "Toronto",
                              "Waterloo", "Wellington Dufferin Guelph", "Windsor-Essex",
                              "York", "Not Reported"),
            health_region_esri = c("Algoma Public Health Unit", "Brant County Health Unit", "Chatham-Kent Health Unit",
                                   "Durham Region Health Department", "Eastern Ontario Health Unit", "Grey Bruce Health Unit",
                                   "Haldimand-Norfolk Health Unit", "Haliburton, Kawartha, Pine Ridge District Health Unit", "Halton Region Health Department",
                                   "Hamilton Public Health Services", "Hastings and Prince Edward Counties Health Unit", "Huron Perth District Health Unit",
                                   "Kingston, Frontenac and Lennox & Addington Public Health", "Lambton Public Health", "Leeds, Grenville and Lanark District Health Unit",
                                   "Middlesex-London Health Unit", "Niagara Region Public Health Department", "North Bay Parry Sound District Health Unit",
                                   "Northwestern Health Unit", "Ottawa Public Health", "Peel Public Health",
                                   "Peterborough Public Health", "Porcupine Health Unit", "Renfrew County and District Health Unit",
                                   "Simcoe Muskoka District Health Unit", "Southwestern Public Health", "Sudbury & District Health Unit",
                                   "Thunder Bay District Health Unit", "Timiskaming Health Unit", "Toronto Public Health",
                                   "Region of Waterloo, Public Health", "Wellington-Dufferin-Guelph Public Health", "Windsor-Essex County Health Unit",
                                   "York Region Public Health Services", "Not Reported"
            )
          )
        }
      },
      "QC" = {
        hr <- get_hr_data(sknew = TRUE) %>%
          dplyr::filter(.data$province_short == prov) %>%
          dplyr::select(.data$health_region) %>%
          dplyr::mutate(health_region_esri = .data$health_region) %>%
          dplyr::mutate(
            health_region_esri = dplyr::case_when(
              .data$health_region_esri == "Gasp\u00E9sie-\u00CEles-de-la-Madeleine" ~ "Gasp\u00E9sie - \u00CEles-de-la-Madeleine",
              .data$health_region_esri == "Saguenay" ~ "Saguenay - Lac-Saint-Jean",
              .data$health_region_esri == "Mauricie" ~ "Mauricie et Centre-du-Qu\u00E9bec",
              TRUE ~ .data$health_region_esri
            )
          )
      },
      "SK" = {
        hr <- data.frame(
          health_region = c("Far North", "Far North", "Far North",
                            "North", "North", "North",
                            "Saskatoon", "Central", "Central",
                            "Regina", "South", "South",
                            "South", "Not Reported"),
          health_region_esri = c("Far North West", "Far North Central", "Far North East",
                                 "North West", "North Central", "North East",
                                 "Saskatoon", "Central West", "Central East",
                                 "Regina", "South West", "South Central",
                                 "South East", "Not Reported")
        )
      }
    )
  }

  # convert unknown/out of province HRs to "Not Reported"
  d <- d %>%
    dplyr::mutate(
      sub_region_1 = ifelse(.data$sub_region_1 %in% c(
        "Unknown", "Out of Canada",
        "Hors Qu\u00E9bec", "Inconnu",
        "R\u00E9gion inconnue",
        "R\u00E9gion \u00e0 d\u00E9terminer"),
                            "Not Reported",
                            .data$sub_region_1)
    )

  # ensure a not reported HR exists, otherwise create one
  nr <- d %>%
    dplyr::filter(.data$sub_region_1 == "Not Reported")
  if (nrow(nr) == 0) {
    d <- d %>%
      {dplyr::add_row(
        .,
        data.frame(
          name = .[["name"]][1],
          province = prov,
          sub_region_1 = "Not Reported",
          date = date_today,
          value = 0
        )
      )}
    cat(prov, " ", d[["name"]][1], ": Adding a row for Not Reported...", sep = "", fill = TRUE)
  }

  # convert column names
  d %>%
    dplyr::left_join(
      hr,
      by = c("sub_region_1" = "health_region_esri")
    ) %>%
    dplyr::select(.data$name, .data$province, .data$health_region,
                  .data$date, .data$value) %>%
    dplyr::rename(sub_region_1 = .data$health_region)
}

#' process_funs: Aggregate values to province level
#'
#' @rdname process_funs
#'
#' @export
process_agg2prov <- function(d) {
  d %>%
    dplyr::select(!dplyr::matches("^sub_region_1$|^sub_region_2$")) %>%
    dplyr::group_by(dplyr::across(c(-.data$value))) %>%
    dplyr::summarize(value = sum(.data$value), .groups = "drop")
}

#' process_funs: Convert province with one health region to health region format
#'
#' @rdname process_funs
#'
#' @export
process_prov2hr <- function(d, prov) {
  match.arg(prov,
            choices = c("NT", "NU", "PE", "YT"),
            several.ok = FALSE)
  d %>%
    dplyr::mutate(
      sub_region_1 = dplyr::case_when(
        prov == "NT" ~ "NWT",
        prov == "NU" ~ "Nunavut",
        prov == "PE" ~ "Prince Edward Island",
        prov == "YT" ~ "Yukon"
      )
    ) %>%
    dplyr::select(.data$name, .data$province, .data$sub_region_1,
                         .data$date, .data$value)
}

#' process_funs: Get current cumulative value(s)
#'
#' @rdname process_funs
#'
#' @export
process_cum_current <- function(d) {
  max_date <- max(d$date)
  current_date <- Sys.Date()
  d %>%
    dplyr::filter(.data$date == max_date) %>%
    dplyr::mutate(date = current_date)
}

#' process_funs: Collate data by value
#'
#' @rdname process_funs
#'
#' @export
process_collate <- function(d, pattern) {
  d[grep(pattern, names(d))] %>%
    dplyr::bind_rows() %>%
    process_prov_names()
}

#' process_funs: Download quickly from Google Sheets
#'
#' @rdname process_funs
#'
#' @export
sheets_load <- function(id, sheet = NULL) {
  if (!is.null(sheet)) {
    googlesheets4::read_sheet(
      ss = id,
      sheet = sheet,
      col_types = "c" # don't mangle dates
    )
  } else {
    googlesheets4::read_sheet(
      ss = id,
      col_types = "c" # don't mangle dates
    )
  }
}

#' process_funs: Format datasets for upload to Google Sheets
#'
#' @rdname process_funs
#'
#' @export
process_format_sheets <- function(d, loc = c("prov", "hr")) {
  # check arg
  match.arg(loc, c("prov", "hr", several.ok = FALSE))

  # format dataset
  if (loc == "prov") {
    d %>%
      tidyr::pivot_wider(
        id_cols = c(.data$province),
        names_from = .data$date,
        values_from = .data$value) %>%
      dplyr::arrange(.data$province)
  } else if (loc == "hr") {
    d %>%
      tidyr::pivot_wider(
        id_cols = c(.data$province, .data$sub_region_1),
        names_from = .data$date,
        values_from = .data$value) %>%
      dplyr::arrange(.data$province, .data$sub_region_1)
  } else {
    stop('loc should be "prov" or "hr".')
  }
}

#' process_funs: Format dates from %Y-%m-%d to %d-%m-%Y
#'
#' @rdname process_funs
#'
#' @export
process_format_dates <- function(...) {
  inputs <- unlist(list(...))
  for (i in inputs) {
    assign(i, get(i, envir = parent.frame()) %>%
             dplyr::mutate(date = format(as.Date(.data$date), format = "%d-%m-%Y")),
           envir = parent.frame()
    )
  }
}
