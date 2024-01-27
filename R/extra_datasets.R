#' Assemble and write extra datasets for CovidTimelineCanada
#'
#' @importFrom rlang .data
#'
#' @export
extra_datasets <- function() {

  # announce start
  cat("Assembling extra datasets...", fill = TRUE)

  # define maximum date for dataset
  dataset_max_date <- as.Date("2023-12-31")

  # function: filter for maximum date
  filter_max_date <- function(d, date_col = "date") {
    # ensure date col is correct type
    d[[date_col]] <- as.Date(d[[date_col]])
    # filter for maximum date
    dplyr::filter(d, .data[[date_col]] <= dataset_max_date)
  }

  # PHAC wastewater dataset
  tryCatch(
    {
      # load data
      d <- read_d("raw_data/active_ts/can/can_wastewater_copies_per_ml_subhr_ts.csv", val_numeric = TRUE)
      # max date
      d <- filter_max_date(d)
      # write dataset
      utils::write.csv(d, file.path("extra_data", "phac_wastewater", "phac_wastewater.csv"), row.names = FALSE, quote = 1:7, na = "")
    },
    error = function(e) {
      cat("Error in updating PHAC wastewater dataset:", fill = TRUE)
      print(e)
    }
  )

  # territories: case and testing data from RVDSS since week ending 2022-09-03
  tryCatch(
    {
      ter <- Covid19CanadaData::dl_dataset("e41c63ec-ac54-47c9-8cf3-da2e1146aa75")
      ter <- ter |>
        dplyr::filter(.data$prname %in% c("Yukon", "Northwest Territories", "Nunavut")) |>
        dplyr::transmute(
          region = dplyr::case_when(
            .data$prname == "Yukon" ~ "YT",
            .data$prname == "Northwest Territories" ~ "NT",
            .data$prname == "Nunavut" ~ "NU"),
          date_start = as.Date(.data$date) - 6,
          date_end = as.Date(.data$date),
          tests_completed_weekly = .data$numtests_weekly,
          percent_positivity_weekly = .data$percentpositivity_weekly,
          cases_weekly = round(.data$percent_positivity_weekly * .data$tests_completed_weekly / 100),
          .data$update
        )
      # max date
      ter <- filter_max_date(ter, "date_end")
      # write dataset
      utils::write.csv(ter, file.path("extra_data", "territories_rvdss_since_2022-09-03", "territories_rvdss_since_2022-09-03.csv"), row.names = FALSE, quote = 1:3, na = "")
    },
    error = function(e) {
      cat("Error in updating territories data:", fill = TRUE)
      print(e)
    }
  )

  # sk biweekly HR-level case snapshots
  tryCatch(
    {
      # process
      sk <- read_d("raw_data/reports/sk/sk_crisp_report.csv") |>
        dplyr::transmute(.data$date_start, .data$date_end, .data$region, .data$sub_region_1, cases_weekly = .data$cases) |>
        dplyr::filter(.data$date_start >= as.Date("2022-12-25") & !is.na(.data$sub_region_1) & !is.na(.data$cases_weekly)) |>
        convert_hr_names()
      # max date
      sk <- filter_max_date(sk, "date_end")
      # write file
      utils::write.csv(sk, file.path("extra_data", "sk_biweekly_cases_hr", "sk_biweekly_cases_hr.csv"), row.names = FALSE, quote = 1:4)
      rm(sk) # clean up
    },
    error = function(e) {
      cat("Error in updating SK biweekly HR-level case snapshots:", fill = TRUE)
      print(e)
    }
  )

  # PHAC individual-level data
  tryCatch(
    {
      ## check if release date has changed
      rd <- readLines(file.path("extra_data", "phac_individual_ts", "release_date.txt"))
      if (length(rd) == 0) rd <- ""
      rd_new <- rvest::read_html("https://www150.statcan.gc.ca/n1/pub/13-26-0003/132600032020001-eng.htm") |>
        rvest::html_text2() |>
        stringr::str_extract("(?<=Release date: )(\\w+ \\d+, \\d+)") |>
        as.Date("%B %d, %Y") |>
        as.character()
      if (rd_new == rd) {
        # no update required
      } else {
        ## download file
        tmp <- tempfile()
        utils::download.file("https://www150.statcan.gc.ca/n1/pub/13-26-0003/2020001/COVID19-eng.zip", destfile = tmp)
        phac <- readr::read_csv(unz(tmp, filename = "COVID19-eng.csv"), col_types = readr::cols()) # silently

        ## get values for episode week group from metadata webpage
        ewg <- rvest::read_html("https://www150.statcan.gc.ca/n1/pub/13-26-0002/132600022020001-eng.htm") |>
          rvest::html_element(xpath = "//table[caption[contains(., 'Information on the Episode Week Group Indicator')]]") |>
          rvest::html_table()
        ewg <- ewg[1:nrow(ewg) - 1, ] # remove 'not applicable'
        ewg <- stats::setNames(
          paste0("Weeks ", ewg$`Episode weeks grouped`, ", grouped with week ", ewg$`Grouped with episode week`, ", 20", ewg$`Episode Year`),
          as.integer(ewg$`Episode week group`))
        ewg[1] <- "No grouping"

        ## process
        phac <- phac |>
          dplyr::transmute(
            region = .data$COV_REG,
            episode_year = .data$COV_EY,
            episode_week = .data$COV_EW,
            episode_week_group = .data$COV_EWG,
            gender = .data$COV_GDR,
            age_group = .data$COV_AGR,
            hospital_status = .data$COV_HSP,
            death = .data$COV_DTH)
        phac <- phac |>
          dplyr::mutate(
            region = dplyr::case_when(
              .data$region == 1 ~ "Atlantic (New Brunswick, Nova Scotia, Prince Edward Island, Newfoundland and Labrador)",
              .data$region == 2 ~ "Quebec",
              .data$region == 3 ~ "Ontario and Nunavut",
              .data$region == 4 ~ "Prairies (Manitoba, Saskatchewan, Alberta) and the Northwest Territories",
              .data$region == 5 ~ "British Columbia and Yukon"
            ),
            episode_year = ifelse(.data$episode_year == 99, "Unknown", paste0("20", .data$episode_year)),
            episode_week = ifelse(.data$episode_week == 99, "Unknown", .data$episode_week),
            episode_week_group = dplyr::recode(.data$episode_week_group, !!!ewg),
            gender = factor(dplyr::case_when(
              .data$gender == 1 ~ "Male",
              .data$gender == 2 ~ "Female",
              .data$gender == 9 ~ "Not stated/Other"
            ), levels = c("Male", "Female", "Not stated/Other")),
            age_group = factor(dplyr::case_when(
              .data$age_group == 1 ~ "0-19",
              .data$age_group == 2 ~ "20-29",
              .data$age_group == 3 ~ "30-39",
              .data$age_group == 4 ~ "40-49",
              .data$age_group == 5 ~ "50-59",
              .data$age_group == 6 ~ "60-69",
              .data$age_group == 7 ~ "70-79",
              .data$age_group == 8 ~ "80+",
              .data$age_group == 99 ~ "Not stated",
            ), levels = c("0-19", "20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80+", "Not stated")),
            hospital_status = factor(dplyr::case_when(
              .data$hospital_status == 1 ~ "Hospitalized - ICU",
              .data$hospital_status == 2 ~ "Hospitalized - Non-ICU",
              .data$hospital_status == 3 ~ "Not Hospitalized",
              .data$hospital_status == 9 ~ "Not stated/Unknown"
            ), levels = c("Hospitalized - ICU", "Hospitalized - Non-ICU", "Not Hospitalized", "Not stated/Unknown")),
            death = factor(dplyr::case_when(
              .data$death == 1 ~ "Yes",
              .data$death == 2 ~ "No",
              .data$death == 9 ~ "Not Stated"
            ), levels = c("Yes", "No", "Not Stated"))
          )
        # create lookup table of episode week dates
        dates <- dplyr::select(phac, c("episode_year", "episode_week")) |>
          dplyr::distinct()
        names(dates) <- c("episode_year", "episode_week")
        dates$episode_year <- suppressWarnings(as.integer(dates$episode_year))
        dates$episode_week <- suppressWarnings(as.integer(dates$episode_week))
        dates <- dates[!is.na(dates$episode_year) & !is.na(dates$episode_week), ]
        dates$episode_week_date_start <- MMWRweek::MMWRweek2Date(dates$episode_year, dates$episode_week)
        dates$episode_week_date_end <- dates$episode_week_date_start + 6
        dates$episode_year <- as.character(dates$episode_year)
        dates$episode_week <- as.character(dates$episode_week)
        # add date info
        phac <- phac |>
          dplyr::left_join(dates, by = c("episode_year", "episode_week")) |>
          dplyr::transmute(
            .data$region,
            .data$episode_year,
            .data$episode_week,
            episode_week_date_start = dplyr::case_when(
              .data$episode_year == "Unknown" & .data$episode_week == "Unknown" ~ "Unknown episode week, unknown year",
              .data$episode_week == "Unknown" ~ paste0("Unknown episode_week, ", .data$episode_year),
              TRUE ~ as.character(.data$episode_week_date_start)),
            episode_week_date_end = dplyr::case_when(
              .data$episode_year == "Unknown" & .data$episode_week == "Unknown" ~ "Unknown episode week, unknown year",
              .data$episode_week == "Unknown" ~ paste0("Unknown episode_week, ", .data$episode_year),
              TRUE ~ as.character(.data$episode_week_date_end)),
            .data$episode_week_group,
            .data$gender,
            .data$age_group,
            .data$hospital_status,
            .data$death
          )
        # summarize data
        phac <- phac |>
          dplyr::group_by(
            .data$region, .data$episode_year, .data$episode_week, .data$episode_week_date_start, .data$episode_week_date_end,
            .data$episode_week_group, .data$gender, .data$age_group, .data$hospital_status, .data$death) |>
          dplyr::summarize(count = dplyr::n(), .groups = 'drop')
        # add empty/missing combinations of age group/gender within missing region/episode week groups
        all_groups <- tidyr::expand_grid(
          age_group = levels(phac$age_group), gender = levels(phac$gender),
          hospital_status = levels(phac$hospital_status), death = levels(phac$death)) # all POSSIBLE combinations
        all_dates <- phac |>
          dplyr::select(
            .data$region, .data$episode_year, .data$episode_week, .data$episode_week_date_start, .data$episode_week_date_end, .data$episode_week_group) |>
          dplyr::distinct() # all EXISTING combinations
        all_rows <- all_dates |>
          tidyr::crossing(all_groups)
        phac <- dplyr::left_join(
          all_rows, phac,
          by = c("region", "episode_year", "episode_week", "episode_week_date_start", "episode_week_date_end", "episode_week_group",
                 "gender", "age_group", "hospital_status", "death")) |>
          tidyr::replace_na(list(count = 0))
        # convert hospital_status and death to columns
        phac_hs <- phac |>
          dplyr::select(-"death") |>
          dplyr::group_by(dplyr::across(-"count")) |>
          dplyr::summarize(count = sum(.data$count), .groups = "drop") |>
          tidyr::pivot_wider(
            names_from = "hospital_status",
            values_from = "count",
            values_fill = 0,
            names_prefix = "hospital_status_",
            names_sep = ""
          )
        phac_d <- phac |>
          dplyr::select(-"hospital_status") |>
          dplyr::group_by(dplyr::across(-"count")) |>
          dplyr::summarize(count = sum(.data$count), .groups = "drop") |>
          tidyr::pivot_wider(
            names_from = "death",
            values_from = "count",
            values_fill = 0,
            names_prefix = "death_",
            names_sep = ""
          )
        phac <- phac |>
          dplyr::select(-c("hospital_status", "death")) |>
          # add count
          dplyr::group_by(dplyr::across(-"count")) |>
          dplyr::summarise(count = sum(.data$count), .groups = "drop") |>
          dplyr::left_join(
            phac_hs,
            by = c("region", "episode_year", "episode_week", "episode_week_date_start", "episode_week_date_end", "episode_week_group", "gender", "age_group")) |>
          dplyr::left_join(
            phac_d,
            by = c("region", "episode_year", "episode_week", "episode_week_date_start", "episode_week_date_end", "episode_week_group", "gender", "age_group"))
        # write dataset
        utils::write.csv(phac, file.path("extra_data", "phac_individual_ts", "phac_individual_ts.csv"), row.names = FALSE, quote = 1:8)
        # write new release date
        writeLines(as.character(rd_new), file.path("extra_data", "phac_individual_ts", "release_date.txt"))
        rm(tmp, phac, dates, ewg, all_groups, all_rows, phac_hs, phac_d, rd) # clean up
      }
    },
    error = function(e) {
      cat("Error in updating individual-level PHAC dataset:", fill = TRUE)
      print(e)
    }
  )

  ## StatCan excess mortality data
  tryCatch(
    {
      ## check if release date has changed
      rd <- readLines(file.path("extra_data", "statcan_excess_mortality", "release_date.txt"))
      if (length(rd) == 0) rd <- ""
      rd_new <- rvest::read_html("https://www150.statcan.gc.ca/t1/tbl1/en/cv.action?pid=1310078401") |>
        rvest::html_text2() |>
        stringr::str_extract("(?<=Release date: )(\\d{4}-\\d{2}-\\d{2})")
      if (rd_new == rd) {
        # no update required
      } else {
        ## download file
        tmp <- tempfile()
        utils::download.file("https://www150.statcan.gc.ca/n1/tbl/csv/13100784-eng.zip", destfile = tmp)
        statcan <- readr::read_csv(unz(tmp, filename = "13100784.csv"), col_types = readr::cols()) # silently
        # process data
        statcan <- statcan |>
          dplyr::transmute(
            region = dplyr::case_when(
              .data$GEO == "Canada, place of occurrence" ~ "CAN",
              .data$GEO == "Newfoundland and Labrador, place of occurrence" ~ "NL",
              .data$GEO == "Prince Edward Island, place of occurrence" ~ "PE",
              .data$GEO == "Nova Scotia, place of occurrence" ~ "NS",
              .data$GEO == "New Brunswick, place of occurrence" ~ "NB",
              .data$GEO == "Quebec, place of occurrence" ~ "QC",
              .data$GEO == "Ontario, place of occurrence" ~ "ON",
              .data$GEO == "Manitoba, place of occurrence" ~ "MB",
              .data$GEO == "Saskatchewan, place of occurrence" ~ "SK",
              .data$GEO == "Alberta, place of occurrence" ~ "AB",
              .data$GEO == "British Columbia, place of occurrence" ~ "BC",
              .data$GEO == "Yukon, place of occurrence" ~ "YT",
              .data$GEO == "Northwest Territories, place of occurrence" ~ "NT",
              .data$GEO == "Nunavut, place of occurrence" ~ "NU"
            ),
            date = .data$REF_DATE,
            characteristics = dplyr::case_when(
              .data$Characteristics == "Adjusted number of deaths" ~ "adjusted_num_of_deaths",
              .data$Characteristics == "Lower 95% prediction interval of adjusted number of deaths" ~ "adjusted_num_of_deaths_pred_interval_95_lower",
              .data$Characteristics == "Upper 95% prediction interval of adjusted number of deaths" ~ "adjusted_num_of_deaths_pred_interval_95_upper",
              .data$Characteristics == "Expected number of deaths" ~ "expected_num_of_deaths",
              .data$Characteristics == "Lower 95% prediction interval of expected number of deaths" ~ "expected_num_of_deaths_pred_interval_95_lower",
              .data$Characteristics == "Upper 95%prediction interval of expected number of deaths" ~ "expected_num_of_deaths_pred_interval_95_upper",
              .data$Characteristics == "Excess mortality estimate" ~ "excess_mortality_est",
              .data$Characteristics == "Lower 95% prediction interval of excess mortality estimate" ~ "excess_mortality_est_pred_interval_95_lower",
              .data$Characteristics == "Upper 95% prediction interval of excess mortality estimate" ~ "excess_mortality_est_pred_interval_95_upper"
            ),
            value = .data$VALUE
          ) |>
          tidyr::pivot_wider(names_from = .data$characteristics, values_from = .data$value) |>
          # sort by region (CAN first) and date
          dplyr::arrange(dplyr::if_else(.data$region == "CAN", 0, 1), .data$region, .data$date)
        # max date
        statcan <- filter_max_date(statcan)
        # write dataset
        utils::write.csv(statcan, file.path("extra_data", "statcan_excess_mortality", "statcan_excess_mortality.csv"), row.names = FALSE, quote = 1:2)
        # write new release date
        writeLines(as.character(rd_new), file.path("extra_data", "statcan_excess_mortality", "release_date.txt"))
      }
    },
    error = function(e) {
      cat("Error in updating StatCan excess mortality data:", fill = TRUE)
      print(e)
    }
  )

  ## hosp/ICU extra data report
  tryCatch(
    {
      # process
      d <- googlesheets4::read_sheet(
        ss = "1ZTUb3fVzi6CLZAbU3lj6T6FTzl5Aq-arBNL49ru3VLo",
        sheet = "hospital_icu_extra",
      )
      # max date
      d <- filter_max_date(d)
      # write dataset
      utils::write.csv(
        d,
        file.path("extra_data", "hospital_icu_extra", "hospital_icu_extra.csv"),
        row.names = FALSE,
        na = "")
    },
    error = function(e) {
      print(e)
      cat("Error in updating hospital/ICU extra data report:", fill = TRUE)
    }
  )

  ## NS Respiratory Watch extra data
  tryCatch(
    {
      d <- read_d("raw_data/reports/ns/ns_respiratory_watch_report.csv")
      d <- d |>
        dplyr::transmute(
          .data$date_start,
          .data$date_end,
          .data$region,
          .data$sub_region_1,
          .data$cases,
          .data$deaths,
          .data$hosp_admissions,
          .data$icu_admissions
        )
      # max date
      d <- filter_max_date(d, "date_end")
      # write dataset
      utils::write.csv(
        d,
        file.path("extra_data", "ns_extra_respiratory_watch", "ns_extra_respiratory_watch.csv"),
        row.names = FALSE,
        na = "")
    },
    error = function(e) {
      cat("Error in updating NS Respiratory Watch extra data:", fill = TRUE)
      print(e)
    }
  )
}
