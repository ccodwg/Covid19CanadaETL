#' Plot CovidTimelineCanada datasets
#'
#' This function expects that data have already been loaded using \code{\link[Covid19CanadaETL]{load_datasets}}.
#'
#' @param plot_dir Directory to save plots. Defaults to current directory.
#'
#' @export
plot_datasets <- function(plot_dir = ".") {

  # function: plot value
  plot_value <- function(data, type = c("daily", "cumulative"), title,
                         hr = FALSE, hide_negative_values = FALSE, facet_by_pt = FALSE) {
    match.arg(type, choices = c("daily", "cumulative"), several.ok = FALSE)
    if (type == "daily") {y <- "value_daily"} else {y <- "value"}
    if (hr) {
      p <- ggplot2::ggplot(data = data, ggplot2::aes(x = .data$date, y = !!rlang::sym(y), colour = .data$region,
                                   group = paste(.data$region, .data$sub_region_1))) +
        ggplot2::geom_line(alpha = 0.3) +
        ggpubr::theme_pubclean() +
        ggplot2::labs(title = title) +
        ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5)) +
        ggplot2::guides(colour = ggplot2::guide_legend(override.aes = list(alpha = 1)))
    } else {
      p <- ggplot2::ggplot(data = data, ggplot2::aes(x = .data$date, y = !!rlang::sym(y), colour = .data$region, group = .data$region)) +
        ggplot2::geom_line() +
        ggpubr::theme_pubclean() +
        ggplot2::labs(title = title) +
        ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5))
    }
    # hide negative values
    if (hide_negative_values) {
      p <- p + ggplot2::ylim(0, NA)
    }
    # facet by PT
    if (facet_by_pt) {
      p <- p + ggplot2::facet_wrap(~.data$region)
    }
    # return plot
    p
  }

  # function: save value plot
  save_plot <- function(file_name, width = 731, height = 603, units = "px", dpi = 96) {
    ggplot2::ggsave(file.path(plot_dir, file_name), width = width, height = height, units = units, dpi = dpi)
  }

  # generate plots

  ## cases
  plot_value(get("cases_pt"), type = "daily", title = "Daily case data", hide_negative_values = TRUE)
  save_plot("cases_pt.png")
  plot_value(get("cases_hr"), type = "daily", title = "Daily case data (health regions)", hr = TRUE, hide_negative_values = TRUE)
  save_plot("cases_hr.png")

  ## deaths
  plot_value(get("deaths_pt"), type = "daily", title = "Daily death data", hide_negative_values = TRUE)
  save_plot("deaths_pt.png")
  plot_value(get("deaths_hr"), type = "daily", title = "Daily death data (health regions)", hr = TRUE, hide_negative_values = TRUE)
  save_plot("deaths_hr.png")

  ## testing
  plot_value(get("tests_completed_pt"), type = "daily", title = "Daily tests completed")
  save_plot("tests_completed_pt.png")

  ## hospitalizations
  plot_value(get("hospitalizations_pt"), type = "cumulative", title = "Hospitalization data")
  save_plot("hospitalizations_pt.png")

  ## icu
  plot_value(get("icu_pt"), type = "cumulative", title = "ICU data")
  save_plot("icu_pt.png")

  ## vaccine_coverage_dose_1
  plot_value(get("vaccine_coverage_dose_1_pt"), type = "cumulative", title = "Vaccine coverage (dose 1)")
  save_plot("vaccine_coverage_dose_1_pt.png")

  ## vaccine_coverage_dose_2
  plot_value(get("vaccine_coverage_dose_2_pt"), type = "cumulative", title = "Vaccine coverage (dose 2)")
  save_plot("vaccine_coverage_dose_2_pt.png")

  ## vaccine_coverage_dose_3
  plot_value(get("vaccine_coverage_dose_3_pt"), type = "cumulative", title = "Vaccine coverage (dose 3)")
  save_plot("vaccine_coverage_dose_3_pt.png")

  ## vaccine_coverage_dose_4
  plot_value(get("vaccine_coverage_dose_4_pt"), type = "cumulative", title = "Vaccine coverage (dose 4)")
  save_plot("vaccine_coverage_dose_4_pt.png")

  ## vaccine_administration_dose_1
  plot_value(get("vaccine_administration_dose_1_pt"), type = "daily", title = "Vaccine administration (dose 1)", hide_negative_values = TRUE)
  save_plot("vaccine_administration_dose_1_pt.png")

  ## vaccine_administration_dose_2
  plot_value(get("vaccine_administration_dose_2_pt"), type = "daily", title = "Vaccine administration (dose 2)", hide_negative_values = TRUE)
  save_plot("vaccine_administration_dose_2_pt.png")

  ## vaccine_administration_dose_3
  plot_value(get("vaccine_administration_dose_3_pt"), type = "daily", title = "Vaccine administration (dose 3)", hide_negative_values = TRUE)
  save_plot("vaccine_administration_dose_3_pt.png")

  ## vaccine_administration_dose_4
  plot_value(get("vaccine_administration_dose_4_pt"), type = "daily", title = "Vaccine administration (dose 4)", hide_negative_values = TRUE)
  save_plot("vaccine_administration_dose_4_pt.png")

  ## vaccine_administration_total_doses
  plot_value(get("vaccine_administration_total_doses_pt"), type = "daily", title = "Vaccine administration (total_doses)", hide_negative_values = TRUE)
  save_plot("vaccine_administration_total_doses_pt.png")
}
