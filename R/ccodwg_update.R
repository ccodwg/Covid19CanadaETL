#' Perform update of CovidTimelineCanada
#'
#' @param email The email to authenticate with Google Sheets. If not provided,
#' manual authentication will be requested.
#' @param path Alternative to email authentication. JSON identifying service
#' account (e.g., file path to JSON). See \code{\link[googlesheets4]{gs4_auth}}.
#' @param skip_raw_update If TRUE, skip updating raw datasets and just assemble final datasets. Default: FALSE.
#' @param skip_extra_update If TRUE, skip updating extra datasets. Default: FALSE.
#'
#' @export
ccodwg_update <- function(email = NULL, path = NULL, skip_raw_update = FALSE, skip_extra_update = FALSE) {

  # setup log file
  log_path <- file.path(tempdir(), "COVID19CanadaETL.log")
  # create empty log
  if (file.exists(log_path)) {
    file.remove(log_path)
  }
  file.create(log_path)

  # authenticate with Google Drive and Google Sheets
  cat("Authenticating with Google Drive...", fill = TRUE)
  auth_gd(email, path)
  cat("Authenticating with Google Sheets", fill = TRUE)
  auth_gs(email, path)

  # download, process, upload, write raw datasets
  update_raw_datasets(skip_raw_update)

  # assemble and write final datasets
  assemble_final_datasets()

  # assemble and write extra datasets
  extra_datasets(skip_extra_update)

  # load error log
  error_log <- readLines(log_path)

  # filter error log
  error_log <- error_log[error_log != ""] # remove blank entries ("")
  error_log <- error_log[!grepl("^\u2716 Request failed \\[\\d{3}\\]. Retry", error_log)] # remove Google Sheets retries

  # email error log if not blank
  if (!identical(error_log, character(0))) {
    error_log <- paste(error_log, collapse = "\n")
    # print
    cat("\n", error_log, fill = TRUE, sep = "")
    # send email
    send_email(subject = "CCODWG update errors", body = error_log)
  } else {
    cat("\nNo errors to report.", fill = TRUE)
  }
}
