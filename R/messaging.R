#' Send email via POST request to GitHub Actions API
#'
#' Environmental variable `GITHUB_PAT` must be present.
#'
#' @param subject Email subject.
#' @param body Email body.
#'
#' @export
send_email <- function(subject, body) {

  # check if GitHub PAT is available in environment
  if (Sys.getenv("GITHUB_PAT") != "") {

    # create JSON request body
    json_body <- jsonlite::toJSON(
      list(ref = "main", inputs = list(subject = subject, body = body)),
      auto_unbox = TRUE)

    # send POST request
    httr::POST(
      url = "https://api.github.com/repos/ccodwg/Covid19CanadaBot/actions/workflows/send-email.yml/dispatches",
      body = json_body,
      encode = "raw",
      httr::add_headers(Accept = "application/vnd.github.v3+json"),
      httr::authenticate(
        user = "jeanpaulrsoucy",
        password = Sys.getenv("GITHUB_PAT"))
    )
  } else {
    warning("Cannot send email. GitHub PAT must be available from the environment as GITHUB_PAT.")
  }
}

#' Send Pushover notification via POST request to GitHub Actions API
#'
#' Environmental variable `GITHUB_PAT` must be present.
#'
#' @param message The body of the notification.
#' @param priority Optional. Message priority, an integer from -2 to 2, see: https://pushover.net/api#priority. Defaults to 0 (normal priority).
#' @param title Optional. The title of the notification. If missing, the application's name will be used.
#' @param device Optional. The name of the device to send the notification to. If missing, the notification will be sent to all devices.
#'
#' @export
pushover <- function(message, priority, title, device) {

  # check if GitHub PAT is available in environment
  if (Sys.getenv("GITHUB_PAT") != "") {

    # assemble inputs
    inputs <- list(message = message)
    if (!missing(priority)) {
      inputs['priority'] = as.character(priority)
    }
    if (!missing(title)) {
      inputs['title'] = title
    }
    if (!missing(device)) {
      inputs['device'] = device
    }

    # create JSON request body
    json_body <- jsonlite::toJSON(
      list(ref = "main", inputs = inputs),
      auto_unbox = TRUE)

    # send POST request
    httr::POST(
      url = "https://api.github.com/repos/ccodwg/Covid19CanadaBot/actions/workflows/pushover.yml/dispatches",
      body = json_body,
      encode = "raw",
      httr::add_headers(Accept = "application/vnd.github.v3+json"),
      httr::authenticate(
        user = "jeanpaulrsoucy",
        password = Sys.getenv("GITHUB_PAT"))
    )
  } else {
    warning("Cannot send notification. GitHub PAT must be available from the environment as GITHUB_PAT.")
  }
}
