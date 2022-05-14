#' Write update time
#'
#' The format of the update time is %Y-%m-%d %H:%M %Z and the time zone is
#' America/Toronto.
#'
#' @return The update time as a string.
#' @export
write_update_time <- function() {
  tryCatch(
    {
      cat("Writing update time...", fill = TRUE)
      update_time <- lubridate::with_tz(Sys.time(), tzone = "America/Toronto") %>%
        format.Date("%Y-%m-%d %H:%M %Z")
      cat(paste0(update_time, "\n"), file = "update_time.txt")
      return(update_time)
    },
    error = function(e) {
      print(e)
      cat("Error in writing update time", fill = TRUE)
    }
  )
}
