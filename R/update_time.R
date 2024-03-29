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
      update_time <- lubridate::now("America/Toronto")
      cat("Update time in America/Toronto is:", as.character(update_time), fill = TRUE)
      update_time <- update_time %>% format.Date("%Y-%m-%d %H:%M %Z")
      cat("Formatted update time is:", update_time, fill = TRUE)
      cat(paste0(update_time, "\n"), file = "update_time.txt")
      return(update_time)
    },
    error = function(e) {
      print(e)
      cat("Error in writing update time", fill = TRUE)
    }
  )
}
