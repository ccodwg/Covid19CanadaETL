.onLoad <- function(libname, pkgname)
{
  # prevent R CMD check from complaining about "."
  utils::globalVariables(".")

  # create environment for package objects
  covid_etl_env <<- new.env(parent = emptyenv())
}
