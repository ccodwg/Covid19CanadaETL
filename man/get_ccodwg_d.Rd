% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils.R
\name{get_ccodwg_d}
\alias{get_ccodwg_d}
\title{Get CCODWG data for a particular value and region}
\usage{
get_ccodwg_d(val, region, from = NULL, to = NULL, drop_not_reported = FALSE)
}
\arguments{
\item{val}{The value to read data for. One of "cases" or "deaths".}

\item{region}{Either "all" or the region to read data for.}

\item{from}{Optional. Exclude data prior to this date.}

\item{to}{Optional. Exclude data after this date.}

\item{drop_not_reported}{Should "Not Reported" sub-regions be dropped? Default: FALSE.}
}
\description{
Get CCODWG data for a particular value and region
}
