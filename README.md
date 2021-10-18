# Covid19CanadaETL: A Pipeline for Canadian COVID-19 Data

A package used to automate the collection of time series data for COVID-19 in Canada.

The automated data collection from this package is used to assemble the [`Covid19Canada`](https://github.com/ccodwg/Covid19Canada) dataset from the [COVID-19 Canada Open Data Working Group](https://opencovid.ca/). It is also used in the [Timeline of COVID-19 in Canada](https://github.com/ccodwg/CovidTimelineCanada), one component of the **[What Happened? COVID-19 in Canada](https://whathappened.coronavirus.icu/)** project.

The data automation pipeline involves three packages:

* [`Covid19CanadaData`](https://github.com/ccodwg/Covid19CanadaData): Loads the live version of the desired dataset (denoted by its UUID in [dataset.json](https://github.com/ccodwg/Covid19CanadaArchive/blob/master/datasets.json)) using the function `Covid19CanadaData::dl_dataset`
* [`Covid19CanadaDataProcess`](https://github.com/ccodwg/Covid19CanadaDataProcess): Processes a given dataset into a standardized data format
* `Covid19CanadaDataETL`: Coordinates the downloading, processing and uploading of all relevant datasets

`Covid19CanadaDataETL` requires [`Covid19CanadaData`](https://github.com/ccodwg/Covid19CanadaData) and [`Covid19CanadaDataProcess`](https://github.com/ccodwg/Covid19CanadaDataProcess) to be installed to run.

This package has been in use since June 1, 2021.
