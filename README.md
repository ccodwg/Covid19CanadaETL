# Covid19CanadaETL: A Pipeline for Canadian COVID-19 Data

A package used to automate time series data collection for the
[COVID-19 Canada Open Data Working Group](https://opencovid.ca/)
[dataset](https://github.com/ccodwg/Covid19Canada).

The data automation pipeline involves three packages:

* [`Covid19CanadaData`](https://github.com/ccodwg/Covid19CanadaData): Loads the live version of the desired dataset (denoted by its UUID in [dataset.json](https://github.com/ccodwg/Covid19CanadaArchive/blob/master/datasets.json)) using the function `Covid19CanadaData::dl_dataset`
* [`Covid19CanadaDataProcess`](https://github.com/ccodwg/Covid19CanadaDataProcess): Processes a given dataset into a standardized data format
* `Covid19CanadaDataETL`: Coordinates the downloading, processing and uploading of all relevant datasets

`Covid19CanadaDataETL` requires [`Covid19CanadaData`](https://github.com/ccodwg/Covid19CanadaData) and [`Covid19CanadaDataProcess`](https://github.com/ccodwg/Covid19CanadaDataProcess) to be installed to run.

This package has been in use since June 1, 2021.
