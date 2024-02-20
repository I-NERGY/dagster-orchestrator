# I-NERGY: FlexDR orchestrated workflows

## Repository information
This repository contains the workflows supporting the FlexDR application. \
FlexDR requires the scheduled execution of these workflows. \
To achieve this, I-NERGY project utilises [Dagster](https://dagster.io/) as the orchestrator engine.

## Workflows within FlexDR
This section aims to describe the workflows involved in FlexDR.
A critical process for FlexDR is the prediction of day-ahead clusters.
This task is closely connected with the corresponding day-ahead prediction
of electricity load for the smart meters involved in the energy community.
The aforementioned tasks require at first the smart meters data to be in appropriate form.

### Smart meters data processing
Smart meters timeseries data are stored in the I-NERGY mongo database. 
However, these sensors' data may contain outliers and the time intervals are not fixed.
This fact, requires the preprocessing of the smart meters data according to the needs of 
the DeepTSF load forecasting service.
In brief this workflow implements the following steps:
* Loads smart meter data from I-NERGY mongo db using the relevant defined resource. 
Period to load smart meters data is a configurable value between 1 and 14 (the latest days).
* Fixes naming issues in smart meters ids, resamples each smart meters' data and removes outliers.
* Uploads data in I-NERGY MinIO object storage.

The following picture describes the workflow described above. \
<img src="assets/dagster-flexdr-smdata.PNG" alt="Smart meters preprocessing job" width="500">

### Day ahead forecasting
This workflow is responsible to generate the assignments entities of the FlexDR backend given
the preprocessed, by the aforementioned workflow, data for the last 7-14 days.
To do so, this workflow follows these steps: 
* Loads the preprocessed smart meters data
* Transforms these data to create the payload of the http request that will be sent to the DeepTSF
load forecasting service
* Loads the registered smart meters from the FlexDR mongo database.
* Utilises the DeepTSF load forecasting service API to acquire the day-ahead load forecasts
* Given the day-ahead load forecasts utilises the FlexDR clustering service API to get the day-ahead cluster predictions
* Given both the day-ahead load and clusters forecasts, uses the FlexDR backend API to register the "assignments" into FlexDR application.
* Makes use of success hook that stores the forecasts in MinIO for possible analytics use.

The following picture describes the workflow described above. \
<img src="assets/dagster-flexdr-forecasts.PNG" alt="Day ahead forecasts job" width="500">

### Historical smart meters data processing


### Workflows orchestrated execution