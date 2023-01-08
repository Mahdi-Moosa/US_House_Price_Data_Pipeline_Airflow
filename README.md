# US House Price Data Pipeline Airflow

This is an Airflow orchestration repo of US House Price Data Warehouse. The data warehouse is similar to the one described [here](https://github.com/Mahdi-Moosa/US_Housing_Price_ETL).

## Project goals & data source 

This Airflow orchasteration generates a Data Warehouse to perform analytics on US housing. The warehouse is designed to perform analytics based on zip codes. Data for the warehouse comes from five different sources:
1. FHFA appraisal data ([UAD Dataset](https://www.fhfa.gov/DataTools/Pages/UAD-Dashboards.aspx)).
2. [Realtor](https://www.realtor.com/research/data/) research data.
3. [Redfin](https://www.redfin.com/news/data-center/) research data.
4. [Zillow](https://www.zillow.com/research/data/) housing index.
5. Zip code data from [GeoNames](http://download.geonames.org/export/zip/).

*Additional data (on census tract to zipcode mapping) comes from [U.S. Census Bureau](https://www.census.gov/) and [U.S. Department of
Housing and Urban Development](https://www.hud.gov/).

## Steps
* Step 1: Prepare appraisal data for zip code-based query (extract data from different sources, transform and join/merge).
* Step 2: Extract and transform realtor, redfin, zillow & zip code data.
* Step 3: Merge/ join transformed FHFA appraisal data, realtor data, redfin data and zillow data to prepare master house price dataset.
* Step 4: Prepare zip code dimension table.
* Step 5: Validate the master house price table & zipcode table for data integrity.

*Notes: At present, the airflow orchestration saves data in pre-specified local directories. This can easily be reconfigured to store data in Amazon S3 bucket and/or Amazon Redshfit. Please see: https://github.com/Mahdi-Moosa/US_Housing_Price_ETL*

Files/ folders in this repo: 
* dags: folder contains .py script to run the Apache Airflow DAG.
* docker-compose.yaml: docker-compose yaml file (for Apache Airflow; run in Windows 10 Pro, Docker Desktop). yaml file was modified to run in the aforementioned OS environment. 

## Graph view of the Airflow Tasks:

![My Image](images/Airflow_Scheme.png)
