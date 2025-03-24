# Sample Data for Taxi journeys January 2021
Demo to showcase a simple medallion style ETL using Jupyter Notebooks with pyspark framework

## Requirements:
Docker image: jupyter/pyspark-notebook 

## Instructions:

### Resources
RawData:
- green_tripdata_2021-01.csv
- yellow_tripdata_2021-01.csv

### Task 1:
Import the Yellow and Green data files, convert them to Parquet format and save them to the PipelineData/Bronze directory

### Task 2:
From PipelineData/Bronze directory generate 3 files:
1. GY_PRE_VALIDATION (as parquet)
    - Union of Yellow and Green bronze data into a single schema and used for validation
3. GY_VALID (as parquet)
    - All journeys must have at least 1 passenger. Journies with no passengers are not valid. All records should have a Vendor Id. Journies with no Vendor Id should have their ID set to 999 before saving to the valid data set.
4. GY_INVALID (as csv for analysis)
    - All invalid data must be stored here along with Duplicate values (based off Pickup location, Pick up Time, Drop off time, drop off location and vendor) with 2 additional columns (IsValid and IsDuplicate) to specify which category the row falls into.
  
All silver tables must have the following fields with the exception of GY_INVALID that has 2 additional columns specified above:

|Silver           |           Green                 |                         Yellow|
|-----------------|---------------------------------|-------------------------------|
|VendorId         |           VendorID              |          VendorID             |
|PickUpDateTime   |           lpep_pickup_datetime  |          tpep_pickup_datetime |
|DropOffDateTime  |           lpep_dropoff_datetime |          tpep_dropoff_datetime|
|PickUpLocationId |           PULocationID          |          PULocationID         |
|DropOffLocationId|           DOLocationID          |          DOLocationID         |
|PassengerCount   |           passenger_count       |          passenger_count      |
|TripDistance     |           trip_distance         |          trip_distance        |
|TipAmount        |           tip_amount            |          tip_amount           |
|TotalAmount      |           total_amount          |          total_amount         |

### Task 3:
Create 2 gold aggregate tables as csv with the following columns:

File 1:
    Locations
    The total fares by pickup location (sum of fares)
    The total tips by pickup location (sum of tips)
    The average distance by pickup location (average distance)
    The average distance by dropoff location (average distance)
File 2:
    Vendors
    The total fare by vendor (sum of fares)
    The total tips by vendor (sum of tips)
    The average fare by vendor (average fares)
    The average tips by vendor (average tips)

### Task 4:
Create a testing script that read from GY_VALID found in silver and applies the correct aggregate calculations.

## File Structure
- PipelineData
  - Bronze
  - Silver
  - Gold
- rawdata
- 01 bronze.ipynb
- Duplicate silver Analysis
- 02 silver.ipynb
- 03 gold.ipynb
- 04 gold test.ipynb
- Test.ipynb
- utils.py

## Overview

The pipeline is structured to run in order:
- 01 bronze.ipynb
- 02 silver.ipynb
- 03 gold.ipynb
- 04 gold test.ipynb

The utils.py file provides classes that are necessary to run the pipeline notebooks. Although classes for a pipeline of this size is excessive the purpose is to provide an example of a pipeline that has reuseable operations allowing the ability to scale up and open opportunities for performance improvements and standardisation. Through the use of object orientated programming I am showcasing some examples of the 4 python principles (encapsulation, inheritance, polymorphism, and abstraction).

### 01 Bronze

This is a like for like etl the raw csv files from RawData are extracted converted to parquet and stored in the bronze directory.

A run_test method is available to provide row and column count as well as a list of columns for each dataset. We can use this for testing/comparison later.

### Duplicate silver Analysis

The purpose of this notebook is to analyse the cause of valid duplicate values. The full analysis is not provided to keep the notebook simple however the final successful check is included showing what we need to apply in the 02 silver Notebook. It was found that TotalAmount has negative floating numbers

#### IMPORTANT: An assumption has been made that refunds are not included in this dataset, based on this TotalAmount cannot negative. Depending on the true meaning of the data this assumption had to be made. If negative values are in fact valid (so refunds are included) the final gold output would be very different.

### 02 silver

The first transformation staged focused on combinining datasets and applying some validation steps, we will be left with 3 silver datasets:
- GY_Pre_validation: union of datasets (only for columns that have data with the same representation) stored as parquet and used for generating GY_Validated and GY_Invalidated.
- GY_Validated: we apply the following steps to build this dataset as parquet
  1. Remove rows where PassengerCount = 0 or null
  2. Set VendorId to 999 if null
  3. Remove duplicates unique row using Composite key of VendorId, PickUpDateTime, DropOffDateTime, PickUpLocationId, DropOffLocationId
- GY_Invalidated: we store a dataset of invalid rows as csv
  1. PassengerCount = 0 or null

### 03 gold

#### IMPORTANT: Once data from silver is validated the aggregation for gold is relatively simple to create however some considerations are to be made:
- File 1: Locations requires aggregation across 2 different columns (Pickup and Dropoff). This will cause null values where a location has only ever had a Pickup (or inversely a Dropoff)

### 04 gold test

Testing gold was a little more involved as these are aggregate tables. The aim is to read from both silver and gold and compare the results by filtering on a testing value (e.g. VendorId = 1) and comparing the aggregate results both from silver and from gold. For gold we are just retriving the value as it is already calculated but for silver the goal is to "manually" do the calculations by collecting all values from silver and applying the appropriate aggregation calculation. This testing method is just printing the results so they can be compared visually however the test has been set up in a way that can easily be automated to run through a sample list (or the entire dataset) and store the results of any mismatches in a testing results table for further investigation.

Note: some values may not be exact matches due to rounding being applied to make value more readable (this may be especially true for average checks).