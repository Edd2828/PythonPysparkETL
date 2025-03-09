# Altrata
PySpark Data Dev Test - Taxi journeys January 2021

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

## Assumptions:
- 'Dedupe' = Remove duplicate values
- 'consistant schema' = append both dataset into 1 schema only for columns that are the same for all valid rows

## Overview

The pipeline is structured to run in order:
- 01 bronze.ipynb
- 02 silver.ipynb
- 03 gold.ipynb

The utils.py file provides classes that are necessary to run the pipeline notebooks. Although classes for a pipeline of this size is excessive the purpose is to provide an example of a pipeline that has reuseable operations allowing the ability to scale up and open opportunities for performance improvements and standardisation. Through the use of OOP (object orientated programming) I am showcasing some examples of the 4 python principles (encapsulation, inheritance, polymorphism, and abstraction).

### 01 Bronze

This is a like for like etl the raw csv files from RawData are extracted converted to parquet and stored in the bronze directory.

A run_test method is available to provide row and column count as well as a list of columns for each dataset. We can use this for testing/comparison later

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
- File 2: 'The average fare by vendor' I am assuming this is the average fare amount by vendor

### 04 gold test

Testing gold was a little more involved as these are aggregate tables. The aim is to read from both silver and gold and compare the results by filtering on a testing value (e.g. VendorId = 1) and comparing the aggregate results both from silver and from gold. For gold we are just retriving the value as it is already calculated but for silver the goal is to "manually" do the calculations by collecting all values from silver and applying the appropriate aggregation calculation. This testing method is just printing the results so they can be compared visually however the test has been set up in a way that can easily be automated to run through a sample list (or the entire dataset) and store the results of any mismatches in a testing results table for further investigation.

Note: some values may not be exact matches due to rounding being applied to make value more readable (this may be especially true for average checks).