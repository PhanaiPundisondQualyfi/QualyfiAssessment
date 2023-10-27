# Qualyfi Assessment: Data Engineering Star Schema

![Solution Design](https://github.com/PhanaiPundisondQualyfi/QualyfiAssessment/assets/147846684/67f16abc-cc43-49c4-aee4-69c06986e736)

## TABLE OF CONTENTS
- [1. Data Modelling](#1-data-modelling)
- [2. Infrastructure Setup](#2-infrastructure-setup)
- [3. Data Engineering With ADF & Databricks](#3-data-engineering-with-adf--databricks)
- [4. Data Visualisation](#4-data-visualisation)
- [5. GitHub Repository](#5-github-repository)
- [Documentation](#documentation)

The assessment utilised the tools and skills gained during the Qualyfi Accelerator program.

The project is an end-to-end data engineering solution designed to process and analyse the NYC yellow taxi dataset in the years, 2019, 2014, and 2010. It caters to data professionals, analysts, and stakeholders interested in gaining insights from this dataset. The key components and functionalities of this project includes:
- Data Model Designs via draw.io
- Infrastructure Setup via Terraform
- Data Pipeline and Orchestration via Azure Data Factory
- Data Transformation via Azure Databricks
- Visualisation via Microsoft Power BI

## 1. Data Modelling

Created three different database designs; conceptual, logical, physical to model the structures, form, and relationships of information. The designs were based on samples of the dataset visualised via VSCode.
- The conceptual database design consisted of only the entities and relationships.
- The logical database design consisted of the entities, relationships, and attributes.
- The physical database design consisted of the entities, relationships, named attributes, keys, data types, and null values.

All 3 designs can be viewed in the Designs directory.

## 2. Infrastructure Setup
Used terraform to set up the storage containers (landing, bronze, silver, gold) in the Azure storage account. A terraform script was created using VSCode and can be viewed in the infrastructure.tf file.

## 3. Data Engineering With ADF & Databricks
Created a pipeline showing a copy data activity and 3 Databricks notebook using Azure Data Factory.
- Copy data activity (CopyToLanding)

Moved data from source container to the landing container using the copy data tool in ADF. Use the LinkedService to connect ADF to the Azure storage blob.

- Notebook 1 (LandingToBronze)

Move and transformed data files from a "landing" container to a "bronze" container in an Azure Storage account using PySpark. Read CSV files for taxi trip data from various years (2019, 2014, and 2010), added columns for the file name and processing timestamp, renameed the "vendor_id" column to "VendorID" for specific years, and partitioned the data by "VendorID" before saving it in Delta Lake format in the "bronze" container.

- Notebook 2 (BronzeToSilver)

Moved data from a "bronze" container to a "silver" container within an Azure Storage account using PySpark and applying data transformations. Configured access to the Azure Storage account, specified the paths for reading data from the "bronze" container for three different VendorIDs (1, 2, and 4), and set the destination path in the "silver" container. Only dataset from the year, 2019, was processed due to issues with the delta format and defined schema. The script loaded data from the "bronze" container into three DataFrames, filtered out records where "total_amount" is non-null and greater than 0, and "trip_distance" is greater than 0 for each VendorID. The filtered data is then written to the "silver" container, partitioned by "PULocationID." The mode("overwrite") and mode("append") options ensure that data is either replaced or appended in the "silver" container.

- Notebook 3 (SilverToGold)

Moved data from a "silver" container to a "gold" container in Azure Storage using PySpark and created dimension tables and a fact table. Configured access to the Azure Storage account, loaded data from the "silver" container into the df_silver DataFrame, and then constructed dimension tables such as "DimTime," "DimLocation," and "DimVendor" by selecting and transforming specific columns. A fact table is created by selecting relevant columns, including the addition of a unique identifier column ("TripID"). All these tables are saved in Delta Lake format in the "gold" container.

- Notebook 4 (GoldToDBFS)

Moved data from "gold" container to the Hive data store, enhancing accessibility for Power BI integration. Configured access to the Azure Storage account and reading "gold" tables such as "DimTime", "DimLocation", "DimVendor", and "FactTrip". Proceede to create a database ("database_pp") in Hive if it doesn't exist already. Subsequently, it  writes the dimension tables and fact table to this database in Hive. 

## 4. Data Visualisation

Connected Databricks to Microsoft Power BI using the provided function, Partner Connect. Before, opening the Power BI file, loaded the gold tables into the hivestore to access via Power BI. Created 4 visualisations; 2 bar charts,  1 pie chart, and 1 line chart to help answer 4 questions.

- Vendor Ranked by Most Trips Taken: VTS
- Which Vendor Is Charging The Lowest?: VTS
- Most Common Pick Up Locations: Midtown Manhattan
- When Is The Best Time To Get A Taxi On Christmas Eve?: 3AM

![PowerBIVisuals](https://github.com/PhanaiPundisondQualyfi/QualyfiAssessment/assets/147846684/ba91224a-db1d-4752-88c7-4613956372c1)

## 5. GitHub Repository

Created a GitHub Repo to document and track changes for all files.

## Documentation

[Terraform](https://developer.hashicorp.com/terraform/docs)

[Azure Blob Storage](https://learn.microsoft.com/en-gb/azure/storage/blobs/)

[Azure Data Factory](https://learn.microsoft.com/en-us/azure/data-factory/)

[Azure Databricks](https://learn.microsoft.com/en-gb/azure/databricks/)

[Microsoft Power BI](https://learn.microsoft.com/en-us/power-bi/)



