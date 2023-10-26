# Qualyfi Assessment: Data Engineering Star Schema

![Solution Design](https://github.com/PhanaiPundisondQualyfi/QualyfiAssessment/assets/147846684/67f16abc-cc43-49c4-aee4-69c06986e736)

The assessment was to utilise the tools and skills gained during the Qualyfi Accelerator program.

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

## 3. Data Engineering: ADF, Databricks
Created a pipeline showing a copy data activity and 3 Databricks notebook using Azure Data Factory.
- Copy data activity (CopyToLanding)

Moved data from source container to the landing container
- Notebook 1 (LandingToBronze)

Read data from landing container to notebook using abfss:// and a storage access key. Added columns to the dataframe; FileName, and CreatedOn, then renamed columns containing a "vendor_id" column to "VendorID" for partition. Mounted the data to DBFS and then wrote the data to the bronze container in Delta format and partitioned by "VendorID".

- Notebook 2 (BronzeToSilver)

Read data from bronze container to notebook using abfss:// and a storage access key. Defined a schema based on the dataset columns in the bronze container. Created a "for loop" to map the dataset and standardise columns that had similar names onto the defined schema. Applied constraints to the total_amount and trip_distance columns. Wrote the data to the silver container in Delta format and partitioned by "PULocationID".

- Notebook 3 (SilverToGold)

Read data from bronze container to notebook using abfss:// and a storage access key.

## 4. Data Visualisation

## 5. GitHub Repository

Created a GitHub Repo to document and track changes for all files.

## Documentation

[Terraform](https://developer.hashicorp.com/terraform/docs)

[Azure Blob Storage](https://learn.microsoft.com/en-gb/azure/storage/blobs/)

[Azure Data Factory](https://learn.microsoft.com/en-us/azure/data-factory/)

[Azure Databricks](https://learn.microsoft.com/en-gb/azure/databricks/)

[Microsoft Power BI](https://learn.microsoft.com/en-us/power-bi/)



