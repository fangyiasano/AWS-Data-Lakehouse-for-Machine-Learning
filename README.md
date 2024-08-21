# AWS Data Lakehouse for Human Balance Analytics

## Project Overview

This repository contains the implementation of a data lakehouse solution for the STEDI Step Trainer sensor data. The project focuses on using AWS services and Apache Spark to process and curate data from a motion-sensing device and a mobile app to train a machine learning model to detect steps accurately.

## Objectives

1. Extract and curate data from Step Trainers and a companion mobile app.
2. Utilize AWS Glue, Athena, and S3 to build a scalable data architecture.
3. Ensure data privacy by processing only data shared for research purposes.

## Technologies Used

**AWS Glue**: Data integration service to prepare and load data.  
**AWS Athena**: Interactive query service to analyze data in Amazon S3 using standard SQL.  
**AWS S3**: Object storage service.  
**Apache Spark**: Unified analytics engine for large-scale data processing.  
**Python**: Programming language used for AWS Glue scripts.  

## Key Works:

### Flowchart for the process:
![chart](https://github.com/user-attachments/assets/ba049a68-7d06-4042-b61e-9a9a155a938d)

### Data Extraction and Curation

- **Extract Data**: 
  - Extract data from three primary sources: `customer_landing`, `step_trainer_landing`, and `accelerometer_landing` zones.
- **AWS Glue Jobs**: 
  - Create AWS Glue jobs to sanitize and curate the data.
- **Curated Data Zones**: 
  - Create curated data zones (`Trusted Zone` and `Curated Zone`) for further use.

### Data Lakehouse Setup

- **Data Storage**: 
  - Store and process data in AWS S3.
- **Glue Tables**: 
  - Create Glue tables for each landing zone and query them using AWS Athena.
- **Data Privacy**: 
  - Ensure that only the data from customers who agreed to share their data for research purposes is included in the curated datasets.

### Data Quality Assurance

- **Data Association**: 
  - Ensure that the correct customer data is associated with Step Trainer records.

### Machine Learning Data Preparation

- **Aggregated Tables**: 
  - Create aggregated tables that combine Step Trainer readings and associated accelerometer data for the same timestamp.
- **Curated Machine Learning Data**: 
  - Store the results in a `machine_learning_curated` table for use in training machine learning models.

 ### Relationship between entities:
![data](https://github.com/user-attachments/assets/3041b7f9-ddbf-40a5-bffb-2a19a19f0020)

## AWS Glue Jobs

**customer_trusted.py**: Sanitizes customer data and stores it in the Trusted Zone.
**accelerometer_trusted.py**: Sanitizes accelerometer data and stores it in the Trusted Zone.
**customers_curated.py**: Curates customer data that includes only customers with accelerometer data.
**step_trainer_trusted.py**: Populates the Trusted Zone with Step Trainer records.
**machine_learning_curated.py**: Creates an aggregated table combining Step Trainer and accelerometer data.

## How to Run the Project

1. Set up S3 Buckets: Create S3 directories for customer_landing, step_trainer_landing, and accelerometer_landing zones. Upload the JSON files to the respective directories.

2. Create Glue Jobs: Use the Python scripts in the scripts/ directory to create AWS Glue jobs. Ensure that the jobs are set up to read from the correct S3 paths.

3. Run Athena Queries: Use the SQL scripts to create and query Glue tables in AWS Athena.

4. Monitor Job Execution: Monitor the Glue jobs and ensure they run successfully by checking the output in the S3 Trusted and Curated zones.

## Conclusion

This project demonstrates how to build a data lakehouse solution using AWS Glue, Python, Spark, and AWS Athena. The curated data is prepared for machine learning purposes, ensuring that only relevant and accurate data is used for training models.
