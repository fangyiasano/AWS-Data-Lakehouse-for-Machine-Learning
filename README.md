# AWS Data Lakehouse for STEDI Human Balance Analytics - My Project Journey

## Project Overview

The STEDI team developed a hardware named STEDI Step Trainer that trains users for balance exercises and collects sensor data to train a machine learning algorithm to detect steps. The device comes with a companion mobile app that collects customer data and interacts with device sensors.

## My Approach

The task was to use the data from the STEDI Step Trainer sensors and the mobile app, curate them into a data lakehouse solution on AWS, so that Data Scientists can train the learning model. Here's how I approached it:

### Data Analysis

I started by understanding the three JSON data sources provided by STEDI - Customer Records, Step Trainer Records, and Accelerometer Records. These records were divided between three separate S3 buckets. 

### AWS Glue Jobs

I created my own S3 directories for `customer_landing`, `step_trainer_landing`, and `accelerometer_landing` zones, and copied the data there as a starting point. I wrote and executed AWS Glue jobs to curate data from these zones. I used AWS Glue to create two Glue tables for two of these landing zones - `customer_landing` and `accelerometer_landing`. I shared the `customer_landing.sql` and `accelerometer_landing.sql` script in this repository.

### AWS Athena Queries

I used AWS Athena to query the tables created in Glue. I took screenshots of each query showing the resulting data, which are available in this repository.

### Data Sanitization

To sanitize the data and only store records of customers who agreed to share their data for research purposes, I created two more Glue jobs. These jobs curated the data into `customer_trusted` and `accelerometer_trusted` tables.

### Data Quality Issue

The customer data had a data quality issue with the serial number, which should be a unique identifier for the STEDI Step Trainer. Due to a defect in the fulfillment website, the same 30 serial numbers were used repeatedly for millions of customers. To resolve this, I sanitized the Customer data and created a Glue Table in the Curated Zone, `customers_curated`, that only includes customers who have accelerometer data and agreed to share their data for research.

### Glue Studio Jobs

Finally, I created two Glue Studio jobs to:

1. Read the Step Trainer IoT data stream and populate a Trusted Zone Glue Table, `step_trainer_trusted`, that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research.
2. Create an aggregated table, `machine_learning_curated`, that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data.

## Final Thoughts

This project gave me hands-on experience with AWS Glue, Athena, S3, and writing Python scripts using AWS Glue and Glue Studio to build a data lakehouse solution. I had the opportunity to tackle a real-world data quality issue and create a system that ultimately helps in training a machine learning model.
