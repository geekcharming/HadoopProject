# CSCI 5751_Hadoop_Project
> Ingest the sales data from the data warehouse into the data lake and prepare it for analysis and consumption.

### Group Name: zeros_and_ones

### Group Members: 
* Ayushi Rastogi,
* Krishna Shravya Gade
* Mourya Karan Reddy
* Roopana Vuppalapati Chenchu

## Table of contents
* [Description](#Description)
* [Technologies](#technologies)
* [Setup Cloudera VM](#setup-cloudera-vm)
* [Deployment Instructions](#deployment-instructions)
* [Rollback Script](#rollback-script)

## Description
The data warehouse has data for Sales, Customers, Employees and Products. Below is the screenshot of data model used. Given data has been cleaned, validated and stored in partitions to facilitate efficient analysis and visualization. 

   ![Sales Data Model](https://github.com/aiBoss/zeroes_and_ones_Hadoop/blob/master/SalesDataModel.png)
  ### Data Clean up & Validation
  * The product has entries with price as zero. These entries have been removed since the price of a product cannot be 0?
  * Few entries had price value with high precision (256.99999999...) These values have been rounded off to two decimals for an   easy visualization to users. 
  ### Data Partitions and Views
  * Views:
  <br/>&nbsp;&nbsp;&nbsp;&nbsp; customer_monthly_sales_2019_view and top_ten_customers_amount_view are created for a quick retrieval of monthy salesin 2019 and top 10 customers. Procedure to run these views is explained in the setup section. 
  * Partitions:
   <br/>&nbsp;&nbsp;&nbsp;&nbsp; Using partitioned views makes the data analysis and visualization more efficient due to multiple reasons. Partitioning divides table entries into distinct groups based on the partition key. Hence when searching for a value in the partitioned table, the number of entries that need to be searched is lesser resulting in a reduced run time. Also, the query can be run in parallel in different partitions, reducing the response time of query. 
   <br/>&nbsp;&nbsp;&nbsp;&nbsp; Below are three partitioned views created as part of the project. It can be observed that, it is more efficient to use 'customer_monthly_sales_2019_partitioned_view' than  'customer_monthly_sales_2019_view' for data retrieval and data visualization due to the partitioning done on sales year and month.
      
      * product_sales_partition: Total sales amount for each product is captured in this table and the data is partitioned on sales year and month
    * customer_monthly_sales_2019_partitioned_view: This table gives monthly sales of each customer in 2019. The data partitioned on year and month.
    * product_region_sales_partition: Regional sales for each product is stored in this table and the data is partitioned on sales year and month

## Technologies
* VirtualBox Cloudera VM - version 5.13.0
  * HDFS
  * Impala

## Setup Cloudera VM
Follow the instructions [here](https://github.com/aiBoss/zeroes_and_ones_Hadoop/blob/master/Cloudera%20VM.pdf) to install and configure Cloudera VM on local machine

## Deployment Instructions
* Download data from [warehouse](https://csci5751-2020sp.s3-us-west-2.amazonaws.com/sales-data/salesdata.tar.gz)
* Unzip the data
* Delete the zip file
* Load the raw data to HDFS
* Create an Impala data base with raw data: zeros_and_ones_sales_raw
* Create another data base with the clean data: zeros_and_ones_sales
* Create sales views 
  how to create ? we have to give run instructions or the sql files/ bash script (TODO)
* Create product_sales_partition
* Create partitioned
* Create product_region_sales_partition

## Rollback Script
* Undo all the steps in one step  (TODO - script to undo)


