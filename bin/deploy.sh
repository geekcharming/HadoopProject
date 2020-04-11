#!/usr/bin/env bash

########################################################
# Declared Variables
########################################################

counter=0
option=$1
data_directory=/home/cloudera/workspace/HadoopProject/DataFolder
hdfs_directory=/salesdb
path_to_files=/home/cloudera/workspace/HadoopProject/SQL
raw_db_name=zeros_and_ones_salesdb_raw
db_name=zeros_and_ones_salesdb

########################################################
# Functions
########################################################

display_help() {
    echo "Usage: $) [option...] " >&2
    echo
    echo "  -h,   --help                               display help contents"
    echo "  -g,   --get_data                           get sales data from Amazon S3 storage for Deliverable 2 Step 1.1"
    echo "  -l,   --load                               load data to hdfs for Deliverable 2 Step 1.2"
    echo "  -crt, --create_raw_tables                  create DB and external tables for deliverable 2 step 1.3,1.4"
    echo "  -qa,  --quality_analysis                   view quality analysis queries used 2 step 2.1"
    echo "  -cpt, --create_parquet_tables              create salesdb and parquet tables for deliverable 2, step 2.2,2.3"
    echo "  -csv, --create_salesdb_views               create views for deliverable 2, step 2.4"
    echo "  -psp, --create_product_salesdb_partition   create partitioned table for deliverable 3, step 1a,1b"
    echo "  -cpv, --create_partitioned_view            create partitioned view for deliverable 3, step 2a,2b"
    echo "  -rsp, --create_product_region_partition    create partitioned table for deliverable 3, step 3a,3b"
    echo "  -rmv, --drop_all                           drop tables,views,all data from HDFS & VM for deliverable 3 step 4"
    exit 1
}

get_data() {
    echo "getting sales data from https://csci5751-2020sp.s3-us-west-2.amazonaws.com/sales-data/salesdata.tar.gz"
    echo "------------------------------------------------------------------------------------------------------"
    wget https://csci5751-2020sp.s3-us-west-2.amazonaws.com/sales-data/salesdata.tar.gz
    echo "unzipping sales data"
    tar -xvzf salesdata.tar.gz
    mv salesdb $data_directory
    echo "moved raw data to a folder"
    echo Deleting the zip file
    sudo rm -f salesdata.tar.gz
}

do_hdfs() {
  echo creating hdfs directory $hdfs_directory
  sudo -u hdfs hdfs dfs -mkdir $hdfs_directory

  for file in "$data_directory"/*
     do
     echo processing "$file"
     filename=$(basename -- "$file")
     echo creating hdfs directory: $hdfs_directory/"${filename%.*}"
     sudo -u hdfs hdfs dfs -mkdir $hdfs_directory/"${filename%.*}"
     echo puting file $data_directory/$filename to hdfs directory: $hdfs_directory/"${filename%.*}"
     sudo -u hdfs hdfs dfs -put $data_directory/$filename $hdfs_directory/"${filename%.*}"/

   done
   echo Changing owner of hdfs directory to hive
   sudo -u hdfs hdfs dfs -chown -R hive:hive $hdfs_directory
}

create_raw() {
   echo creating raw tables on csv files
   impala-shell -f "$path_to_files"/ddl_create_sales_raw.sql
   echo "filename------"
   echo "$path_to_files"/ddl_create_sales_raw.sql

}

create_sales_parquet() {
   echo creating parquet tables as Select
   impala-shell -f "$path_to_files"/ddl_create_sales_parquet.sql

}

create_views_salesdb(){
  echo creating views on salesdb
  impala-shell -f "$path_to_files"/create_views.sql
}

do_quality_analysis(){
  echo running quality analysis queries
  impala-shell -f "$path_to_files"/quality_check.sql
}

create_sales_partition() {
   echo creating parquet partitioned tables
   impala-shell -f "$path_to_files"/product_sales_partition.sql
}

create_partitioned_view(){
  echo creating view from partitioned table
  impala-shell -f "$path_to_files"/customer_monthly_sales_2019_partitioned_view.sql
}

create_sales_partition_by_region() {
   echo creating parquet partitioned tables
   impala-shell -f "$path_to_files"/product_region_sales_partition.sql

}

drop_all(){
  echo drop tables,views,all data from HDFS and VM

  echo Dropping databse and cascade tables
  impala-shell -q "DROP DATABASE IF EXISTS $raw_db_name CASCADE;"
  impala-shell -q "DROP DATABASE IF EXISTS $db_name CASCADE;"

  echo Removing views
  impala-shell -q "Drop VIEW IF EXISTS $db_name.customer_monthly_sales_2019_view;"
  impala-shell -q "Drop VIEW IF EXISTS $db_name.top_ten_customers_amount_view;"
  impala-shell -q "Drop VIEW IF EXISTS $db_name.customer_monthly_sales_2019_partitioned_view;"

  echo Removing raw sales data from HDFS
  sudo -u hdfs hdfs dfs -rmr $hdfs_directory

  echo Removing raw sales data from VM
  sudo rm -rf $data_directory
}


########################################################
# Run Time Commands
########################################################

while [ $counter -eq 0 ]; do
    counter=$(( counter + 1 ))

    case $option in
      -h | --help)
          display_help
          ;;

      -g | --get_data)
          echo "Geting data and unzipping file"
          get_data
          ;;

      -l | --load)
          echo "Loading data to HDFS"
          do_hdfs
          ;;

      -crt | --create_raw_tables)
          echo "Creating raw external tables"
          create_raw
          ;;

      -qa | --quality_analysis)
        echo "Running QA Queries"
        do_quality_analysis
        ;;

      -cpt | --create_sales_tables_parquet)
          echo "Creating parquet format tables"
          create_sales_parquet
          ;;

       -csv | --create_salesdb_views)
          echo "Creating view on salesdb"
          create_views_salesdb
          ;;

      -psp | --create_sales_partitioned_table)
          echo "Creating partitioned sales table"
          create_sales_partition
          ;;

       -cpv | create_partitioned_view )
          echo "Creating view on salesdb"
          create_partitioned_view
          ;;

      -rsp | --create_sales_partitioned_table)
          echo "Creating partitioned sales table"
          create_sales_partition_by_region
          ;;

       -rmv | --drop_all)
          echo "drop tables,views,all data from HDFS & VM"
          drop_all
          ;;

      --) # End of all options
          shift
          break
          ;;

      -*)
          echo "Error: Unknown option: $1" >&2
          ## or call function display_help
          exit 1
          ;;

      *)  # No more options
          break
          ;;

    esac
done