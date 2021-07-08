# autoinc-hdfs-spark

This project is a the follow up to a previous project located here:\
https://github.com/jwittbold/hadoop_streaming_mapreduce \
This previous project utilized a series of MapReduce jobs to return records of vehicle accident reports.


In this project we will work with the same dataset and achieve the same output, but we will be executing it with Spark while working with a Spark RDD (Resilient Distributed Dataset). This project highlights the advantages of working with Spark and the simplicity it allows for as compared to traditional MapReduce jobs. 

The following assumes you have working installations of Hadoop and Spark. Please refer to the previoulsy mentioned project for info regarding setting up Hadoop. 

## File on HDFS
As seen below, our data file is already present within HDFS as it was added in the previous project.\
```hdfs dfs -ls /input```

![hdfs_input_dir](/screenshots/hdfs_input_dir.png)


## Contents of data.csv
We can take a look at the contents of our data file by running:\
```hdfs dfs -cat /input/data.csv ```

![hdfs_data_file](/screenshots/hdfs_data_file.png)


## Raw RDD
Within ```autoinc_spark.py``` we have built a SparkSession and using SparkContext we have created an RDD from \
```/input/data.csv``` 

We can view the contents by collecting the raw RDD and the output will appear as below:

![raw_rdd_collect](/screenshots/raw_rdd_collect.png)


## Mapping Values
We map the ```extract_vin_kv()``` function to each element within the RDD in order to return records formated as: \
(VIN, (incident_type, make, year)) tuple.

![extract_vin_kv_after](screenshots/extract_vin_kv_after.png)

## GroupByKey, FlatMap populate_make() function
Because only 'I' records contain full vehicle information, but we only want accident, 'A' records, we must propagate 'make' and 'year' values from 'I' to 'A' records. We can achieve this by using Sparks groupByKey() and flatMap() method to apply our populate_make() function on records sharing the same key.
The result is shown below, four accident records consisting of vehilce make and year.

![enhanced_make_after](screenshots/enhanced_make_after.png)

## Extract Make, map extract_make_kv() function
The next step creates a composite key from make-year and outputs records as a tuple containing make-year and count set to 1. \
The count element will be used in the following reduceByKey step. 

As shown below, vehicle records are formatted as (make-year, 1)

![make_kv_after](screenshots/make_kv_after.png)

## ReduceByKey 
Using reduceByKey() method groups records by key and applies custom logic to get sum of values (the '1' in the previous step)
![after_reduceByKey](screenshots/after_reduceByKey.png)

## Final output as string
Our final output is cast as a string and saved to HDFS using .saveAsTextFile \
![final_output](screenshots/final_output.png)

## HDFS output directory before running script
To view the contents: \
```hdfs dfs -ls /output```
HDFS output directory contains output from the previous projects MapReduce jobs.
![hdfs_output_dir_before](screenshots/hdfs_output_dir_before.png)

## HDFS output directory after running script 
HDFS now contains a new directory,  ```/output/make_year_count_spark```  
To view the contents: \
```hdfs dfs -ls /output```

![hdfs_output_dir_after](screenshots/hdfs_output_dir_after.png)

## Files created in HDFS '/output/make_year_count_spark'
To view the contents of the newly created directory:\
```hdfs dfs -ls /output/make_year_count_spark```
![hdfs_make_year_count_spark_dir](screenshots/hdfs_make_year_count_spark_dir.png)

## Results from Spark job
Exploring the contents of the output from the Spark job we can see that the job executed successfully and we have three accident records showing vehicle make, year and total accidents for each vehicle with an accident record.

![hdfs_cat_part-00000](screenshots/hdfs_cat_part-00000.png)
