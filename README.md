# autoinc-hdfs-spark

This project is a the follow up to a previous project located here:\
https://github.com/jwittbold/hadoop_streaming_mapreduce
This previous project utilized a series of MapReduce jobs to return records of vehicle accident reports.


In this project we will work with the same dataset and achieve the same output, but we will be executing it with Spark while working with a Spark RDD (Resilient Distributed Dataset). This project highlights the advantages of working with Spark and the simplicity it allows for when compared to traditional MapReduce jobs. 

The following assumes you have a working installation of Hadoop and Spark. Please refer to the previoulsy mentioned project for info regarding setting up Hadoop. 

## File on HDFS
As seen below, our data file is already present within HDFS as it was added in the previous project.
![hdfs_input_dir](/screenshots/hdfs_input_dir.png)


## Contents of data.csv
We can take a look at the contents of our data file by running:\
```hdfs dfs -cat /input/data.csv ```
![hdfs_data_file](/screenshots/hdfs_data_file.png)


## Raw RDD
![raw_rdd_collect](/screenshots/raw_rdd_collect.png)

![extract_vin_kv_after](screenshots/extract_vin_kv_after.png)

![enhanced_make_after](screenshots/enhanced_make_after.png)

![make_kv_after](screenshots/make_kv_after.png)


![after_reduceByKey](screenshots/after_reduceByKey.png)

![final_output](screenshots/final_output.png)

![hdfs_output_dir_before](screenshots/hdfs_output_dir_before.png)

![hdfs_output_dir_after](screenshots/hdfs_output_dir_after.png)

![hdfs_make_year_count_spark_dir](screenshots/hdfs_make_year_count_spark_dir.png)

![hdfs_cat_part-00000](screenshots/hdfs_cat_part-00000.png)
