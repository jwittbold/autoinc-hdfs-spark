from pyspark.sql import SparkSession
from pyspark import SparkContext


# build spark session
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('Autoinc-Spark') \
    .getOrCreate()


# location of file stored within HDFS
hdfs_data_file = '/input/data.csv'

# create rdd from contents of file
raw_rdd = spark.sparkContext.textFile(hdfs_data_file)
# print(raw_rdd.collect())


def extract_vin_kv(line):
    """
    Splits CSV data and assigns needed variables based on index value.
    Parameters: line (rdd)
    Returns: composite key / value tuple with VIN as key and incident_type, make, and year as value.
    """
    record = line.split(',')
    incident_type = record[1]
    vin_number = record[2]
    make = record[3]
    year = record[5]
    vehicle_info = (incident_type, make, year)
    return (vin_number, vehicle_info)

# maps extract_vin_kv() function to each element in rdd and sets resulting rdd to 'vin_kv' variable
vin_kv = raw_rdd.map(lambda x: extract_vin_kv(x))
# vin_kv.foreach(print)


def populate_make(records):
    """
    Loops through records and propagates make and year value from 'I' records to 'A' records.
    Parameters: records (rdd)
    Returns: tuple containing make and year values for 'A' records.
    """
    make = ''
    year = ''
    for record in records:
        if record[0] == 'I':
            make = record[1]
            year = record[2]
    return [(make, year) for record in records if record[0] == 'A']

# groups records in rdd by key (vin) and maps populate_make() function to each element
enhanced_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))
# enhanced_make.foreach(print)


def extract_make_kv(record):
    """
    Creates a tuple of vehicle 'make-year' and count of 1 for use in reduceByKey function
    Parameters: record (rdd)
    Returns: (make-year, 1)
    """
    return (record[0] + '-' + record[1], 1)


# map extract_make_kv() function to each element in rdd
make_kv = enhanced_make.map(lambda x: extract_make_kv(x))
# make_kv.foreach(print)


# reduceByKey() on rdd to return sum of total accidents for each vehicle with accident record
accident_report = make_kv.reduceByKey(lambda x,y: x+y)
# accident_report.foreach(print)


# cast and concatenate rdd tuple values as string, write file to HDFS as .txt 
accident_report_txt = accident_report.map(lambda record: str(record[0]) + ', ' + str(record[1])) \
    .saveAsTextFile('hdfs:///output/make_year_count_spark')