#Author: Krishna Nyshadham
#Description: shell script to define the spark submit configurations and run the weather_data_spark_job
#The job reads the csv data located hdfs and outputs the country with highest mean temperature, country with maximum consecutive tornado/funnel clouds and the country with maximum average wind speed in the year 2019
#usage: spark-submit <path of the py script> --master=yarn --num-executors='' --executor_memory='' (add as many parameters as you want for future requirements

#!/bin/bash

spark-submit /home/hadoop/weather_data_spark_job.py                                   
