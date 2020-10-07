#Author: Krishna Nyshadham
#date: October 6 2020
#Description: The below code is a spark job which reads weather data as csv along with station codes and country codes metadata. With this data, it enables the user to findout the country with normal or anamolous weater patterns. The code uses pyspark libraries and is massively parrallel and scalable.
#usage: the job is triggered with the help od a shell script submit_spark_job.sh which can be configured to utilize customized resource configurations for the job.

#import necessary spark libraries.
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as f

#Build the spark session with app name pre loaded. Most of the inputs like input file locations in hdfs, appnames etc can be parameterized
spark=SparkSession.builder.appName('process_weather_data').getOrCreate()
#Read the countrylist metadata into a DF. This contains the mapping of shot names and full names of countries. This can also be broadcast for better performance in future if required.
df1=spark.read.csv("/user/data/countrylist.csv",header=True)
#Read the stationlist meta data into a DF. This contains data about station codes and the countries mapping.This can also be broadcast for better performance in future if required.
df2=spark.read.csv("/user/data/stationlist.csv",header=True)
#Join the station codes and country codes to get the full details about countries and the station codes int he countries.
df_joined=df1.join(df2,df1['country_abbr']==df2['country_abbr']).select(df1['country_abbr'],df1['country_full'],df2['stn_no'])
#Read the sensor weather data
df3=spark.read.csv('/user/data/weather_data/*',header=True)
#Form a consolidated base dataframe with all the station codes country codes country names and weather readings.
weather_data=df3.join(df_joined,df3['stn---']==df_joined['stn_no'])
#Creating temp views to make code mode readable and leverage on SQL syntax for spark sql.
weather_data.createOrReplaceTempView("weather_data")
#Create 1,0 flag for having a tornado or funnel recorderd yes or no scenario.
weather_data_with_torn_flag=spark.sql("select *,substr(frshtt,6,1) as torn_yes from weather_data")
weather_data_with_torn_flag.createOrReplaceTempView("weather_data_with_torn_flag")
#Create 3 windows w1,w2,w3 for taking 3 different groups of dataframe to subtracte w1 row_number from the w2 row_number. this will give us the sequence of streaks. Then use the 3rd window to leverage on t
he w3 row number and select the necessary streak statistics.
w1=Window.partitionBy(weather_data_with_torn_flag['country_full']).orderBy(weather_data_with_torn_flag['yearmoda'])
w2=Window.partitionBy(weather_data_with_torn_flag['country_full'],weather_data_with_torn_flag['torn_yes']).orderBy(weather_data_with_torn_flag['yearmoda'])
res=weather_data_with_torn_flag.withColumn('grouped',f.row_number().over(w1)-f.row_number().over(w2))
w3 = Window.partitionBy(res['country_full'],res['torn_yes'],res['grouped']).orderBy(res['yearmoda'])
consecutive_res = res.withColumn('streak_0',f.when(res['torn_yes'] == 1,0).otherwise(f.row_number().over(w3))).withColumn('streak_1',f.when(res['torn_yes'] == 0,0).otherwise(f.row_number().over(w3)))
consecutive_res.createOrReplaceTempView("consecutive_res")
ranked_sequence_streak=spark.sql("select *,dense_rank() over(order by streak_1 desc) as dense_rank_val from consecutive_res")
ranked_sequence_streak.createOrReplaceTempView("ranked_sequence_streak")
#the resulting output od show is the country details with most consecutive tornado/funnel warnings.
spark.sql("select * from ranked_sequence_streak").show()
#Filter out the null values. In this case 9999.9 values to reduce the skewness of averages.
average_temperature_by_country_ordered=spark.sql("select avg(temp) as average_temp_by_country,country_full from weather_data where temp !='9999.9' group by country_full order by avg(temp) desc")
average_temperature_by_country_ordered.show()
#Perform similar filter on windspeed data and take averaged result based on dense_rank() to take second max average windspeed value.
average_windspeed_ordered_by_country=spark.sql("select avg(wdsp) as average_wind_speed_by_country, country_full from weather_data where wdsp!='999.9' group by country_full order by avg(wdsp) desc")
average_windspeed_ordered_by_country.createOrReplaceTempView("average_windspeed_ordered_by_country")
average_windspeed_ordered_by_country_ranked=spark.sql("select *, dense_rank() over (order by average_wind_speed_by_country desc) as rank from average_windspeed_ordered_by_country")
average_windspeed_ordered_by_country_ranked.createOrReplaceTempView("average_windspeed_ordered_by_country_ranked")
spark.sql("select * from average_windspeed_ordered_by_country_ranked where rank=2").show()
