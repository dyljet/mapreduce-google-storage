# mapreduce-google-storage

This is a map reduce program which takes massive amounts of temperature data in an excel file, and plots difference between max and min temperature of each day at a given location.

Makes use of google path storage to run the file:

Google storage paths
Jar file: gs://hadoopy_weather/Untitled.jar 
Input: gs://hadoopy_weather/2018.csv 
Output: gs://hadoopy_weather/output 
Code to execute program:
gcloud dataproc jobs submit hadoop --cluster mapreduce-cluster --region=europe-west2 --jar gs://hadoopy_weather/Untitled.jar -- mapReduce gs://hadoopy_weather/2018.csv gs://hadoopy_weather/output
