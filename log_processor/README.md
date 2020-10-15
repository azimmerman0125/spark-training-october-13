** Tasks:

1. Implement the pipeline.create_alarm_df() method

It is up to you what you implement as a business logic. The operation team would like to see an alarm
record in a specific folder when a certain condition has been met. For example:

If the number of non 200 response codes exceeds 500 for any resource, they would like to see.

2. The security team provides a set of IP addresses (see sample_data folder). Your task is to see, if there 
are requests coming from any IP addresses from this list in a high volume (e.g. >100 per hour).

If yes you should write a data record in a specific folder in json format which contains the IP address, the date, the hour and the number of requests

Running spark driver: 
spark-submit --packages mysql:mysql-connector-java:5.1.39 driver.py
