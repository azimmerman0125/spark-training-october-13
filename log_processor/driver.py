import pyspark
from pyspark.sql import SparkSession
import utils
import pipeline

# INPUT_FOLDER = 's3://...'
INPUT_FOLDER = 'sample_data/unit-test2.log'
EVIL_IP_INPUT = 'sample_data/ip-list.txt'
# OUTPUT_FOLDER = 's3://...'
OUTPUT_FOLDER_LOG = 'output'
OUTPUT_FOLDER_STATS = 'output'

# Constructing the Spark Context ...
sc = utils.gen_spark_context(local=False)
spark = SparkSession(sc)

# Handling input
# input_rdd = sc.parallelize([ ... ])
input_rdd = sc.textFile(INPUT_FOLDER)
evil_ip_list_rdd = sc.textFile(EVIL_IP_INPUT)

# Building the pipeline
pipeline = pipeline.LogProcessorPipeline(sc, spark)
(log_df, stat_df, alarm_df, malicious_ip_df) = pipeline.build_pipeline(input_rdd, evil_ip_list_rdd)

# Writing down the data
log_df.write \
    .format('parquet') \
    .mode('overwrite') \
    .partitionBy('date') \
    .save(OUTPUT_FOLDER_LOG)
   # .saveAsTable('table_name')

#stat_df.write \
#    .format('jdbc') \
#    .option('url', 'jdbc:mysql://localhost/spark_test') \
#    .option('dbtable', 'log_report') \
#    .option('user', 'spark') \
#    .option('driver', 'com.mysql.jdbc.Driver') \
#    .option('password', 'spark123') \
#    .option('numPartition', '1') \
#    .save()

stat_df.write \
    .format('parquet') \
    .mode('overwrite') \
    .save(OUTPUT_FOLDER_STATS)

alarm_df.write \
    .format('json') \
    .mode('overwrite') \
    .option('compression', 'gzip') \
    .save('alarms')

malicious_ip_df.coalesce(1).write \
    .format('json') \
    .mode('overwrite') \
    .save('malicious_activities')

sc.stop()
