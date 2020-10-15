import time
import datetime
from pyspark.sql.types import *

## HELPER FUNCTIONS (not spark related) 
#######################################

def get_ip(s):
    return s.split(' ')[0]

def get_timestamp(str):
    s = str.find('[')
    l = str.find(']')
    ts_str = str[s + 1:l]
    #return long(ts)
    return ts_str

def get_header(str):
    s = str.find('"')
    l = str[s + 1:].find('"')
    header = str[s + 1:s + l + 1].split(' ')
    method = header[0] if len(header) > 0 else "malformed"
    resource = header[1] if len(header) > 1 else "malformed"
    protocol = header[2] if len(header) > 2 else "malformed"        
    return (method, resource, protocol)
    
def get_error_code(str):
    f = str.split(' ')
    if len(f) < 9:
        return 0
    try:
        code = int(f[8])
    except ValueError:
        code = 0
    return code

# input: raw access log from the RDD
# output: structured daa: (ip, ts, date, hour, method, resource, protocol, response code)
def process_access_log_line(log_line):
    header = get_header(log_line)
    ts_str = get_timestamp(log_line)
    td = datetime.datetime.strptime(ts_str, "%d/%b/%Y:%H:%M:%S %z")
    date_str = '{}-{}-{}'.format(td.year, td.month, td.day)   
    return (get_ip(log_line), ts_str, date_str, td.hour, header[0], header[1], header[2], get_error_code(log_line))


class LogProcessorPipeline:

    def __init__(self, sc, spark):
        self.sc = sc
        self.spark = spark

    def build_pipeline(self, input_rdd):
        # input_rdd -> access_log_df -> data lake (in PQ) to query raw data when the incident happens
        # access_log_df -> stat_df -> DWH for daily/hourly ~ real-time querying
        # access_log_df -> alarms: notification service (~ json file stored in a folder)

        access_log_df = self.create_access_log_df(input_rdd)
        access_log_df.cache()
        stat_df = self.create_stat_df(access_log_df)
        alarm_df = None
        
        return (access_log_df, stat_df, alarm_df)

    def create_access_log_df(self, rdd):
        self.access_log_schema = StructType([
            StructField('ip', StringType(), True),
            StructField('ts', StringType(), True),
            StructField('date', StringType(), True),
            StructField('hour', IntegerType(), True),
            StructField('method', StringType(), True),
            StructField('resource', StringType(), True),
            StructField('protocol', StringType(), True),
            StructField('response', IntegerType(), True)
        ])

        df = rdd \
            .filter(lambda log_line: len(log_line) > 1) \
            .map(lambda log_line: process_access_log_line(log_line)) \
            .toDF(self.access_log_schema)
        
        return df

    def create_stat_df(self, access_log_df):
        access_log_df.createOrReplaceTempView('access_log_temp')
        stat_df = self.spark.sql("""
        SELECT date, hour, method, resource, response, count(1) as access_count
        FROM access_log_temp
        GROUP BY date, hour, method, resource, response
        """)
        return stat_df

    def create_alarm_df(self, access_log_df):
        return None