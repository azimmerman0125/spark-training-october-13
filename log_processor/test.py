import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
import pandas as pd
import pipeline
import utils

def get_sorted_data_frame(data_frame, columns_list):
    return data_frame.sort_values(columns_list).reset_index(drop=True)

def test_access_log_df_pipeline(input_rdd):
    df = pipeline.create_access_log_df(input_rdd)

    expected_output = [
        ['109.169.248.247', "12/Dec/2015:18:25:11 +0100", '2015-12-12', 18, 'GET', '/administrator/', 'HTTP/1.1', 200],
        ['109.169.248.247', "12/Dec/2015:18:25:11 +0100", '2015-12-12', 18, 'POST', '/administrator/index.php', 'HTTP/1.1', 200],
    ]

    # Steps for the test:
    # 1. create a pandas dataframe fro the actual and expected data
    # 2. ... then compare these two dfs to each other
    expected_df = pipeline.spark.createDataFrame(expected_output, pipeline.access_log_schema)

    # converting spark dfs to pandas df
    actual = get_sorted_data_frame(df.toPandas(), ['ip', 'ts', 'method', 'resource', 'protocol', 'response'])
    expected = get_sorted_data_frame(expected_df.toPandas(), ['ip', 'ts', 'method', 'resource', 'protocol', 'response'])
    pd.testing.assert_frame_equal(expected, actual, check_like=True)
    print('#### Unit test 1 passed! ######')    

def test_stat_df_pipeline(access_log_df):
    df = pipeline.create_stat_df(access_log_df)
    df.sort(desc('access_count')).show()

if __name__ == '__main__':
    sc = utils.gen_spark_context(local=True)
    spark = SparkSession(sc)
    pipeline = pipeline.LogProcessorPipeline(sc, spark)

    # Unit test #1
    #input_rdd = sc.textFile('sample_data/unit-test.log')
    #test_access_log_df_pipeline(input_rdd)

    # Unit test #2
    input_rdd = sc.textFile('sample_data/unit-test2.log')
    access_log_df = pipeline.create_access_log_df(input_rdd)
    test_stat_df_pipeline(access_log_df)

    sc.stop()
