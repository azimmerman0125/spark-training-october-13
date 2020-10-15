import pyspark
from pyspark.sql import SparkSession
import pipeline
import utils

def test_access_log_df_pipeline(input_rdd):
    df = pipeline.create_access_log_df(input_rdd)
    df.show()

if __name__ == '__main__':
    sc = utils.gen_spark_context(local=True)
    spark = SparkSession(sc)
    pipeline = pipeline.LogProcessorPipeline(sc, spark)

    # Unit test #1
    input_rdd = sc.textFile('sample_data/unit-test.log')
    test_access_log_df_pipeline(input_rdd)

    sc.stop()
