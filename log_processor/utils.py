import pyspark

def gen_spark_context(local=False):
    conf = pyspark.SparkConf()
    conf.setAppName('LogProcessor')
    if local:
        conf.setMaster('local')
    sc = pyspark.SparkContext(conf=conf)
    return sc

