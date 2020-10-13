import pyspark
from pyspark.sql import SparkSession
import random

num_samples = 100000000

sc = pyspark.SparkContext(appName="Pi")
spark = SparkSession(sc)

def gen_random_point(p):
    # just generate two random x and y coordinates between (0, 1), regardless the input
    x = random.random()
    y = random.random()
    return (x, y)

# point is a tuple (x, y)
def inside_the_circle(point):
    # returns true if the point is in the circle
    x = point[0]
    y = point[1]
    return (x**2 + y**2 < 1)
    
sparkJob = sc.parallelize(range(0, num_samples)) \
    .map(gen_random_point) \
    .filter(inside_the_circle)

points_in_circle = sparkJob.count()

pi_estimate = 4.0 * points_in_circle / num_samples

print(pi_estimate)


