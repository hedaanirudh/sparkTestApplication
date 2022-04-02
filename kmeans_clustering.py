from numpy import array
from math import sqrt
from pyspark.mllib.clustering import KMeans, KMeansModel
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

class KMeansClustering:
    def __init__(self) -> None:
        pass

    def get_spark_context_all_core(self):
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("k_means_streaming_example") \
            .getOrCreate()
        return spark.sparkContext

    def create_clusters(self, input_data):
        def test(l):
            array([float(x) for x in l.split(' ')]) 

        def rmse(pt):
            c = clusters.centers[clusters.predict(pt)]
            return sqrt(sum([y**2 for y in (c - pt)]))

        input_data = input_data.map(lambda l: array([float(x) for x in l.split(' ')]))
        clusters = KMeans.train(input_data, 2, initializationMode="random", maxIterations=8)
        # clusters = KMeans.train(input_data, 5, initializationMode="random", maxIterations=13)
        RMSEE = input_data.map(lambda pt: rmse(pt))
        RMSEE = RMSEE.reduce(lambda m, n: m + n)
        print("Root Mean Squared Error = " + str(RMSEE))

if __name__ == "__main__":
    kmeans = KMeansClustering()
    sc = kmeans.get_spark_context_all_core()
    start_time = time.time()
    input_data = sc.textFile("input_kmeans_small2_dataset.txt")
    kmeans.create_clusters(input_data)
    end_time = time.time()
    print("\n\n ************************************")
    print("\n\n Total execution time for large dataset: ", end_time - start_time)
    print("\n\n ************************************")
    sc.stop()