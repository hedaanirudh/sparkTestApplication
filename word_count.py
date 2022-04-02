import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
import time
import shutil

class SparkTestApplication:

    def __init__(self):
        self.a = 1

    def get_spark_context_all_core(self):
        spark = SparkSession.builder \
            .master("local[6]") \
            .appName("WordCount") \
            .getOrCreate()
        return spark.sparkContext

    def get_spark_context_four_core(self):
        spark = SparkSession.builder \
            .master("local[4]") \
            .appName("WordCount") \
            .getOrCreate()
        return spark.sparkContext

    def get_spark_context_one_core(self):
        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("WordCount") \
            .getOrCreate()
        return spark.sparkContext

    def get_spark_context_two_core(self):
        spark = SparkSession.builder \
            .master("local[2]") \
            .appName("WordCount") \
            .getOrCreate()
        return spark.sparkContext

    def word_count_program(self, sc, input_file):
        # read data from text file and split each line into words
        words = sc.textFile(input_file).flatMap(lambda line: line.split(" "))
        words = words.filter(lambda a: a!='')
        # count the occurrence of each word
        word_counts = words.map(lambda word: (word, 1))
        word_counts = word_counts.reduceByKey(lambda a, b: a + b).sortByKey()
        # save the counts to output
        myfile = "output"
        ## If file exists, delete it ##
        if os.path.isdir(myfile):
            shutil.rmtree(myfile)
        word_counts.saveAsTextFile("output/")

if __name__ == "__main__":
    # create Spark context with necessary configuration
    sparkApplication = SparkTestApplication()
    # sc = sparkApplication.get_spark_context_all_core()
    start_time = time.time()
    sc = sparkApplication.get_spark_context_one_core()
    # sc = sparkApplication.get_spark_context_two_core()
    # sc = sparkApplication.get_spark_context_four_core()
    # sc = sparkApplication.get_spark_context_all_core()

    sparkApplication.word_count_program(sc, "input_word_count_large_dataset.txt")
    end_time = time.time()
    print("\n\n ************************************")
    print("\n\n Total execution time for large dataset: ", end_time - start_time)
    print("\n\n ************************************")
 
    # sparkApplication.word_count_program(sc, "input_word_count_large_dataset.txt")
    sc.stop()