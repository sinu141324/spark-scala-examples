package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession

object wordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("hari").getOrCreate()
    val input = spark.sparkContext.textFile("C:\\Users\\harik\\IdeaProjects\\Practice\\src\\main\\Amulya")
    val output =  input.flatMap(line=>line.split("")).map(word=>(word,1)).reduceByKey(_+_)
    output.foreach(println)



  }

}
