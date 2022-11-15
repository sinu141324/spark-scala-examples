package Csvread

import org.apache.spark.sql.SparkSession

object Dateformat {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("json").getOrCreate()

    val df = spark.read.json("F:\\Downloads\\MOCK_DATA.json")
    df.printSchema()
    df.show()
    // To know the no of rows in a dataframe
    println(df.count())

    // To know the no of columns in a dataframe
    println(df.columns.size)

    // To get the no of partitions of a dataframe
    println(df.rdd.getNumPartitions)

    spark.sparkContext.setLogLevel("ERROR")


  }

}
