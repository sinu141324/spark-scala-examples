package Csvread

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection.universe.show

object Dateformat {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("json").getOrCreate()

    val df = spark.read.json("F:\\Downloads\\MOCK_DATA.json")
    df.printSchema()
    df.show()

    spark.sparkContext.setLogLevel("ERROR")




  }

}
