package Csvread

import org.apache.spark.sql.SparkSession

object JsonCreateDataFrame {



  def main(args: Array[String]): Unit = {


     val spark = SparkSession.builder().master("local").getOrCreate()




  }

}
