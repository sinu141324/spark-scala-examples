package Csvread

import org.apache.spark.sql.SparkSession

object DropColumn {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
  }

}
