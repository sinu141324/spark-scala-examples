package Csvread

import org.apache.spark.sql.SparkSession

object Date_toDF {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("date_timestamp").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import org.apache.spark.sql.functions._
    import spark.sqlContext.implicits._
    Seq(("04/13/2019"))
      .toDF("Input")
      .select(col("Input"),
        to_date(col("Input"), "MM/dd/yyyy").as("to_date")
      ).show()

  }

}
