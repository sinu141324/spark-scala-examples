package Csvread

import org.apache.spark.sql.SparkSession

object Date_toDF {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("date_timestamp").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import org.apache.spark.sql.functions._
    import spark.sqlContext.implicits._
    val data1 = Seq(("2019-01-23"))

    val df1 = data1.toDF("Input")
    val df2 = df1.select(current_date() as ("current_date"),
      col("Input"),
      date_format(col("Input"), "MM-dd-yyyy").as("format")
    )
    df2.show()

    val data2 = Seq(("04/13/2019"))
      val df3 =data2 .toDF("Input")
      .select(col("Input"),

      )
        df3.show()


  }

}
