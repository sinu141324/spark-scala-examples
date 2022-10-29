package Csvread

import org.apache.spark.sql.SparkSession

object emptydataframe_create {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("hari").getOrCreate()


    import spark.implicits._
    val columns = Seq("language","users_count")
    val data = Seq(("",""))
    val df = data.toDF(columns:_*)

    df.show()
  }

}
