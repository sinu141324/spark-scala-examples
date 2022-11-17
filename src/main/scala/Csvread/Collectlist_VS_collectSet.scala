package Csvread

import org.apache.spark.sql.functions.{collect_list, collect_set}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Collectlist_VS_collectSet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // creating a dataframe
    val data = Seq(Row("ayyappa", "hadoop"), Row("ayyappa", "spark"),
      Row("ayyappa", "scala"), Row("ayyappa", "sql"), Row("gopi", "hadoop"), Row("gopi", "spark"), Row("gopi", "spark")
      , Row("gopi", "scala"), Row("gopi", "sql"))
    val schema = new StructType().add("name", StringType)
      .add("course_name", StringType)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.show()

    val collect_list_Df = df.groupBy("name").agg(collect_list("course_name")).as("course_name")
    collect_list_Df.show(false)
    collect_list_Df.printSchema()


    val collect_set_Df = df.groupBy("name").agg(collect_set("course_name")).as("course_name")
    collect_set_Df.show(false)
    collect_set_Df.printSchema()



  }

}
