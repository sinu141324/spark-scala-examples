package Csvread

import org.apache.spark.sql.SparkSession

object Map_VS_Flatmap {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("MapVSflatmap").getOrCreate()

    val data = Seq("Project Gutenberg’s",
      "Alice’s Adventures in Wonderland",
      "Project Gutenberg’s",
      "Adventures in Wonderland",
      "Project Gutenberg’s")
    import spark.sqlContext.implicits._

    val df = data.toDF("data")
    df.show()


  }

}
