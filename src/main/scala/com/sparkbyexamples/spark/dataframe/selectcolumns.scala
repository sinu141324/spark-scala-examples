package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession

object selectcolumns {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("selectColimns").getOrCreate()


    val data = Seq(("James", "Smith", "USA", "CA"),
      ("Michael", "Rose", "USA", "NY"),
      ("Robert", "Williams", "USA", "CA"),
      ("Maria", "Jones", "USA", "FL")
    )
    val columns = Seq("firstname", "lastname", "country", "state")
    import spark.implicits._
    val df = data.toDF(columns: _*)
    df.show(false)


    df.select("firstname","lastname").show()

    df.select(df("firstname"),df("lastname")).show()


  }





}
