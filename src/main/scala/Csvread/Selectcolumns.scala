package Csvread

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object Selectcolumns {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder.master("local").appName("SparkByExamples.com").getOrCreate()

    val data = Seq(("James", "Smith", "USA", "CA"),
      ("Michael", "Rose", "USA", "NY"),
      ("Robert", "Williams", "USA", "CA"),
      ("Maria", "Jones", "USA", "FL")
    )


    import spark.implicits._

    val df = data.toDF("firstname", "lastname", "country", "state")
    df//.show(false)
    df.printSchema()



    df.withColumn("Country",lit("USA"))
    df.show()
//
//    //chaining to operate on multiple columns
//    df.withColumn("Country", lit("USA"))
//      .withColumn("anotherColumn", lit("anotherValue"))

    //
//    //selecting columns from data
//    df.select("firstname","lastname").show()
//
//    df.select(df("firstname"),df("lastname")).show()

    //Using col function, use alias() to get alias name

//    df.select(col("firstname").alias("fn"),col("lastname").alias("ln"))
//      df//.show()


    //Show all columns from DataFrame
    df//.select("*").show()











  }
}
