package Csvread

import org.apache.spark.sql.SparkSession

object CreateTempView {
  def main(args: Array[String]): Unit = {

    // Create DataFrame
    val spark = SparkSession.builder
      .master("local")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq(("James", "Smith", "USA", "CA"),
      ("Michael", "Rose", "USA", "NY"),
      ("Robert", "Williams", "USA", "CA"),
      ("Maria", "Jones", "USA", "FL")
    )
    val columns = Seq("firstname", "lastname", "country", "state")
    import spark.implicits._
    val df = data.toDF(columns: _*)
    df.show(false)




    // Create Temporary View/Table
    df.createOrReplaceTempView("Person")
    spark.sql("""select firstname,country from person""").show()


  }

}
