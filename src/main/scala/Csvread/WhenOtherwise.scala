package Csvread

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, when}

object WhenOtherwise {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Case when").getOrCreate()

    import spark.sqlContext.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val data = List(("James", "", "Smith", "36636", "M", 60000),
      ("Michael", "Rose", "", "40288", "M", 70000),
      ("Robert", "", "Williams", "42114", "", 400000),
      ("Maria", "Anne", "Jones", "39192", "F", 500000),
      ("Jen", "Mary", "Brown", "", "F", 0))


    val cols = Seq("first_name", "middle_name", "last_name", "dob", "gender", "salary")
    val df = spark.createDataFrame(data).toDF(cols:_*)
    df.show()

    val df2 = df.withColumn("new_gender",when(col("gender")==="M","Male")
    .when(col("gender")==="F","Female")
    .otherwise("Unknown"))
    df2.show()


    val df3 = df.withColumn("new_gender",
      expr("case when gender = 'M' then 'Male' " +
        "when gender = 'F' then 'Female' " +
        "else 'Unknown' end"))
    df3.show()


    val df4 = df.select(col("*"),
      expr("case when gender = 'M' then 'Male' " +
        "when gender = 'F' then 'Female' " +
        "else 'Unknown' end").alias("new_gender"))
    df4.show()


  }


}
