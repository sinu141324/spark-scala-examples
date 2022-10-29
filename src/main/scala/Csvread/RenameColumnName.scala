package Csvread

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object RenameColumnName {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("karthik").getOrCreate()

  /*  val data = Seq(("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
      ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
      ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
      ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico"))
    import spark.sqlContext.implicits._

    val df = data.toDF("Product", "Amount", "Country")
    // df.show(5,false)


    //Changing existing column name with new name

    val newColumnNameDF =  df.withColumnRenamed("amount","price")
    newColumnNameDF.show()
    newColumnNameDF.printSchema()
    print(newColumnNameDF)*/


    val data = Seq(Row(Row("James ", "", "Smith"), "36636", "M", 3000),
      Row(Row("Michael ", "Rose", ""), "40288", "M", 4000),
      Row(Row("Robert ", "", "Williams"), "42114", "M", 4000),
      Row(Row("Maria ", "Anne", "Jones"), "39192", "F", 4000),
      Row(Row("Jen", "Mary", "Brown"), "", "F", -1)
    )
    import spark.sqlContext.implicits._



    val schema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("dob", StringType)
      .add("gender", StringType)
      .add("salary", IntegerType)


    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.printSchema()

    df.show()


  }

}
