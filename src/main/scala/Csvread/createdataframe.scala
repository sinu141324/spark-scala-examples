package Csvread

import org.apache.spark.sql.SparkSession

object createdataframe {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("joins").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    val emp = Seq((1, "Smith", -1, "2018", "10", "M", 3000),
      (2, "Rose", 1, "2010", "20", "M", 4000),
      (3, "Williams", 1, "2010", "10", "M", 1000),
      (4, "Jones", 2, "2005", "10", "F", 2000),
      (5, "Brown", 2, "2010", "40", "", -1),
      (6, "Brown", 2, "2010", "50", "", -1)
    )
    val empColumns = Seq("emp_id", "name", "superior_emp_id", "year_joined",
      "emp_dept_id", "gender", "salary")
    import spark.sqlContext.implicits._
    val empDF = emp.toDF(empColumns: _*)
    empDF.show(false)

  }
}