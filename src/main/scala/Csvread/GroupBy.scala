package Csvread

import org.apache.spark.sql.SparkSession

object GroupBy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("GroupBy").getOrCreate()

    import spark.implicits._
    val simpleData = Seq(("James", "Sales", "NY", 90000, 34, 10000),
      ("Michael", "Sales", "NY", 86000, 56, 20000),
      ("Robert", "Sales", "CA", 81000, 30, 23000),
      ("Maria", "Finance", "CA", 90000, 24, 23000),
      ("Raman", "Finance", "CA", 99000, 40, 24000),
      ("Scott", "Finance", "NY", 83000, 36, 19000),
      ("Jen", "Finance", "NY", 79000, 53, 15000),
      ("Jeff", "Marketing", "CA", 80000, 25, 18000),
      ("Kumar", "Marketing", "NY", 91000, 50, 21000)
    )

    val df = simpleData.toDF("employee_name", "department", "state", "salary", "age", "bonus")
    df.show()

    val df1 = df.groupBy("department").sum("salary").show()
    val df2 = df.groupBy("department").count()
    df2.show()


    val df3 = df.groupBy("department").min("salary")
    df3.show()

    val df4 = df.groupBy("department").max("salary")
    df4.show()

    val df5 = df.groupBy("department").avg("salary")
    df5.show()

    val df6 = df.groupBy("department").mean("salary")
    df6.show()


    //GroupBy on multiple columns
    val df7 = df.groupBy("department", "state")
      .sum("salary", "bonus")
    df7.show(false)


    import org.apache.spark.sql.functions._
    val df8 = df.groupBy("department")
      .agg(
        sum("salary").as("sum_salary"),
        avg("salary").as("avg_salary"),
        sum("bonus").as("sum_bonus"),
        max("bonus").as("max_bonus"))
    df8.show(false)


  }


}
