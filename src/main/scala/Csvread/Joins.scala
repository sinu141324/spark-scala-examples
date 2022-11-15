package Csvread

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.spark_partition_id

object Joins {


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

    val dept = Seq(("Finance", 10),
      ("Marketing", 20),
      ("Sales", 30),
      ("IT", 40)
    )

    val deptColumns = Seq("dept_name", "dept_id")
    val deptDF = dept.toDF(deptColumns: _*)
    deptDF.show(false)
    //inner join
    val df = empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "inner")
    df.show()
    //outer,full,fullouter
    val df1 = empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "fullouter")
    df1.show()
    //leftouter join
    val df2 = empDF.join(deptDF,empDF("emp_dept_id")===deptDF("dept_id"),"left")
    df2.show()

    //right ,rightouter
    val df3 = empDF.join(deptDF,empDF("emp_dept_id")===deptDF("dept_id"),"right")
    df3.show()
//get no.of partion
    println(df.rdd.getNumPartitions)
//repartion
    val df_repartion = df.repartition(4)
    println(df_repartion.rdd.getNumPartitions)
//no.of rows per partion
    val no_of_rows_per_partion = df_repartion.groupBy(spark_partition_id).count()
    no_of_rows_per_partion.show()





  }

}
