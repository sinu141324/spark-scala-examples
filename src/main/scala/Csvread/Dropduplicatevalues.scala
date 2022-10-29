package Csvread

import org.apache.spark.sql.SparkSession

object Dropduplicatevalues {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("hari").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    import spark.implicits._

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    val df = simpleData.toDF("employee_name", "department", "salary")
    //df.show()

    // Use distinct() – Remove Duplicate Rows on DataFrame

   /* val distinctdf = df.distinct()
    println("Distinct count:"+distinctdf.count())
    distinctdf.show()
    //distinctdf.count()*/


    // Distinct using dropDuplicates row same as distinct
    val df1=df.dropDuplicates().show()

    //Distinct using dropDuplicate in particular columns

    val dropdf = df.dropDuplicates("department","salary")

    dropdf.show()










  }

}
