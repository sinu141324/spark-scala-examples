package Csvread

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object PivotTable {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("karthik").getOrCreate()
    val data = Seq(("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
      ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
      ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
      ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico"))
    import spark.sqlContext.implicits._

    val df = data.toDF("Product","Amount","Country")
   // df.show(5,false)


    //val pivotDF= df.groupBy("Product").pivot("Country").sum("Amount")
    //pivotDF.show()


    val countries = Seq("USA", "China", "Canada", "Mexico")
    val pivotDF = df.groupBy("Product").pivot("Country", countries).sum("Amount")
    pivotDF.show()


    //unpivot
    val unPivotDF = pivotDF.select($"Product",
      expr("stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,sum)"))
      .where("sum is not null")
    unPivotDF.show()


  }

}
