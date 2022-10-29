package com.sparkbyexamples.spark.dataframe.examples

import org.apache.spark.sql.SparkSession

object dataFrameJoins {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().master("local").appName("Craetedataframe").getOrCreate()
    import spark.implicits._
    val service = Seq((1, "hk", "cse", 45000),
      (2, "pk", "eee", 50000),
      (3, "rk", "eee", 45000),
      (4, "vnr", "mech", 55000))
    val serviceColumns = Seq("id", "name", "dept", "salary")
    val servicedf = service.toDF(serviceColumns:_*)
    servicedf.show()

    val produt = Seq((1, "pkr", "ap", 4000), (2, "hkr", "ts", 5000), (3, "anr", "ts", 5500))
    val productColumns = Seq("id_product", "p_name", "dept", "price")
    val produtdf = produt.toDF(productColumns: _*)
    produtdf.show(false)

    servicedf.join(produtdf,servicedf("dept")===produtdf("dept"),"left").show()


    val customer = Seq((1, "anr", "india", 45000), (2, "vnr", "america", 40000))
    val customerColumns = Seq("id", "name", "country", "salary")
    val customerdf = customer.toDF(customerColumns: _*)
    customerdf.show(false)

    servicedf.join(produtdf,servicedf("dept")===produtdf("dept"),"left")
      .join(customerdf,servicedf("id")===customerdf("id"),"left")
      .show(false)


  }

}
