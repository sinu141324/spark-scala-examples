package Csvread

import com.sparkbyexamples.spark.rdd.RDDCache.sc
import org.apache.spark.sql.SparkSession

object Practise {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("practise").getOrCreate()
    val x = sc.parallelize(Array("b","a","c"))
    val y = x.map(z=>(z,1))
    println(x.collect())


  }

}
