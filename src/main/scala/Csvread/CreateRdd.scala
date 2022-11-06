package Csvread

import com.sparkbyexamples.spark.SparkContextExample.sparkContext
import com.sparkbyexamples.spark.SparkContextExample.sparkContext.parallelize
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CreateRdd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("createRdd").getOrCreate()
    val rdd:RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5))
    val rddCollect:Array[Int] = rdd.collect()
    println("Number of partions: "+rdd.getNumPartitions)
   println("Action : First Element: "+rdd.first())
    println("Action: RDD converted to Arry[Int] : ")
    rddCollect.foreach(println)



  }


}
