package Csvread

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Cache_Persistent {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val columns = Seq("Seqno", "Quote")
    val data = Seq(("1", "Be the change that you wish to see in the world"),
      ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
      ("3", "The purpose of our lives is to be happy."))
    val df = data.toDF(columns: _*)
    df.show()
    //Cache the df
    val dfCache = df.cache()
    dfCache.show(false)


//    persist df by default memory & disk

    val dfPersist = df.persist()
    dfPersist.show(false)
//persist memory only


    val dfPersist1 = df.persist(StorageLevel.MEMORY_ONLY)
    dfPersist1.show(false)
//persist MEMORY_ONLY_SER

    val dfPersist2 = df.persist(StorageLevel.MEMORY_ONLY_SER)
    dfPersist2.show(false)


    val dfPersist3 = dfPersist.unpersist()
    dfPersist3.show(false)

//shows dag schedular
    scala.io.StdIn.readLine()


  }

}
