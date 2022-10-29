import org.apache.spark.sql.catalyst.ScalaReflection.universe.Name
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

package object Csvread {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkByExamples.com")
      .getOrCreate()





    import spark.implicits._

    val schema = StructType(
      StructField("firstName", StringType, true) ::
        StructField("lastName", IntegerType, false) ::
        StructField("middleName", IntegerType, false) :: Nil)

    val colSeq = Seq("firstName", "lastName", "middleName")
    val df = spark.emptyDataFrame
    df.show()
    df.printSchema()












  }

}
