package Csvread

import com.sparkbyexamples.spark.dataframe.CastColumnType.simpleData
import com.sparkbyexamples.spark.dataframe.DataTypeExample.arrayType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DataTypes, DoubleType, IntegerType, StringType, StructField, StructType}

object DataType {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("DataType").getOrCreate()


    //StructType
    val structType = DataTypes.createStructType(
      Array(DataTypes.createStructField("fieldName",StringType,true)))

    val simpleSchema = StructType(Array(
      StructField("name",StringType,true),
      StructField("id", IntegerType, true),
      StructField("gender", StringType, true),
      StructField("salary", DoubleType, true)
    ))

    val anotherSchema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("lastname",StringType))
      .add("id",IntegerType)
      .add("salary",DoubleType)


    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(simpleData), simpleSchema)
    df.printSchema()
    df.show()


  }

}
