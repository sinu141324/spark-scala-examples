/*
package Csvread

import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.io.Source

object Struct_type {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Struct_type").getOrCreate()

    val simpleData = Seq(Row("James ", "", "Smith", "36636", "M", 3000),
      Row("Michael ", "Rose", "", "40288", "M", 4000),
      Row("Robert ", "", "Williams", "42114", "M", 4000),
      Row("Maria ", "Anne", "Jones", "39192", "F", 4000),
      Row("Jen", "Mary", "Brown", "", "F", -1)
    )

    val simpleSchema = StructType(Array(
      StructField("firstname", StringType, true),
      StructField("middlename", StringType, true),
      StructField("lastname", StringType, true),
      StructField("id", StringType, true),
      StructField("gender", StringType, true),
      StructField("salary", IntegerType, true)
    ))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData), simpleSchema)
    df.printSchema()
    df.show()


    val structureData = Seq(
      Row(Row("James ", "", "Smith"), "36636", "M", 3100),
      Row(Row("Michael ", "Rose", ""), "40288", "M", 4300),
      Row(Row("Robert ", "", "Williams"), "42114", "M", 1400),
      Row(Row("Maria ", "Anne", "Jones"), "39192", "F", 5500),
      Row(Row("Jen", "Mary", "Brown"), "", "F", -1)
    )

    val structureschema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("id", StringType)
      .add("gender", StringType)
      .add("salary", IntegerType)
    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData), structureschema)
    df2.printSchema()
    df2.show()


    val url = ClassLoader.getSystemResource("schema.json")
    val schemaSource = Source.fromFile(url.getFile).getLines.mkString
    val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]
    val df3 = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData), schemaFromJson)
    df3.printSchema()
    df3.show()


    val arrayStructureData = Seq(
      Row(Row("James ", "", "Smith"), List("Cricket", "Movies"), Map("hair" -> "black", "eye" -> "brown")),
      Row(Row("Michael ", "Rose", ""), List("Tennis"), Map("hair" -> "brown", "eye" -> "black")),
      Row(Row("Robert ", "", "Williams"), List("Cooking", "Football"), Map("hair" -> "red", "eye" -> "gray")),
      Row(Row("Maria ", "Anne", "Jones"), null, Map("hair" -> "blond", "eye" -> "red")),
      Row(Row("Jen", "Mary", "Brown"), List("Blogging"), Map("white" -> "black", "eye" -> "black"))
    )

    val arrayStructureSchema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("hobbies", ArrayType(StringType))
      .add("properties", MapType(StringType, StringType))

    val df5 = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema)
    df5.printSchema()
    df5.show()


  }

}
*/
