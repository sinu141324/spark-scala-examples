package Csvread

import org.apache.spark.sql.SparkSession

object csvread {

  def main(args: Array[String]): Unit = {
    //csv read file

    val spark = SparkSession.builder().master("local").appName("hari").getOrCreate()
    val df = spark.read.option("header", true).option("inferschema", true).option("delimiter", ",").csv("F:\\Downloads\\csv file.csv")
    df.show()
    df.printSchema()


    val df1 = spark.read.option("header", true).csv("F:\\Downloads\\csv file.csv")
    df1.show()
    df1.printSchema()

    val df2 = spark.read.option("inferschema", true).csv("F:\\Downloads\\csv file.csv")
    df2.show()
    df2.printSchema()

    val df3 = spark.read.option("delimiter", ",").csv("F:\\Downloads\\csv file.csv")
    df3.show(5, false)
    df3.printSchema()


    val df4 = spark.read.options(Map("header" -> "true", "inferschema" -> "true", "delimiter" -> ",")).csv("F:\\Downloads\\csv file.csv")
    df4.show(2)
    df4.printSchema()

    //to see the DAG we use below property
    // scala.io.StdIn.readLine()


    //reading multiple csv files
    val df5 = spark.read.csv("F:\\Downloads\\csv file.csv", "C:\\Users\\harik\\OneDrive\\Documents\\csv eaxmple.txt")
    df5.show()



    //
    //    //reading multiple csv with different columns i.e one with 5 columns and one with 6 columns
    //    val df_csv_5col_6col = spark.read.csv("C:\\Users\\Admin\\IdeaProjects\\learn-spark\\source-code\\learn-spark\\src\\main\\resources\\practice.csv", "C:\\Users\\Admin\\IdeaProjects\\learn-spark\\source-code\\learn-spark\\src\\main\\resources\\practice.csv")
    //    df_csv_5col_6col.show()


        //creating a dataframe from a directory with multiple csv files
        val df_with_a_directory = spark.read.option("header", true).csv("D:\\intellije")
        df_with_a_directory.show(30,false)
        df_with_a_directory.printSchema()

        // write spark data frame to csv file
       // df.write.mode("overwrite").format("text").save("C:\\Users\\harik\\IdeaProjects\\spark-scala-examples\\src\\main\\resources\\csv example")


        // to get no of partions of a given dataframe
        println(df1.rdd.getNumPartitions)

//        //creating a dataframe with certain columns of another data frame
//        val df1_from_another_df1 = df5.select("id", "first_name", "last_name", "email")
//        df5.show()


        // Filtering a dataframe using between , and operator


        val df6 = spark.read.options(Map("header" -> "true", "inferschema" -> "true", "delimiter" -> ",")).csv("C:\\Users\\harik\\OneDrive\\Documents\\csv file.csv")
        df6.printSchema()
        df6.show()
        df6.filter("salary between '10000' and '20000'").show()


    //
    //    // filtering using '=' operator
    //    df6.filter("place = 'pinnelli'").show()
    //
    //
    //    // filtering using '==' operator
    //    df5.filter("place == 'pinnelli'").show()
    //
    //    // using where() instead of filter both gives the same result
    //    df5.where("place=='dharanikota'").show()
    //
    //
    //    // counting no of rows in a dataframe using count()
    //    println(df5.count())
    //
    //    // printing no of columns of a data frame
    //    println(df5.columns.size)
    //
    //    // another way of printing columns of a data frame
    //    println((df5.columns).length)
    //
    //    //printing both rows and columns of a dataframe
    //    println(df5.count(), df5.columns.size)
    //
    //
    //    // truncate the column values at desired length
    //    val df_with_column_values_at_desired_length = spark.read.option("header", "true").csv("C:\\Users\\Admin\\IdeaProjects\\learn-spark\\source-code\\learn-spark\\src\\main\\resources\\direcytory_with_multiple_csv_files\\file2.csv")
    //    df_with_column_values_at_desired_length.show(2, 1)
    //    df_with_column_values_at_desired_length.show(2, 100)
    //
    //    /*   // let's create a data frame with null values and custom schema
    //       val data_for = List(("tiger analytics", "ayyappa", "sse", 1500000), ("winwire", "ayyappa", "sde", 1300000), ("impetus", "tiru", "sse", 1300000), ("legato", "gopi", "sse", 1300000)
    //       )
    //       val custom_schema = StructType(Array(StructField("company_name", StringType, true),
    //         StructField("emp_name", StringType, true),
    //         StructField("designation", StringType, true),
    //         StructField("salary", IntegerType, true)))
    //       //val  df_with_null_values = spark.createDataFrame(spark.sparkContext.)
    //   */
    //    df5.createTempView("employee")
    //    spark.sql("""select name, dept, salary, dense_rank() over(partition by dept order by salary desc) as dr from employee""").show()
    //


  }


}
