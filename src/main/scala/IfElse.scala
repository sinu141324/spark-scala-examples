import org.apache.spark.sql.SparkSession

object IfElse {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().master("local").appName("IfElse").getOrCreate()
    val a=25
   if(a==25){
     println("a is equal to 25")
   }else
     {
       println("a is not equla to 25")

     }



  }

}
