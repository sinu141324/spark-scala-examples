package com.sparkbyexamples.spark.spark30

object thirdCharToUpper extends App{


  val str = "hai my name is harikrishna"

  def toUpper(s:String)=
    if(s.length<3) s
    else
      s.substring(0,2)+s.substring(2,3).toUpperCase()+s.substring(3,s.length)

  val toUpperCase = str.split(" ").map(m=>toUpper(m)).mkString(" ")
    val countOfWords = str.split(" ")
      .map(f=>(f,1))
      .groupBy(_._1)
//      .mapValues(value=>value.map(_._2).count)
      .mapValues(values=>values.map(_._2).sum)

    toUpperCase.foreach(print)
    countOfWords.foreach(println)

}
