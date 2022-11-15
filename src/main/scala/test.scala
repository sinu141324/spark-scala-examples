object test extends App{


  val str = "abbbcccc"
  //expected output :a1b3c4

  var output = ""
  for (i <- 0 to str.length - 1) {

    var sum = 0
    for (j <- i + 1 to str.length - 1) {
      if (str(i) == str(j))
        sum = sum + 1
      println(sum)
      //      output = str(i) + output + sum
      //    }

    }

    //  println(output)


  }
}
