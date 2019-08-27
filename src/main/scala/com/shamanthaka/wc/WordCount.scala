package com.shamanthaka.wc


import org.apache.spark.sql.SparkSession

object WordCount extends App{

  val sparkSession = SparkSession
                    .builder
                    .master("local")
                    .appName("WordCount")
                    .getOrCreate()

  import sparkSession.implicits._
  val data = sparkSession.read.text("input.txt").as[String]

  val words = data.flatMap(rec => rec.split(" "))
  //words.collect().foreach(println)
  words.take(5).foreach(println)
  println("*************")
  val groupedWords = words.groupByKey(_.toLowerCase())
  val counts = groupedWords.count()
  counts.show()

  /*
   val text = sc.textFile("mytextfile.txt")

 val counts = text.flatMap(line => line.split(" ")

 ).map(word => (word,1)).reduceByKey(_+_) counts.collect
   */

}
