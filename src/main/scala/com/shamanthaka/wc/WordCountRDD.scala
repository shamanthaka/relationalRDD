package com.shamanthaka.wc

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession


object WordCountRDD {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder
      .master("local")
      .appName("WordCount")
      .getOrCreate()


    val sc = sparkSession.sparkContext

    val inputPath = "/home/shamanthaka/IdeaProjects/spark-template/input.txt" //args(0)
    val outputPath = "/home/shamanthaka/IdeaProjects/spark-template/output" //args(1)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputPathExists = fs.exists(new Path(inputPath))
    val outPathExists = fs.exists(new Path(outputPath))

    if (!inputPathExists) {
      println("Invalid input path")
      return
    }

    if (outPathExists) {
      fs.delete(new Path(outputPath), true)
    }

    val lines = sc.textFile(inputPath)
    val a= lines.flatMap(rec => rec.split(" "))
    //a.foreach(println)
    val b= a.map(rec => (rec, 1))
    //b.foreach(println)
    val c = b.sortByKey()
    //val c= b.reduceByKey((acc, value) => acc + value)

    c.foreach(println)
    println("******************************")

    /*    wc.map(rec => rec.productIterator.mkString(("\t")))
            .saveAsObjectFile(outputPath)*/

    c.map(rec => rec._1 + "\t" + rec._2)
      .saveAsTextFile(outputPath)
  }

}
