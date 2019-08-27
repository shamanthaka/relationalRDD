package com.shamanthaka.table


import org.apache.spark.sql.{Row, SparkSession}

object DataLoader4RDD {

  def main(args: Array[String]): Unit = {

    //String firstName;

    val sparkSession = SparkSession.builder().master("local").appName("Spark DataLoad").getOrCreate()



    import sparkSession.implicits._

    val sc = sparkSession.sparkContext

    println("*********************purchase****************************")
    /*
    Question 2.1 How much did each seller earn? Develop both the PIG and Spark RDD
    (Spark RDD means using only the transformations and actions, not Spark SQL) versions to solve the query.
     */

    val purchaseRDD = sc.textFile("/home/shamanthaka/IdeaProjects/spark-template/purchase")

    val pparts = purchaseRDD.map(rec => rec.split("\t"))

    val purchase = pparts.map(rec => (rec(3), rec(4).toInt))

    purchase.foreach(r => println(r))

    println("************* suming by seller *****************")

    val result = purchase.reduceByKey((x,y) => x + y)

    result.foreach(println)

    /*
    Question 2.2 Find the names of the books that Amazon gives the lowest price among all sellers.
    Develop both the PIG and Spark RDD versions to solve this query.
     */
    println("*********************2.2 purchase****************************")
    val purchaseRDD2 = sc.textFile("/home/shamanthaka/IdeaProjects/spark-template/purchase")

    val pparts2 = purchaseRDD2.map(rec => rec.split("\t"))

    val purchase2 = pparts2.map(rec => (rec(2), rec(3) + " " +rec(4)))

    purchase2.foreach(r => println(r))

    //load the book data
    println("*********************2.2 book****************************")

    val bookRDD = sc.textFile("/home/shamanthaka/IdeaProjects/spark-template/book")

    val bparts2 = bookRDD.map(rec => rec.split("\t"))

    val book = bparts2.map(rec => (rec(0), rec(1)))

    book.foreach(r => println(r))

    //combined with both

    val joiningTable = book.join(purchase2)
    joiningTable.foreach(r => println(r))
    //combinedPars = combined.map(lambda x: x[1])
    //print
    println("**************************Joinings*******************")
    val joiningPart = joiningTable.map(r => r._2)


    joiningPart.foreach(r => println(r))

    def findMinPrice(x: String,y: String) : String = {

      val splitX = x.split(" ")
      val px = splitX(splitX.length - 1)

      val splitY = y.split(" ")
      val py = splitY(splitY.length - 1)

      if( px.toInt <= py.toInt){
        return x
      }else{
        return y
      }

    }

    val minValues = joiningPart.reduceByKey(findMinPrice)

    println("*******minValues******************")

    minValues.foreach(println)

    val filteredResult = minValues.filter(x => x._2.split(" ")(0) == "Amazon")

    println("********filtered one *****************")

    filteredResult.foreach(println)

  }

}