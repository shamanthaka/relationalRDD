package com.shamanthaka.table


import org.apache.spark.sql.{Row, SparkSession,Dataset}

case class Customer2(cid: String, name: String, age: Int,city:String, sex: String)

case class Purchase(year: String, cid: String, isbn: String,city:String, cost: Int)

case class Book(isbn: String,  name: String)


object DataLoader4DS {

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder().master("local").appName("Spark DataLoad").getOrCreate()

    import sparkSession.implicits._
    //import org.apache.spark.sql.catalyst.encoders.RowEncoder

    val sc = sparkSession.sparkContext


    println("*********************CUSTOMER****************************")


    val custRDD = sc.textFile("customer")

    val cparts = custRDD.map(rec => rec.split("\t"))
    val purchaseDataSet = cparts.map(p => Customer2(p(0), p(1), p(2).toInt, p(3), p(4))).toDS()

    purchaseDataSet.createOrReplaceTempView("Customer")

    val result = sparkSession.sql("select * from Customer")
    result.show();

    //second way
    /* val purchase = cparts.map(p => Customer(p(0), p(1), p(2).toInt, p(3), p(4)))

      val purchaseDataSet = sparkSession.createDataset(purchase)

      purchaseDataSet.createOrReplaceTempView("Customer")

      val result = sparkSession.sql("select * from Customer")
      result.show();*/



    println("*********************PURCHASE****************************")

    val purchases = sc.textFile("purchase")

    val pparts = purchases.map(rec => rec.split("\t"))

    val purchaseDS = pparts.map(p => Purchase(p(0), p(1), p(2), p(3), p(4).toInt)).toDS()
    //purchase.foreach(println)


    purchaseDS.createOrReplaceTempView("Purchase")

    sparkSession.sql("select * from Purchase").show()

    println("*********************BOOK****************************")

    val books = sc.textFile("book")

    val pbooks = books.map(rec => rec.split("\t"))

    val bookDS = pbooks.map(p => Book(p(0), p(1))).toDS()

    //book.foreach(println)

    bookDS.createOrReplaceTempView("Book")

    sparkSession.sql("select * from Book").show()

    println("***********QUERY**************")

    //2.4
    val result2 = sparkSession.sql("select distinct name from Customer , Purchase  where Customer.cid = Purchase.cid " +
      " and isbn in (select isbn from Customer,Purchase where Customer.cid = Purchase.cid and Customer.name = 'Harry Smith') " +
      "and Customer.name != 'Harry Smith'")
    result2.show();
    //2.3  Find the family that spent the most money in the books.
    val result3 = sparkSession.sql("select distinct name from Customer , Purchase  where Customer.cid = Purchase.cid " +
      "and isbn in (select isbn from Customer,Purchase where Customer.cid = Purchase.cid and Customer.name = 'Harry Smith')")

    result3.show();
  }

}