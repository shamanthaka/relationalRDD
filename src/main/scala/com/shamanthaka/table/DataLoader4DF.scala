package com.shamanthaka.table

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DataLoader4DF {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder
      .master("local")
      .appName("Spark Customer")
      .getOrCreate()



    import sparkSession.implicits._

    val sc = sparkSession.sparkContext


    println("*********************CUSTOMER****************************")
    val customers = sc.textFile("/home/shamanthaka/IdeaProjects/spark-template/customer")
    val cparts = customers.map(rec => rec.split("\t"))

    val customer = cparts.map(p => Row(p(0), p(1), p(2).toInt, p(3), p(4)))
    customer.foreach(println)
    println("*************************")

    val schemaCustomer = StructType(Array(StructField("cid", StringType), StructField("name", StringType),
      StructField("age", IntegerType), StructField("city", StringType), StructField("sex", StringType)))

    val rddToDF = sparkSession.createDataFrame(customer, schemaCustomer)
    rddToDF.foreach(r => println(r))

    rddToDF.createOrReplaceTempView("Customer")

    sparkSession.sql("select * from Customer").show()

    println("*********************PURCHASE****************************")

    val purchases = sc.textFile("/home/shamanthaka/IdeaProjects/spark-template/purchase")
    val pparts = purchases.map(rec => rec.split("\t"))
    val purchase = pparts.map(p => Row(p(0), p(1), p(2), p(3), p(4).toInt))
    purchase.foreach(println)
    println("*************************")

    val schemaPurchase = StructType(Array(StructField("year", StringType), StructField("cid", StringType),
      StructField("isbn", StringType), StructField("city", StringType), StructField("cost", IntegerType)))

    val rddToDFPurchase = sparkSession.createDataFrame(purchase, schemaPurchase)
    rddToDFPurchase.foreach(r => println(r))

    rddToDFPurchase.createOrReplaceTempView("Purchase")

    sparkSession.sql("select * from Purchase").show()

    println("*********************BOOK****************************")

    val books = sc.textFile("/home/shamanthaka/IdeaProjects/spark-template/book")
    val pbooks = books.map(rec => rec.split("\t"))
    val book = pbooks.map(p => Row(p(0), p(1)))
    book.foreach(println)
    println("*************************")

    val schemaBook = StructType(Array(StructField("isbn", StringType), StructField("name", StringType)))

    val rddToDFBook = sparkSession.createDataFrame(book, schemaBook)
    rddToDFBook.foreach(r => println(r))

    rddToDFBook.createOrReplaceTempView("Book")

    sparkSession.sql("select * from Book").show()

    println("***********QUERY**************")

    val result = sparkSession.sql("select distinct name from Customer, Purchase where " +
      "Customer.cid = Purchase.cid and isbn in (select isbn from Customer,Purchase where " +
      "Customer.cid = Purchase.cid and Customer.name = 'Harry Smith') and Customer.name != 'Harry Smith'")
    result.show()

    val result3 = sparkSession.sql("select distinct name from Customer , Purchase  where Customer.cid = Purchase.cid " +
      "and isbn in (select isbn from Customer,Purchase where Customer.cid = Purchase.cid and Customer.name = 'Harry Smith')")


    result3.show();

    sparkSession.sql(
      """select
        | split(c.name, '\\s')[1] as last_name, sum(p.cost) as money_spent
        |from
        | Purchase p
        |left join
        | Customer c
        |on
        |  p.cid = c.cid
        |group by split(c.name, '\\s')[1]
        |order by money_spent desc limit 1
      """.stripMargin).show(false)



  }


}

