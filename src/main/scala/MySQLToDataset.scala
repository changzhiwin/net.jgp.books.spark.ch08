package net.jgp.books.spark.ch08.lab100_mysql_ingestion

import net.jgp.books.spark.basic.Basic

import org.apache.spark.sql.functions.{col}
import java.util.Properties

object MySQLToDataset extends Basic {
  def run(): Unit = {
    val spark = getSession("MySQL to Dataframe using a JDBC Connection")

    val url = "jdbc:mysql://localhost:3306/spark_labs"

    // Method 1: use properties
    val props = new Properties()
    props.put("useSSL", "false")
    props.put("user", "root")
    props.put("password", "NpzQhB3r")
    props.put("driver", "com.mysql.cj.jdbc.Driver")
    //val df = spark.read.jdbc(url, "ch02", props)

    // Method 2: use format
    val df = spark.read.
      option("url", url).
      option("dbtable", "ch02").
      option("user", "root").
      option("password", "NpzQhB3r").
      option("useSSL", "false").
      option("driver", "com.mysql.cj.jdbc.Driver").
      format("jdbc").
      load()

    val orderDF = df.orderBy(col("fname"))

    orderDF.show(5)
    orderDF.printSchema
    println("The dataframe contains " + orderDF.count() + " record(s).")
  }
}