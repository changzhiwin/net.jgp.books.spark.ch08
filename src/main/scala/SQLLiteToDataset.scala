package net.jgp.books.spark.ch08.lab200_sqlite_dialect

import org.apache.spark.sql.jdbc.JdbcDialects

import net.jgp.books.spark.basic.Basic
import net.jgp.books.spark.ch08.lab_200_sqlite_dialect.SQLiteJdbcDialect

object SQLiteToDataset extends Basic {
  def run(): Unit = {
    val spark = getSession("SQLite to Dataframe using a JDBC Connection")

    val dialect = new SQLiteJdbcDialect()
    JdbcDialects.registerDialect(dialect)

    val df = spark.read.
      format("jdbc").
      option("url", "jdbc:sqlite:./data/sqlite-db").
      option("driver", "org.sqlite.JDBC").
      option("dbtable", "t1").
      load()

    df.show(5)
    df.printSchema
    println("The dataframe contains " + df.count() + " record(s).");
  }
}