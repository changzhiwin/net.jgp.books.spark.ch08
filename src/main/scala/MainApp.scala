package net.jgp.books.spark

import net.jgp.books.spark.ch08.lab400_es_ingestion.ElasticsearchToDataset
import net.jgp.books.spark.ch08.lab100_mysql_ingestion.MySQLToDataset
import net.jgp.books.spark.ch08.lab200_sqlite_dialect.SQLiteToDataset

object MainApp {
  def main(args: Array[String]) = {

    val (whichCase, otherArg) = args.length match {
      case 1 => (args(0).toUpperCase, "")
      case 2 => (args(0).toUpperCase, args(1).toUpperCase)
      case _ => ("SQLite", "")
    }

    println(s"=========== whichCase = $whichCase, otherArg = $otherArg ===========")

    whichCase match {
      case "ES"      => ElasticsearchToDataset.run()
      case "MYSQL"   => MySQLToDataset.run()
      case _         => SQLiteToDataset.run()
    }
  }
}