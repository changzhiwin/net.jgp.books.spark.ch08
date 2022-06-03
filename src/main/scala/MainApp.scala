package net.jgp.books.spark

import net.jgp.books.spark.ch08.lab400_es_ingestion.ElasticsearchToDataset

object MainApp {
  def main(args: Array[String]) = {

    val (whichCase, otherArg) = args.length match {
      case 1 => (args(0).toUpperCase, "")
      case 2 => (args(0).toUpperCase, args(1).toUpperCase)
      case _ => ("TEXT", "")
    }

    println(s"=========== whichCase = $whichCase, otherArg = $otherArg ===========")

    whichCase match {
      case _      => ElasticsearchToDataset.run()
    }
  }
}