package net.jgp.books.spark.ch08.lab400_es_ingestion

import net.jgp.books.spark.basic.Basic

object ElasticsearchToDataset extends Basic{

  def run(): Unit = {
    val spark = getSession("Elasticsearch to Dataframe")

    val df = spark.read.
      format("org.elasticsearch.spark.sql").
      option("es.nodes", "47.115.165.44").
      option("es.port", "9200").
      option("es.query", "?q=*").
      option("es.nodes.wan.only", "true").
      option("es.read.field.as.array.include", "Inspection_Date").
      load("nyc_restaurants")

    df.show(10)
    df.printSchema

    println("---------------- The dataframe contains " + df.count() + " record(s).")
    //println("The dataframe is split over " + df.rdd.getPartitions.length + " partition(s).")
  }
}