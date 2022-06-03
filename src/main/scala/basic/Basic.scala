package net.jgp.books.spark.basic

import org.apache.spark.sql.SparkSession

trait Basic {
  def getSession(appName: String): SparkSession = {
    SparkSession.builder().
      appName(appName).
      // master config get from command args
      // master("local[*]").
      config("spark.executor.memory", "6g").
      getOrCreate()
  }
}