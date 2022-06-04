package net.jgp.books.spark.ch08.lab100_mysql_ingestion

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col}
import java.util.Properties

import net.jgp.books.spark.basic.Basic

object MySQLToDataset extends Basic {
  def run(mode: String): Unit = {
    val spark = getSession("MySQL to Dataframe using a JDBC Connection")

    val url = "jdbc:mysql://localhost:3306/sakila"

    val props = new Properties()
    props.put("useSSL", "false")
    props.put("user", "root")
    props.put("password", "NpzQhB3r")
    props.put("driver", "com.mysql.cj.jdbc.Driver")
    
    val df = mode.toLowerCase match {
      case "order"   =>  withOrderByToDataset(spark, url, props)
      case "where"   =>  withWhereClauseToDataset(spark, url, props)
      case "join"    =>  withJoinToDataset(spark, url, props)
      case _         =>  withPartitionToDataset(spark, url, props)
    }

    df.show(5)
    df.printSchema
    println(s"The dataframe contains ${df.count()} record(s).")
    println(s"The dataframe is split over ${df.rdd.getNumPartitions} partition(s).")
  }

  private def withOrderByToDataset(spark: SparkSession, url: String, props: Properties): DataFrame = {

    //Notice: optioin style, no props is ok.
    val df = spark.read.
      option("url", url).
      option("dbtable", "actor").
      option("user", "root").
      option("password", "NpzQhB3r").
      option("useSSL", "false").
      option("driver", "com.mysql.cj.jdbc.Driver").
      format("jdbc").
      load()

    df.orderBy(col("last_name"))
  }

  private def withWhereClauseToDataset(spark: SparkSession, url: String, props: Properties): DataFrame = {

    val sqlQuery = """
      select * from film where 
      (title like "%ALIEN%" or title like "%victory%" 
      or title like "%agent%" or description like "%action%") 
      and rental_rate>1 
      and (rating="G" or rating="PG")
    """

    spark.read.jdbc(url, s"(${sqlQuery}) film_alias", props)
  }

  private def withJoinToDataset(spark: SparkSession, url: String, props: Properties): DataFrame = {
    val sqlQuery = """
      select actor.first_name, actor.last_name, film.title, 
      film.description 
      from actor, film_actor, film 
      where actor.actor_id = film_actor.actor_id 
      and film_actor.film_id = film.film_id
    """

    spark.read.jdbc(url, s"(${sqlQuery}) actor_film_alias", props)
  }

  private def withPartitionToDataset(spark: SparkSession, url: String, props: Properties): DataFrame = {
    props.put("partitionColumn", "film_id")
    props.put("lowerBound", "1")
    props.put("upperBound", "1000")
    props.put("numPartitions", "10")

    spark.read.jdbc(url, "film", props)
  }
}