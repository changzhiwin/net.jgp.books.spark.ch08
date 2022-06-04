# Purpose
pure scala version of https://github.com/jgperrin/net.jgp.books.spark.ch08

# Environment
- Java 11
- Scala 2.13.8
- Spark 3.2.1
- Mysql 5.7.16
- Elasticsearch 6.8.23 [Install](https://www.elastic.co/cn/downloads/elasticsearch)

# How to run
## 1, sbt package, in project root dir
When success, there a jar file at ./target/scala-2.13. The name is `main-scala-ch8_2.13-1.0.jar` (the same as name property in sbt file)

## 2, submit jar file, in project root dir
```
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class net.jgp.books.spark.MainApp \
  --master "local[*]" \
  --jars jars/elasticsearch-spark-30_2.13-8.2.2.jar,jars/mysql-connector-java-8.0.29.jar,jars/sqlite-jdbc-3.32.3.3.jar \
  target/scala-2.13/main-scala-ch8_2.13-1.0.jar
```

## 3, print

### Case: mysql orderby fname
**Notice!!** I use result of chapter 2(2.3.4) as input.
```
+--------+------------+--------------------+
|   lname|       fname|                name|
+--------+------------+--------------------+
|  Pascal|      Blaise|      Pascal, Blaise|
|Voltaire|    François|  Voltaire, François|
|   Karau|      Holden|       Karau, Holden|
|  Perrin|Jean-Georges|Perrin, Jean-Georges|
| Zaharia|       Matei|      Zaharia, Matei|
+--------+------------+--------------------+
only showing top 5 rows

root
 |-- lname: string (nullable = true)
 |-- fname: string (nullable = true)
 |-- name: string (nullable = true)
only showing top 5 rows

root
 |-- lname: string (nullable = true)
 |-- fname: string (nullable = true)
 |-- name: string (nullable = true)

 The dataframe contains 6 record(s).
```

## 4, Some diffcult case

### Download jar
Search some key words. For example [elasticsearch-hadoop](https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-hadoop/6.8.23).
> https://mvnrepository.com/

