# Purpose
pure scala version of https://github.com/jgperrin/net.jgp.books.spark.ch08

# Environment
- Java 11
- Scala 2.13.8
- Spark 3.2.1
- elasticsearch 6.8.23 [Install](https://www.elastic.co/cn/downloads/elasticsearch)

# How to run
## 1, sbt package, in project root dir
When success, there a jar file at ./target/scala-2.13. The name is `main-scala-ch8_2.13-1.0.jar` (the same as name property in sbt file)

## 2, submit jar file, in project root dir
```
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class net.jgp.books.spark.MainApp \
  --master "local[*]" \
  --jars jars/elasticsearch-hadoop-6.8.23.jar \
  target/scala-2.13/main-scala-ch8_2.13-1.0.jar
```

## 3, print

## 4, Some diffcult case

### Download jar
Search some key words. For example [elasticsearch-hadoop](https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-hadoop/6.8.23).
> https://mvnrepository.com/

