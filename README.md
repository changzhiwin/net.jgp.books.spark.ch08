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

## 2, import schema and data to mysql
```
mysql -uroot -p < /YOUR_PROJECT_PATH/data/sakila-schema.sql
mysql -uroot -p < /YOUR_PROJECT_PATH/data/sakila-data.sql
```

## 3, submit jar file, in project root dir
```
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class net.jgp.books.spark.MainApp \
  --master "local[*]" \
  --jars jars/elasticsearch-spark-30_2.13-8.2.2.jar,jars/mysql-connector-java-8.0.29.jar,jars/sqlite-jdbc-3.32.3.3.jar \
  target/scala-2.13/main-scala-ch8_2.13-1.0.jar SQLITE
```

## 4, Print out

### Case: mysql order
```
+--------+----------+---------+-------------------+
|actor_id|first_name|last_name|        last_update|
+--------+----------+---------+-------------------+
|      92|   KIRSTEN|   AKROYD|2006-02-15 04:34:33|
|      58| CHRISTIAN|   AKROYD|2006-02-15 04:34:33|
|     182|    DEBBIE|   AKROYD|2006-02-15 04:34:33|
|     118|      CUBA|    ALLEN|2006-02-15 04:34:33|
|     145|       KIM|    ALLEN|2006-02-15 04:34:33|
+--------+----------+---------+-------------------+
only showing top 5 rows

root
 |-- actor_id: integer (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- last_update: timestamp (nullable = true)

The dataframe contains 200 record(s).
The dataframe is split over 1 partition(s).
```

### Case: mysql where
```
+-------+--------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
|film_id|         title|         description|release_year|language_id|original_language_id|rental_duration|rental_rate|length|replacement_cost|rating|    special_features|        last_update|
+-------+--------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
|      6|  AGENT TRUMAN|A Intrepid Panora...|  2006-01-01|          1|                null|              3|       2.99|   169|           17.99|    PG|      Deleted Scenes|2006-02-15 05:03:42|
|     13|   ALI FOREVER|A Action-Packed D...|  2006-01-01|          1|                null|              4|       4.99|   150|           21.99|    PG|Deleted Scenes,Be...|2006-02-15 05:03:42|
|    137|CHARADE DUFFEL|A Action-Packed D...|  2006-01-01|          1|                null|              3|       2.99|    66|           21.99|    PG|Trailers,Deleted ...|2006-02-15 05:03:42|
|    217|    DAZED PUNK|A Action-Packed S...|  2006-01-01|          1|                null|              6|       4.99|   120|           20.99|     G|Commentaries,Dele...|2006-02-15 05:03:42|
|    396|  HANGING DEEP|A Action-Packed Y...|  2006-01-01|          1|                null|              5|       4.99|    62|           18.99|     G|Trailers,Commenta...|2006-02-15 05:03:42|
+-------+--------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
only showing top 5 rows

root
 |-- film_id: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- description: string (nullable = true)
 |-- release_year: date (nullable = true)
 |-- language_id: integer (nullable = true)
 |-- original_language_id: integer (nullable = true)
 |-- rental_duration: integer (nullable = true)
 |-- rental_rate: decimal(4,2) (nullable = true)
 |-- length: integer (nullable = true)
 |-- replacement_cost: decimal(5,2) (nullable = true)
 |-- rating: string (nullable = true)
 |-- special_features: string (nullable = true)
 |-- last_update: timestamp (nullable = true)

The dataframe contains 16 record(s).
The dataframe is split over 1 partition(s).
```

### Case: mysql join
```
+----------+---------+--------------------+--------------------+
|first_name|last_name|               title|         description|
+----------+---------+--------------------+--------------------+
|  PENELOPE|  GUINESS|    ACADEMY DINOSAUR|A Epic Drama of a...|
|  PENELOPE|  GUINESS|ANACONDA CONFESSIONS|A Lacklusture Dis...|
|  PENELOPE|  GUINESS|         ANGELS LIFE|A Thoughtful Disp...|
|  PENELOPE|  GUINESS|BULWORTH COMMANDM...|A Amazing Display...|
|  PENELOPE|  GUINESS|       CHEAPER CLYDE|A Emotional Chara...|
+----------+---------+--------------------+--------------------+
only showing top 5 rows

root
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- title: string (nullable = true)
 |-- description: string (nullable = true)

The dataframe contains 5462 record(s).
The dataframe is split over 1 partition(s).
```

### Case: mysql partition
```
+-------+----------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
|film_id|           title|         description|release_year|language_id|original_language_id|rental_duration|rental_rate|length|replacement_cost|rating|    special_features|        last_update|
+-------+----------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
|      1|ACADEMY DINOSAUR|A Epic Drama of a...|  2006-01-01|          1|                null|              6|       0.99|    86|           20.99|    PG|Deleted Scenes,Be...|2006-02-15 05:03:42|
|      2|  ACE GOLDFINGER|A Astounding Epis...|  2006-01-01|          1|                null|              3|       4.99|    48|           12.99|     G|Trailers,Deleted ...|2006-02-15 05:03:42|
|      3|ADAPTATION HOLES|A Astounding Refl...|  2006-01-01|          1|                null|              7|       2.99|    50|           18.99| NC-17|Trailers,Deleted ...|2006-02-15 05:03:42|
|      4|AFFAIR PREJUDICE|A Fanciful Docume...|  2006-01-01|          1|                null|              5|       2.99|   117|           26.99|     G|Commentaries,Behi...|2006-02-15 05:03:42|
|      5|     AFRICAN EGG|A Fast-Paced Docu...|  2006-01-01|          1|                null|              6|       2.99|   130|           22.99|     G|      Deleted Scenes|2006-02-15 05:03:42|
+-------+----------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
only showing top 5 rows

root
 |-- film_id: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- description: string (nullable = true)
 |-- release_year: date (nullable = true)
 |-- language_id: integer (nullable = true)
 |-- original_language_id: integer (nullable = true)
 |-- rental_duration: integer (nullable = true)
 |-- rental_rate: decimal(4,2) (nullable = true)
 |-- length: integer (nullable = true)
 |-- replacement_cost: decimal(5,2) (nullable = true)
 |-- rating: string (nullable = true)
 |-- special_features: string (nullable = true)
 |-- last_update: timestamp (nullable = true)

The dataframe contains 1000 record(s).
The dataframe is split over 10 partition(s).
```



### Case: sqlite dialect
Ref: https://www.sqlite.org/datatype3.html
```
+----------+------+----+------+--------------------+
|         t|    nu|   i|     r|                  no|
+----------+------+----+------+--------------------+
|       001| 100.0| 200| 300.0|          [34 30 30]|
|        02|  10.0|  20|  30.0|             [04 00]|
|comparison|1000.0|2000|3000.0|[41 20 76 61 6C 7...|
+----------+------+----+------+--------------------+

root
 |-- t: string (nullable = true)
 |-- nu: double (nullable = true)
 |-- i: integer (nullable = true)
 |-- r: float (nullable = true)
 |-- no: binary (nullable = true)

The dataframe contains 3 record(s).
```

### Case: ES
It cost a very long time on my laptop(Mac pro 8g).
```
+--------------------+--------------------+---------+----------+--------+--------------------+-------------+-------------------+--------------------+-----+-------------------+--------------------+--------------------+----------+-------------------+-----+--------------------+--------------+---------------------+-------+
|              Action|             Address|     Boro|  Building|   Camis|               Coord|Critical_Flag|Cuisine_Description|                 Dba|Grade|         Grade_Date|     Inspection_Date|     Inspection_Type|     Phone|        Record_Date|Score|              Street|Violation_Code|Violation_Description|Zipcode|
+--------------------+--------------------+---------+----------+--------+--------------------+-------------+-------------------+--------------------+-----+-------------------+--------------------+--------------------+----------+-------------------+-----+--------------------+--------------+---------------------+-------+
|Violations were c...|13507 LEFFERTS BO...|   QUEENS|     13507|41030732|[-73.8206948, 40....| Not Critical|            Chinese|SHUN FOO CHINESE ...| null|2014-08-07 00:00:00|[2014-08-07 00:00...|Cycle Inspection ...|7188437074|2016-03-21 00:00:00| null|  LEFFERTS BOULEVARD|           08A| Facility not verm...|  11420|
|Violations were c...|13507 LEFFERTS BO...|   QUEENS|     13507|41030732|[-73.8206948, 40....| Not Critical|            Chinese|SHUN FOO CHINESE ...| null|2014-08-07 00:00:00|[2014-08-07 00:00...|Cycle Inspection ...|7188437074|2016-03-21 00:00:00| null|  LEFFERTS BOULEVARD|           09C| Food contact surf...|  11420|
|Violations were c...|13507 LEFFERTS BO...|   QUEENS|     13507|41030732|[-73.8206948, 40....| Not Critical|            Chinese|SHUN FOO CHINESE ...| null|               null|[2014-01-08 00:00...|Administrative Mi...|7188437074|2016-03-21 00:00:00| null|  LEFFERTS BOULEVARD|           22A| Nuisance created ...|  11420|
|Violations were c...|1488 1 AVENUE MAN...|MANHATTAN|1488      |41030858|[-73.9531563, 40....| Not Critical|              Pizza|        LA MIA PIZZA| null|               null|[2015-09-10 00:00...|Cycle Inspection ...|2124721200|2016-03-21 00:00:00| null|1 AVENUE         ...|           08A| Facility not verm...|  10075|
|Violations were c...|1488 1 AVENUE MAN...|MANHATTAN|1488      |41030858|[-73.9531563, 40....| Not Critical|              Pizza|        LA MIA PIZZA| null|2014-07-23 00:00:00|[2014-07-23 00:00...|Cycle Inspection ...|2124721200|2016-03-21 00:00:00| null|1 AVENUE         ...|           08A| Facility not verm...|  10075|
|Violations were c...|1488 1 AVENUE MAN...|MANHATTAN|1488      |41030858|[-73.9531563, 40....|     Critical|              Pizza|        LA MIA PIZZA| null|               null|[2014-07-02 00:00...|Cycle Inspection ...|2124721200|2016-03-21 00:00:00| 26.0|1 AVENUE         ...|           02G| Cold food item he...|  10075|
|Violations were c...|1488 1 AVENUE MAN...|MANHATTAN|1488      |41030858|[-73.9531563, 40....|     Critical|              Pizza|        LA MIA PIZZA|    A|2014-01-08 00:00:00|[2014-01-08 00:00...|Cycle Inspection ...|2124721200|2016-03-21 00:00:00| 12.0|1 AVENUE         ...|           02B| Hot food item not...|  10075|
|Violations were c...|1488 1 AVENUE MAN...|MANHATTAN|1488      |41030858|[-73.9531563, 40....| Not Critical|              Pizza|        LA MIA PIZZA| null|2013-05-24 00:00:00|[2013-05-24 00:00...|Cycle Inspection ...|2124721200|2016-03-21 00:00:00| null|1 AVENUE         ...|           08A| Facility not verm...|  10075|
|Violations were c...|1488 1 AVENUE MAN...|MANHATTAN|1488      |41030858|[-73.9531563, 40....| Not Critical|              Pizza|        LA MIA PIZZA| null|2013-05-24 00:00:00|[2013-05-24 00:00...|Cycle Inspection ...|2124721200|2016-03-21 00:00:00| null|1 AVENUE         ...|           10F| Non-food contact ...|  10075|
|Violations were c...|3625 KINGSBRIDGE ...|    BRONX|3625      |41030870|[-73.9019993, 40....| Not Critical|              Greek|CHRISTOS GYRO & S...| null|2015-04-08 00:00:00|[2015-04-08 00:00...|Cycle Inspection ...|7184056464|2016-03-21 00:00:00| null|KINGSBRIDGE AVENU...|           10F| Non-food contact ...|  10463|
+--------------------+--------------------+---------+----------+--------+--------------------+-------------+-------------------+--------------------+-----+-------------------+--------------------+--------------------+----------+-------------------+-----+--------------------+--------------+---------------------+-------+
only showing top 10 rows

root
 |-- Action: string (nullable = true)
 |-- Address: string (nullable = true)
 |-- Boro: string (nullable = true)
 |-- Building: string (nullable = true)
 |-- Camis: long (nullable = true)
 |-- Coord: array (nullable = true)
 |    |-- element: double (containsNull = true)
 |-- Critical_Flag: string (nullable = true)
 |-- Cuisine_Description: string (nullable = true)
 |-- Dba: string (nullable = true)
 |-- Grade: string (nullable = true)
 |-- Grade_Date: timestamp (nullable = true)
 |-- Inspection_Date: array (nullable = true)
 |    |-- element: timestamp (containsNull = true)
 |-- Inspection_Type: string (nullable = true)
 |-- Phone: string (nullable = true)
 |-- Record_Date: timestamp (nullable = true)
 |-- Score: double (nullable = true)
 |-- Street: string (nullable = true)
 |-- Violation_Code: string (nullable = true)
 |-- Violation_Description: string (nullable = true)
 |-- Zipcode: long (nullable = true)

The dataframe contains 473039 record(s).
The dataframe is split over 5 partition(s).
```

# Some diffcult case

## Should use elasticsearch jar for scala 2.13, matching scala version
[elasticsearch-spark-30_2.13-8.2.2.jar](https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-30)
> https://mvnrepository.com/  

## Using remote node, should set this option
```
option("es.nodes.wan.only", "true")
```