package net.jgp.books.spark.ch08.lab_200_sqlite_dialect

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{ DataType, DataTypes, StringType, MetadataBuilder }

class  SQLiteJdbcDialect extends JdbcDialect{
  
  def canHandle(url: String): Boolean = {
    url.startsWith("jdbc:sqlite")
  }

  
  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {

    // just for test
    println(s"===================> sqlType = ${sqlType}, typeName = ${typeName} size = ${size}")

    // you can implement your logic here
    typeName.toUpperCase match {
      case "TEXT" => Some(DataTypes.StringType)
      case _      => None
    }
  }
  
}