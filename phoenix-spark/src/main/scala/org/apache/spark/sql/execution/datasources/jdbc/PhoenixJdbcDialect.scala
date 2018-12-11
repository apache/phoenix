package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._

private object PhoenixJdbcDialect  extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:phoenix")

  /**
    * This is only called for ArrayType (see JdbcUtils.makeSetter)
    */
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("VARCHAR", java.sql.Types.VARCHAR))
    case BinaryType => Some(JdbcType("BINARY(" + dt.defaultSize + ")", java.sql.Types.BINARY))
    case ByteType => Some(JdbcType("TINYINT", java.sql.Types.TINYINT))
    case _ => None
  }


}