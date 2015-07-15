/*
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package org.apache.phoenix.spark

import java.sql.{PreparedStatement, ResultSet}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.db.DBWritable
import org.apache.phoenix.schema.types.{PDataType, PDate, PhoenixArray}
import org.apache.phoenix.util.ColumnInfo
import org.joda.time.DateTime
import scala.collection.{immutable, mutable}
import scala.collection.JavaConversions._


class PhoenixRecordWritable(columnMetaDataList: List[ColumnInfo]) extends DBWritable {
  val upsertValues = mutable.ArrayBuffer[Any]()
  val resultMap = mutable.Map[String, AnyRef]()

  def result : immutable.Map[String, AnyRef] = {
    resultMap.toMap
  }

  override def write(statement: PreparedStatement): Unit = {
    // Make sure we at least line up in size
    if(upsertValues.length != columnMetaDataList.length) {
      throw new UnsupportedOperationException(
        s"Upsert values ($upsertValues) do not match the specified columns (columnMetaDataList)"
      )
    }

    // Correlate each value (v) to a column type (c) and an index (i)
    upsertValues.zip(columnMetaDataList).zipWithIndex.foreach {
      case ((v, c), i) => {
        if (v != null) {

          // Both Java and Joda dates used to work in 4.2.3, but now they must be java.sql.Date
          // Can override any other types here as needed
          val (finalObj, finalType) = v match {
            case dt: DateTime => (new java.sql.Date(dt.getMillis), PDate.INSTANCE)
            case d: java.util.Date => (new java.sql.Date(d.getTime), PDate.INSTANCE)
            case _ => (v, c.getPDataType)
          }

          // Save as array or object
          finalObj match {
            case obj: Array[AnyRef] => {
              // Create a java.sql.Array, need to lookup the base sql type name
              val sqlArray = statement.getConnection.createArrayOf(
                PDataType.arrayBaseType(finalType).getSqlTypeName,
                obj
              )
              statement.setArray(i + 1, sqlArray)
            }
            case _ => statement.setObject(i + 1, finalObj)
          }
        } else {
          statement.setNull(i + 1, c.getSqlType)
        }
      }
    }
  }

  override def readFields(resultSet: ResultSet): Unit = {
    val metadata = resultSet.getMetaData
    for(i <- 1 to metadata.getColumnCount) {

      // Return the contents of a PhoenixArray, if necessary
      val value = resultSet.getObject(i) match {
        case x: PhoenixArray => x.getArray
        case y => y
      }

      // Put a (ColumnLabel -> value) entry in the result map
      resultMap(metadata.getColumnLabel(i)) = value
    }
  }

  def add(value: Any): Unit = {
    upsertValues.append(value)
  }

  // Empty constructor for MapReduce
  def this() = {
    this(List[ColumnInfo]())
  }

}
