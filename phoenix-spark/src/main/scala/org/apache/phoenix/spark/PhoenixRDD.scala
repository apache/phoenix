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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.io.NullWritable
import org.apache.phoenix.mapreduce.PhoenixInputFormat
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil
import org.apache.phoenix.schema.types._
import org.apache.phoenix.util.ColumnInfo
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

class PhoenixRDD(sc: SparkContext, table: String, columns: Seq[String],
                 predicate: Option[String] = None, zkUrl: Option[String] = None,
                 @transient conf: Configuration)
  extends RDD[PhoenixRecordWritable](sc, Nil) with Logging {

  @transient lazy val phoenixConf = {
    getPhoenixConfiguration
  }

  val phoenixRDD = sc.newAPIHadoopRDD(phoenixConf,
    classOf[PhoenixInputFormat[PhoenixRecordWritable]],
    classOf[NullWritable],
    classOf[PhoenixRecordWritable])

  override protected def getPartitions: Array[Partition] = {
    phoenixRDD.partitions
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext) = {
    phoenixRDD.compute(split, context).map(r => r._2)
  }

  def printPhoenixConfig(conf: Configuration): Unit = {
    for (mapEntry <- conf.iterator().asScala) {
      val k = mapEntry.getKey
      val v = mapEntry.getValue

      if (k.startsWith("phoenix")) {
        println(s"$k = $v")
      }
    }
  }

  def buildSql(table: String, columns: Seq[String], predicate: Option[String]): String = {
    val query = "SELECT %s FROM \"%s\"".format(
      columns.map(f => "\"" + f + "\"").mkString(", "),
      table
    )

    query + (predicate match {
      case Some(p: String) => " WHERE " + p
      case _ => ""
    })
  }

  def getPhoenixConfiguration: Configuration = {

    // This is just simply not serializable, so don't try, but clone it because
    // PhoenixConfigurationUtil mutates it.
    val config = new Configuration(conf)

    PhoenixConfigurationUtil.setInputQuery(config, buildSql(table, columns, predicate))
    PhoenixConfigurationUtil.setSelectColumnNames(config, columns.mkString(","))
    PhoenixConfigurationUtil.setInputTableName(config, "\"" + table + "\"")
    PhoenixConfigurationUtil.setInputClass(config, classOf[PhoenixRecordWritable])

    // Override the Zookeeper URL if present. Throw exception if no address given.
    zkUrl match {
      case Some(url) => config.set(HConstants.ZOOKEEPER_QUORUM, url )
      case _ => {
        if(config.get(HConstants.ZOOKEEPER_QUORUM) == null) {
          throw new UnsupportedOperationException(
            s"One of zkUrl or '${HConstants.ZOOKEEPER_QUORUM}' config property must be provided"
          )
        }
      }
    }

    config
  }

  // Convert our PhoenixRDD to a DataFrame
  def toDataFrame(sqlContext: SQLContext): DataFrame = {
    val columnList = PhoenixConfigurationUtil
      .getSelectColumnMetadataList(new Configuration(phoenixConf))
      .asScala

    val columnNames: Seq[String] = columnList.map(ci => {
      ci.getDisplayName
    })

    // Lookup the Spark catalyst types from the Phoenix schema
    val structFields = phoenixSchemaToCatalystSchema(columnList).toArray

    // Create the data frame from the converted Spark schema
    sqlContext.createDataFrame(map(pr => {
      val values = pr.resultMap
      val row = new GenericMutableRow(values.size)

      columnNames.zipWithIndex.foreach {
        case (columnName, i) => {
          row.update(i, values(columnName))
        }
      }

      row.asInstanceOf[Row]
    }), new StructType(structFields))
  }

  def phoenixSchemaToCatalystSchema(columnList: Seq[ColumnInfo]) = {
    columnList.map(ci => {
      val structType = phoenixTypeToCatalystType(ci.getPDataType)
      StructField(ci.getDisplayName, structType)
    })
  }

  // Lookup table for Phoenix types to Spark catalyst types
  def phoenixTypeToCatalystType(phoenixType: PDataType[_]): DataType = phoenixType match {
    case t if t.isInstanceOf[PVarchar] || t.isInstanceOf[PChar] => StringType
    case t if t.isInstanceOf[PLong] || t.isInstanceOf[PUnsignedLong] => LongType
    case t if t.isInstanceOf[PInteger] || t.isInstanceOf[PUnsignedInt] => IntegerType
    case t if t.isInstanceOf[PFloat] || t.isInstanceOf[PUnsignedFloat] => FloatType
    case t if t.isInstanceOf[PDouble] || t.isInstanceOf[PUnsignedDouble] => DoubleType
    case t if t.isInstanceOf[PDecimal] => DecimalType(None)
    case t if t.isInstanceOf[PTimestamp] || t.isInstanceOf[PUnsignedTimestamp] => TimestampType
    case t if t.isInstanceOf[PTime] || t.isInstanceOf[PUnsignedTime] => TimestampType
    case t if t.isInstanceOf[PDate] || t.isInstanceOf[PUnsignedDate] => TimestampType
    case t if t.isInstanceOf[PBoolean] => BooleanType
    case t if t.isInstanceOf[PVarbinary] || t.isInstanceOf[PBinary] => BinaryType
    case t if t.isInstanceOf[PIntegerArray] || t.isInstanceOf[PUnsignedIntArray] => ArrayType(IntegerType, containsNull = true)
    case t if t.isInstanceOf[PBooleanArray] => ArrayType(BooleanType, containsNull = true)
    case t if t.isInstanceOf[PVarcharArray] || t.isInstanceOf[PCharArray] => ArrayType(StringType, containsNull = true)
    case t if t.isInstanceOf[PVarbinaryArray] || t.isInstanceOf[PBinaryArray] => ArrayType(BinaryType, containsNull = true)
    case t if t.isInstanceOf[PLongArray] || t.isInstanceOf[PUnsignedLongArray] => ArrayType(LongType, containsNull = true)
    case t if t.isInstanceOf[PSmallintArray] || t.isInstanceOf[PUnsignedSmallintArray] => ArrayType(IntegerType, containsNull = true)
    case t if t.isInstanceOf[PTinyintArray] || t.isInstanceOf[PUnsignedTinyintArray] => ArrayType(ByteType, containsNull = true)
    case t if t.isInstanceOf[PFloatArray] || t.isInstanceOf[PUnsignedFloatArray] => ArrayType(FloatType, containsNull = true)
    case t if t.isInstanceOf[PDoubleArray] || t.isInstanceOf[PUnsignedDoubleArray] => ArrayType(DoubleType, containsNull = true)
    case t if t.isInstanceOf[PDecimalArray] => ArrayType(DecimalType(None), containsNull = true)
    case t if t.isInstanceOf[PTimestampArray] || t.isInstanceOf[PUnsignedTimestampArray] => ArrayType(TimestampType, containsNull = true)
    case t if t.isInstanceOf[PDateArray] || t.isInstanceOf[PUnsignedDateArray] => ArrayType(TimestampType, containsNull = true)
    case t if t.isInstanceOf[PTimeArray] || t.isInstanceOf[PUnsignedTimeArray] => ArrayType(TimestampType, containsNull = true)
  }
}