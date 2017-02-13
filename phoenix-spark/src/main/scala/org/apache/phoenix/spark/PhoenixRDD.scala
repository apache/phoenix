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

import java.sql.DriverManager

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.io.NullWritable
import org.apache.phoenix.jdbc.PhoenixDriver
import org.apache.phoenix.mapreduce.PhoenixInputFormat
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil
import org.apache.phoenix.schema.types._
import org.apache.phoenix.util.ColumnInfo
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.JavaConverters._

class PhoenixRDD(sc: SparkContext, table: String, columns: Seq[String],
                 predicate: Option[String] = None,
                 zkUrl: Option[String] = None,
                 @transient conf: Configuration, dateAsTimestamp: Boolean = false,
                 tenantId: Option[String] = None
                )
  extends RDD[PhoenixRecordWritable](sc, Nil) {

  // Make sure to register the Phoenix driver
  DriverManager.registerDriver(new PhoenixDriver)

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

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    phoenixRDD.preferredLocations(split)
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

  def getPhoenixConfiguration: Configuration = {

    // This is just simply not serializable, so don't try, but clone it because
    // PhoenixConfigurationUtil mutates it.
    val config = HBaseConfiguration.create(conf)

    PhoenixConfigurationUtil.setInputClass(config, classOf[PhoenixRecordWritable])
    PhoenixConfigurationUtil.setInputTableName(config, table)

    if(!columns.isEmpty) {
      PhoenixConfigurationUtil.setSelectColumnNames(config, columns.toArray)
    }

    if(predicate.isDefined) {
      PhoenixConfigurationUtil.setInputTableConditions(config, predicate.get)
    }

    // Override the Zookeeper URL if present. Throw exception if no address given.
    zkUrl match {
      case Some(url) => ConfigurationUtil.setZookeeperURL(config, url)
      case _ => {
        if(ConfigurationUtil.getZookeeperURL(config).isEmpty) {
          throw new UnsupportedOperationException(
            s"One of zkUrl or '${HConstants.ZOOKEEPER_QUORUM}' config property must be provided"
          )
        }
      }
    }

    tenantId match {
      case Some(tid) => ConfigurationUtil.setTenantId(config, tid)
      case _ =>
    }

    config
  }

  // Convert our PhoenixRDD to a DataFrame
  def toDataFrame(sqlContext: SQLContext): DataFrame = {
    val columnInfoList = PhoenixConfigurationUtil
      .getSelectColumnMetadataList(new Configuration(phoenixConf))
      .asScala

    // Keep track of the sql type and column names.
    val columns: Seq[(String, Int)] = columnInfoList.map(ci => {
      (ci.getDisplayName, ci.getSqlType)
    })


    // Lookup the Spark catalyst types from the Phoenix schema
    val structFields = phoenixSchemaToCatalystSchema(columnInfoList).toArray

    // Create the data frame from the converted Spark schema
    sqlContext.createDataFrame(map(pr => {

      // Create a sequence of column data
      val rowSeq = columns.map { case (name, sqlType) =>
        val res = pr.resultMap(name)
          // Special handling for data types
          if (dateAsTimestamp && (sqlType == 91 || sqlType == 19)) { // 91 is the defined type for Date and 19 for UNSIGNED_DATE
            new java.sql.Timestamp(res.asInstanceOf[java.sql.Date].getTime)
          } else if (sqlType == 92 || sqlType == 18) { // 92 is the defined type for Time and 18 for UNSIGNED_TIME
            new java.sql.Timestamp(res.asInstanceOf[java.sql.Time].getTime)
          } else {
            res
          }
      }

      // Create a Spark Row from the sequence
      Row.fromSeq(rowSeq)
    }), new StructType(structFields))
  }

  def phoenixSchemaToCatalystSchema(columnList: Seq[ColumnInfo]) = {
    columnList.map(ci => {
      val structType = phoenixTypeToCatalystType(ci)
      StructField(ci.getDisplayName, structType)
    })
  }


  // Lookup table for Phoenix types to Spark catalyst types
  def phoenixTypeToCatalystType(columnInfo: ColumnInfo): DataType = columnInfo.getPDataType match {
    case t if t.isInstanceOf[PVarchar] || t.isInstanceOf[PChar] => StringType
    case t if t.isInstanceOf[PLong] || t.isInstanceOf[PUnsignedLong] => LongType
    case t if t.isInstanceOf[PInteger] || t.isInstanceOf[PUnsignedInt] => IntegerType
    case t if t.isInstanceOf[PSmallint] || t.isInstanceOf[PUnsignedSmallint] => ShortType
    case t if t.isInstanceOf[PTinyint] || t.isInstanceOf[PUnsignedTinyint] => ByteType
    case t if t.isInstanceOf[PFloat] || t.isInstanceOf[PUnsignedFloat] => FloatType
    case t if t.isInstanceOf[PDouble] || t.isInstanceOf[PUnsignedDouble] => DoubleType
    // Use Spark system default precision for now (explicit to work with < 1.5)
    case t if t.isInstanceOf[PDecimal] => 
      if (columnInfo.getPrecision == null || columnInfo.getPrecision < 0) DecimalType(38, 18) else DecimalType(columnInfo.getPrecision, columnInfo.getScale)
    case t if t.isInstanceOf[PTimestamp] || t.isInstanceOf[PUnsignedTimestamp] => TimestampType
    case t if t.isInstanceOf[PTime] || t.isInstanceOf[PUnsignedTime] => TimestampType
    case t if (t.isInstanceOf[PDate] || t.isInstanceOf[PUnsignedDate]) && !dateAsTimestamp => DateType
    case t if (t.isInstanceOf[PDate] || t.isInstanceOf[PUnsignedDate]) && dateAsTimestamp => TimestampType
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
    case t if t.isInstanceOf[PDecimalArray] => ArrayType(
      if (columnInfo.getPrecision == null || columnInfo.getPrecision < 0) DecimalType(38, 18) else DecimalType(columnInfo.getPrecision, columnInfo.getScale), containsNull = true)
    case t if t.isInstanceOf[PTimestampArray] || t.isInstanceOf[PUnsignedTimestampArray] => ArrayType(TimestampType, containsNull = true)
    case t if t.isInstanceOf[PDateArray] || t.isInstanceOf[PUnsignedDateArray] => ArrayType(TimestampType, containsNull = true)
    case t if t.isInstanceOf[PTimeArray] || t.isInstanceOf[PUnsignedTimeArray] => ArrayType(TimestampType, containsNull = true)
  }
}
