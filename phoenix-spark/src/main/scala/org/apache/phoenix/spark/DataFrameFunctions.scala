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
import org.apache.hadoop.io.NullWritable
import org.apache.phoenix.mapreduce.PhoenixOutputFormat
import org.apache.phoenix.mapreduce.util.{ColumnInfoToStringEncoderDecoder, PhoenixConfigurationUtil}
import org.apache.phoenix.schema.types._
import org.apache.phoenix.util.{ColumnInfo, SchemaUtil}
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scala.collection.JavaConversions._

class DataFrameFunctions(data: DataFrame) extends Logging with Serializable {
  //add parameter to make indicate if there are column that is not exist in the table,
  //save it as dynamic column
  def saveToPhoenix(tableName: String, conf: Configuration = new Configuration,
                    zkUrl: Option[String] = None): Unit = {


    // Retrieve the schema field names and normalize to Phoenix, need to do this outside of mapPartitions
    //val fieldArray = data.schema.fieldNames.map(x => SchemaUtil.normalizeIdentifier(x))
    //put it to format name:dataType so that later when constructing column,we can take care of that.
    val fieldArray = data.schema.fields.map(x=>SchemaUtil.normalizeIdentifier(x.name) +":"
      + catalystTypeToPhoenixTypeString(x.dataType))

    // Create a configuration object to use for saving
    @transient val outConfig = ConfigurationUtil.getOutputConfiguration(tableName, fieldArray, zkUrl, Some(conf))

    // Retrieve the zookeeper URL
    val zkUrlFinal = ConfigurationUtil.getZookeeperURL(outConfig)

    // Map the row objects into PhoenixRecordWritable
    val phxRDD = data.mapPartitions{ rows =>
 
       // Create a within-partition config to retrieve the ColumnInfo list
       @transient val partitionConfig = ConfigurationUtil.getOutputConfiguration(tableName, fieldArray, zkUrlFinal)
       @transient val columns = PhoenixConfigurationUtil.getUpsertColumnMetadataList(partitionConfig).toList
 
       rows.map { row =>
         val rec = new PhoenixRecordWritable(columns)
         row.toSeq.foreach { e => rec.add(e) }
         (null, rec)
       }
    }

    // Save it
    phxRDD.saveAsNewAPIHadoopFile(
      "",
      classOf[NullWritable],
      classOf[PhoenixRecordWritable],
      classOf[PhoenixOutputFormat[PhoenixRecordWritable]],
      outConfig
    )
  }
  // Lookup table for Phoenix types to Spark catalyst types
  def catalystTypeToPhoenixTypeString(dataType: DataType): String = dataType match {
    case t if t.isInstanceOf[StringType] => "VARCHAR"
    case t if t.isInstanceOf[LongType] => "LONG"
    case t if t.isInstanceOf[IntegerType] => "INTEGER"
    case t if t.isInstanceOf[ShortType] => "SHORT"
    case t if t.isInstanceOf[ByteType] => "BYTE"
    case t if t.isInstanceOf[FloatType] => "FLOAT"
    case t if t.isInstanceOf[DoubleType] => "DOUBLE"
    case t if t.simpleString.startsWith("decimal") => "DECIMAL"
    case t if t.isInstanceOf[TimestampType] => "TIMESTAMP"
    case t if t.isInstanceOf[DateType] => "DATE"
    case t if t.isInstanceOf[BooleanType] => "BOOLEAN"
    case t if t.isInstanceOf[BinaryType] => "BINARY"
    //signed
    case t if t.simpleString == "array<int>"  => "INTEGER ARRAY"
    case t if t.simpleString == "array<varchar>"  => "VARCHAR ARRAY"
    case t if t.simpleString == "array<varbinary>"  => "VARBINARY ARRAY"
    case t if t.simpleString == "array<long>"  => "LONG ARRAY"
    case t if t.simpleString == "array<smallint>"  => "SMALLINT ARRAY"
    case t if t.simpleString == "array<tinyint>"  => "TINYINT ARRAY"
    case t if t.simpleString == "array<float>"  => "FLOAT ARRAY"
    case t if t.simpleString == "array<double>"  => "DOUBLE ARRAY"
    case t if t.simpleString == "array<timestamp>"  => "TIMESTAMP ARRAY"
    case t if t.simpleString == "array<date>"  => "TIMESTAMP ARRAY"
    case t if t.simpleString == "array<time>"  => "TIMESTAMP ARRAY"
    case t if t.simpleString == "array<string>"  => "VARCHAR ARRAY"
    //special type
    case t if t.simpleString.startsWith("array<decimal")  => "DECIMAL ARRAY"
    case t if t.simpleString == "array<boolean>"  => "BOOLEAN ARRAY"
    case _ => throw new UnsupportedOperationException(s"Cannot convert SQL type ${dataType.simpleString} to Phoenix type")

  }

}
