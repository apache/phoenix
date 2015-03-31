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
import org.apache.phoenix.mapreduce.PhoenixOutputFormat
import org.apache.phoenix.mapreduce.util.{ColumnInfoToStringEncoderDecoder, PhoenixConfigurationUtil}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

class ProductRDDFunctions[A <: Product](data: RDD[A]) extends Logging with Serializable {

  def saveToPhoenix(tableName: String, cols: Seq[String],
                    conf: Configuration = new Configuration, zkUrl: Option[String] = None)
                    : Unit = {

    // Setup Phoenix output configuration, make a local copy
    val config = new Configuration(conf)
    PhoenixConfigurationUtil.setOutputTableName(config, tableName)
    PhoenixConfigurationUtil.setUpsertColumnNames(config, cols.mkString(","))

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

    // Encode the column info to a serializable type
    val encodedColumns = ColumnInfoToStringEncoderDecoder.encode(
      PhoenixConfigurationUtil.getUpsertColumnMetadataList(config)
    )

    // Map each element of the product to a new (NullWritable, PhoenixRecordWritable)
    val phxRDD: RDD[(NullWritable, PhoenixRecordWritable)] = data.map { e =>
      val rec = new PhoenixRecordWritable(encodedColumns)
      e.productIterator.foreach { rec.add(_) }
      (null, rec)
    }

    // Save it
    phxRDD.saveAsNewAPIHadoopFile(
      "",
      classOf[NullWritable],
      classOf[PhoenixRecordWritable],
      classOf[PhoenixOutputFormat[PhoenixRecordWritable]],
      config
    )
  }
}