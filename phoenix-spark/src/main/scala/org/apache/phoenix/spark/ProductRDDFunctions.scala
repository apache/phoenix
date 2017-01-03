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
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

class ProductRDDFunctions[A <: Product](data: RDD[A]) extends Serializable {

  def saveToPhoenix(tableName: String, cols: Seq[String],
                    conf: Configuration = new Configuration, zkUrl: Option[String] = None, tenantId: Option[String] = None)
                    : Unit = {

    // Create a configuration object to use for saving
    @transient val outConfig = ConfigurationUtil.getOutputConfiguration(tableName, cols, zkUrl, tenantId, Some(conf))

    // Retrieve the zookeeper URL
    val zkUrlFinal = ConfigurationUtil.getZookeeperURL(outConfig)

    // Map the row objects into PhoenixRecordWritable
    val phxRDD = data.mapPartitions{ rows =>

      // Create a within-partition config to retrieve the ColumnInfo list
      @transient val partitionConfig = ConfigurationUtil.getOutputConfiguration(tableName, cols, zkUrlFinal, tenantId)
      @transient val columns = PhoenixConfigurationUtil.getUpsertColumnMetadataList(partitionConfig).toList

      rows.map { row =>
        val rec = new PhoenixRecordWritable(columns)
        row.productIterator.foreach { e => rec.add(e) }
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
}