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
import org.apache.phoenix.util.SchemaUtil
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._


class DataFrameFunctions(data: DataFrame) extends Serializable {

  def saveToPhoenix(tableName: String, conf: Configuration = new Configuration,
                    zkUrl: Option[String] = None, tenantId: Option[String] = None): Unit = {


    // Retrieve the schema field names and normalize to Phoenix, need to do this outside of mapPartitions
    val fieldArray = data.schema.fieldNames.map(x => SchemaUtil.normalizeIdentifier(x))

    // Create a configuration object to use for saving
    @transient val outConfig = ConfigurationUtil.getOutputConfiguration(tableName, fieldArray, zkUrl, tenantId, Some(conf))

    // Retrieve the zookeeper URL
    val zkUrlFinal = ConfigurationUtil.getZookeeperURL(outConfig)

    // Map the row objects into PhoenixRecordWritable
    val phxRDD = data.rdd.mapPartitions{ rows =>
 
       // Create a within-partition config to retrieve the ColumnInfo list
       @transient val partitionConfig = ConfigurationUtil.getOutputConfiguration(tableName, fieldArray, zkUrlFinal, tenantId)
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
}
