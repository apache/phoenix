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
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.phoenix.mapreduce.util.{ColumnInfoToStringEncoderDecoder, PhoenixConfigurationUtil}
import org.apache.phoenix.util.ColumnInfo
import scala.collection.JavaConversions._

object ConfigurationUtil extends Serializable {

  def getOutputConfiguration(tableName: String, columns: Seq[String], zkUrl: Option[String], conf: Option[Configuration] = None): Configuration = {

    // Create an HBaseConfiguration object from the passed in config, if present
    val config = conf match {
      case Some(c) => HBaseConfiguration.create(c)
      case _ => HBaseConfiguration.create()
    }

    // Set the table to save to
    PhoenixConfigurationUtil.setOutputTableName(config, tableName)
    PhoenixConfigurationUtil.setPhysicalTableName(config, tableName)

    // Infer column names from the DataFrame schema
    PhoenixConfigurationUtil.setUpsertColumnNames(config, Array(columns : _*))

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

    // Return the configuration object
    config
  }

  // Return a serializable representation of the columns
  def encodeColumns(conf: Configuration) = {
    ColumnInfoToStringEncoderDecoder.encode(conf, PhoenixConfigurationUtil.getUpsertColumnMetadataList(conf)
    )
  }

  // Decode the columns to a list of ColumnInfo objects
  def decodeColumns(conf: Configuration): List[ColumnInfo] = {
    ColumnInfoToStringEncoderDecoder.decode(conf).toList
  }
  
  def getZookeeperURL(conf: Configuration): Option[String] = {
    Option(conf.get(HConstants.ZOOKEEPER_QUORUM))
  }
}
