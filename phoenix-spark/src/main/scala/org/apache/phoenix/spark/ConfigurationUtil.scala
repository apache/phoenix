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
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver
import org.apache.phoenix.mapreduce.util.{ColumnInfoToStringEncoderDecoder, PhoenixConfigurationUtil}
import org.apache.phoenix.util.{ColumnInfo, PhoenixRuntime}

import scala.collection.JavaConversions._

object ConfigurationUtil extends Serializable {

  def getOutputConfiguration(tableName: String, columns: Seq[String], zkUrl: Option[String], tenantId: Option[String] = None, conf: Option[Configuration] = None): Configuration = {

    // Create an HBaseConfiguration object from the passed in config, if present
    val config = conf match {
      case Some(c) => HBaseConfiguration.create(c)
      case _ => HBaseConfiguration.create()
    }

    // Set the tenantId in the config if present
    tenantId match {
      case Some(id) => setTenantId(config, id)
      case _ =>
    }

    // Set the table to save to
    PhoenixConfigurationUtil.setOutputTableName(config, tableName)
    PhoenixConfigurationUtil.setPhysicalTableName(config, tableName)

    // Infer column names from the DataFrame schema
    PhoenixConfigurationUtil.setUpsertColumnNames(config, Array(columns : _*))

    // Override the Zookeeper URL if present. Throw exception if no address given.
    zkUrl match {
      case Some(url) => setZookeeperURL(config, url)
      case _ => {
        if (ConfigurationUtil.getZookeeperURL(config).isEmpty) {
          throw new UnsupportedOperationException(
            s"One of zkUrl or '${HConstants.ZOOKEEPER_QUORUM}' config property must be provided"
          )
        }
      }
    }
    // Return the configuration object
    config
  }

  def setZookeeperURL(conf: Configuration, zkUrl: String) = {
    val info = PhoenixEmbeddedDriver.ConnectionInfo.create(zkUrl)
    conf.set(HConstants.ZOOKEEPER_QUORUM, info.getZookeeperQuorum)
    if (info.getPort != null)
      conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, info.getPort)
    if (info.getRootNode != null)
      conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, info.getRootNode)
  }

  def setTenantId(conf: Configuration, tenantId: String) = {
    conf.set(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId)
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
    List(
      Option(conf.get(HConstants.ZOOKEEPER_QUORUM)),
      Option(conf.get(HConstants.ZOOKEEPER_CLIENT_PORT)),
      Option(conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT))
    ).flatten match {
      case Nil => None
      case x: List[String] => Some(x.mkString(":"))
    }
  }
}
