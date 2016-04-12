/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive;

import com.google.common.base.Splitter;
import com.google.common.base.Splitter.MapSplitter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.phoenix.hive.util.HiveConnectionUtil;
import org.apache.phoenix.hive.util.HiveConfigurationUtil;
import org.apache.phoenix.hive.util.HiveTypeUtil;
import org.apache.phoenix.hive.util.PhoenixUtil;

/**
* PhoenixMetaHook
* This class captures all create and delete Hive queries and passes them to phoenix
*
* @version 1.0
* @since   2015-02-08 
*/

public class PhoenixMetaHook
  implements HiveMetaHook
{
  static Log LOG = LogFactory.getLog(PhoenixMetaHook.class);

  /**
   *commitCreateTable creates a Phoenix table after the hive table has been created 
   * incoming hive types.
   * @param tbl the table properties
   * 
   */
  public void commitCreateTable(Table tbl)
    throws MetaException
  {
    LOG.debug("PhoenixMetaHook commitCreateTable ");
    Map fields = new LinkedHashMap();
    Map mps = tbl.getParameters();

    String tablename = mps.get(HiveConfigurationUtil.TABLE_NAME) != null ? (String)mps.get
            (HiveConfigurationUtil.TABLE_NAME) : tbl.getTableName();

    String mapping = (String)mps.get(HiveConfigurationUtil.COLUMN_MAPPING);
    Map mappings = null;
    if ((mapping != null) && (mapping.length() > 0)) {
      mapping = mapping.toLowerCase();
      mappings = Splitter.on(",").omitEmptyStrings().trimResults().withKeyValueSeparator(":").split(mapping);
    }

    for (FieldSchema fs : tbl.getSd().getCols()) {
      try {
        String fname = fs.getName().toLowerCase();
        if (mappings != null) {
          fname = mappings.get(fname) == null ? fs.getName().toLowerCase() : (String)mappings.get(fname);
        }

        fields.put(fname, HiveTypeUtil.HiveType2PDataType(fs.getType()).toString());
      } catch (SerDeException e) {
        throw new MetaException("Unable to process field " + fs.getName());
      }
    }

    String pk = (String)mps.get(HiveConfigurationUtil.PHOENIX_ROWKEYS);
    if ((pk == null) || (pk.length() == 0)) {
      throw new MetaException("Phoenix Table no Rowkeys specified in phoenix.rowkeys");
    }

    int salt_buckets = 0;
    String salting = (String)mps.get(HiveConfigurationUtil.SALT_BUCKETS);

    if ((salting != null) && (salting.length() > 0)) {
      try {
        salt_buckets = Integer.parseInt(salting);
        if (salt_buckets > 256) {
          LOG.warn("Salt Buckets should be between 1-256 we will cap at 256");
          salt_buckets = 256;
        }
        if (salt_buckets < 0) {
          LOG.warn("Salt Buckets should be between 1-256 we will undercap at 0");
          salt_buckets = 0;
        }
      } catch (NumberFormatException nfe) {
        salt_buckets = 0;
      }
    }
    String version = (String)mps.get(HiveConfigurationUtil.VERSIONS);
    int version_num = 0 ;
    if ((version != null) && (version.length() > 0)) {
        version_num = Integer.parseInt(version);
        if (version_num <0) {
            LOG.warn("Versions should be > 0 ignoring the property");
            version_num = 0;
        }
        if (version_num > 5) {
            LOG.warn("Versions should be between 0-5 we will cap at 5");
            salt_buckets = 256;
          }
    }
    
    String compression = (String)mps.get(HiveConfigurationUtil.COMPRESSION);
    if ((compression != null) && (compression.equalsIgnoreCase("gz")))
      compression = "GZ";
    else {
      compression = null;
    }

    try
    {
      Connection conn = HiveConnectionUtil.getConnection(tbl);

      if (tbl.getTableType().equals(TableType.MANAGED_TABLE.name())) {
        if (PhoenixUtil.findTable(conn, tablename)) {
          throw new MetaException(" Phoenix table already exists cannot create use EXTERNAL");
        }

        PhoenixUtil.createTable(conn, tablename, fields, pk.split(","),
                false, salt_buckets, compression,version_num);
      }
      else if (tbl.getTableType().equals(TableType.EXTERNAL_TABLE.name())) {
        if (PhoenixUtil.findTable(conn, tablename)) {
          LOG.info("CREATE External table table already exists");
          PhoenixUtil.testTable(conn, tablename, fields);
        } else if ((tbl.getParameters().get(HiveConfigurationUtil.AUTOCREATE) != null) &&
                ((tbl.getParameters().get(HiveConfigurationUtil.AUTOCREATE)).equalsIgnoreCase("true"))) {
          PhoenixUtil.createTable(conn, tablename, fields, pk.split(","),
                  false, salt_buckets, compression,version_num);
        }
      } else {
        throw new MetaException(" Phoenix Unsupported table Type: " + tbl.getTableType());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw new MetaException(" Phoenix table creation SQLException: " + e.getMessage());
    }
  }
  
  /**
   *commitDropTable requests a phoenix drop table when deleting a Hive table
   * this only happens if this is a managed table
   * should not drop an external table unless autodrop is set.
   * @param tbl the table properties
   * 
   */

  public void commitDropTable(Table tbl, boolean bool)
    throws MetaException
  {
    Map mps = tbl.getParameters();

    String tablename = mps.get(HiveConfigurationUtil.TABLE_NAME) != null ? (String)mps.get
            (HiveConfigurationUtil.TABLE_NAME) : tbl.getTableName();
    try
    {
      if (tbl.getTableType().equals(TableType.MANAGED_TABLE.name())) {
        Connection conn = HiveConnectionUtil.getConnection(tbl);
        PhoenixUtil.dropTable(conn, tablename);
      }
      if ((tbl.getTableType().equals(TableType.EXTERNAL_TABLE.name())) &&
              (tbl.getParameters().get(HiveConfigurationUtil.AUTODROP) != null) &&
              ((tbl.getParameters().get(HiveConfigurationUtil.AUTODROP)).equalsIgnoreCase("true")))
      {
        Connection conn = HiveConnectionUtil.getConnection(tbl);
        PhoenixUtil.dropTable(conn, tablename);
      }
    } catch (SQLException e) {
      throw new MetaException("Phoenix table drop SQLException: " + e.getMessage());
    }
  }

  public void preCreateTable(Table tbl)
    throws MetaException
  {
  }

  public void preDropTable(Table tbl)
    throws MetaException
  {
  }

  public void rollbackCreateTable(Table tbl)
    throws MetaException
  {
  }

  public void rollbackDropTable(Table tbl)
    throws MetaException
  {
  }
}