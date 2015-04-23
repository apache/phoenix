/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.phoenix.hive.util.HiveConfigurationUtil;

public class PhoenixStorageHandler extends DefaultStorageHandler implements
        HiveStoragePredicateHandler {
    static Log LOG = LogFactory.getLog(PhoenixStorageHandler.class.getName());

    private Configuration conf = null;

    public PhoenixStorageHandler() {
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return new PhoenixMetaHook();
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        configureJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void
            configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        configureJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        configureJobProperties(tableDesc, jobProperties);
    }

    /**
     * Extract all job properties to configure this job 
     * parameter tableDesc tabledescription, jobProperties
     * TODO this avoids any pushdown must revisit
     */
    private void configureJobProperties(TableDesc tableDesc, Map<String, String> jobProperties)
    {
      Properties tblProps = tableDesc.getProperties();
      tblProps.getProperty("phoenix.hbase.table.name");
      HiveConfigurationUtil.setProperties(tblProps, jobProperties);

      //TODO this avoids any pushdown must revisit and extract meaningful parts
      jobProperties.put("phoenix.select.stmt", "select * from " + (String)jobProperties.get("phoenix.hbase.table.name"));
      if(((String)jobProperties.get("phoenix.hbase.table.name")).contains("limit")==true){
          
      }
      LOG.debug("ConfigurationUtil.SELECT_STATEMENT " + (String)jobProperties.get("phoenix.select.stmt"));
    }
    
    /**
     * Getter for the class serializing data from Phoenix to Hive 
     */
    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return HivePhoenixInputFormat.class;
    }

    /**
     * Getter for the class serializing data from Hive to Phoenix
     */
    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return HivePhoenixOutputFormat.class;
        //return PhoenixOutputFormat.class;
    }

    /**
     * Getter for the Phoenix Serde
     */
    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return PhoenixSerde.class;
    }

    /**
     * Class t access Hive query in a case of a select statement
     * parameters: jobConf jobconfiguration, ExprNodeDesc contains details on the expression
     */
    public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer arg1,
            ExprNodeDesc exprn) {
        
        //TODO revisit the whole logic some information is not consistent
        return null;
    }

}