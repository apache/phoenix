/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.metadata.InputEstimator;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.mapreduce.PhoenixInputFormat;
import org.apache.phoenix.hive.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.hive.ppd.PhoenixPredicateDecomposer;
import org.apache.phoenix.hive.ppd.PhoenixPredicateDecomposerManager;
import org.apache.phoenix.hive.util.PhoenixStorageHandlerUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This class manages all the Phoenix/Hive table initial configurations and SerDe Election
 */
@SuppressWarnings("deprecation")
public class PhoenixStorageHandler extends DefaultStorageHandler implements
        HiveStoragePredicateHandler, InputEstimator {

    private static final Log LOG = LogFactory.getLog(PhoenixStorageHandler.class);

    public PhoenixStorageHandler() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("PhoenixStorageHandler created");
        }
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return new PhoenixMetaHook();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return PhoenixOutputFormat.class;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String>
            jobProperties) {
        configureJobProperties(tableDesc, jobProperties);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Configuring input job for table : " + tableDesc.getTableName());
        }

        // initialization efficiency. Inform to SerDe about in/out work.
        tableDesc.getProperties().setProperty(PhoenixStorageHandlerConstants.IN_OUT_WORK,
                PhoenixStorageHandlerConstants.IN_WORK);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String>
            jobProperties) {
        configureJobProperties(tableDesc, jobProperties);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Configuring output job for  table : " + tableDesc.getTableName());
        }

        // initialization efficiency. Inform to SerDe about in/out work.
        tableDesc.getProperties().setProperty(PhoenixStorageHandlerConstants.IN_OUT_WORK,
                PhoenixStorageHandlerConstants.OUT_WORK);
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String>
            jobProperties) {
        configureJobProperties(tableDesc, jobProperties);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void configureJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        Properties tableProperties = tableDesc.getProperties();

        String inputFormatClassName =
                tableProperties.getProperty(PhoenixStorageHandlerConstants
                        .HBASE_INPUT_FORMAT_CLASS);

        if (LOG.isDebugEnabled()) {
            LOG.debug(PhoenixStorageHandlerConstants.HBASE_INPUT_FORMAT_CLASS + " is " +
                    inputFormatClassName);
        }

        Class<?> inputFormatClass;
        try {
            if (inputFormatClassName != null) {
                inputFormatClass = JavaUtils.loadClass(inputFormatClassName);
            } else {
                inputFormatClass = PhoenixInputFormat.class;
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        if (inputFormatClass != null) {
            tableDesc.setInputFileFormatClass((Class<? extends InputFormat>) inputFormatClass);
        }

        String tableName = tableProperties.getProperty(PhoenixStorageHandlerConstants
                .PHOENIX_TABLE_NAME);
        if (tableName == null) {
            tableName = tableDesc.getTableName();
            tableProperties.setProperty(PhoenixStorageHandlerConstants.PHOENIX_TABLE_NAME,
                    tableName);
        }

        jobProperties.put(PhoenixConfigurationUtil.INPUT_TABLE_NAME, tableName);
        jobProperties.put(PhoenixStorageHandlerConstants.ZOOKEEPER_QUORUM, tableProperties
                .getProperty(PhoenixStorageHandlerConstants.ZOOKEEPER_QUORUM,
                        PhoenixStorageHandlerConstants.DEFAULT_ZOOKEEPER_QUORUM));
        jobProperties.put(PhoenixStorageHandlerConstants.ZOOKEEPER_PORT, tableProperties
                .getProperty(PhoenixStorageHandlerConstants.ZOOKEEPER_PORT, String.valueOf
                        (PhoenixStorageHandlerConstants.DEFAULT_ZOOKEEPER_PORT)));
        jobProperties.put(PhoenixStorageHandlerConstants.ZOOKEEPER_PARENT, tableProperties
                .getProperty(PhoenixStorageHandlerConstants.ZOOKEEPER_PARENT,
                        PhoenixStorageHandlerConstants.DEFAULT_ZOOKEEPER_PARENT));

        jobProperties.put(hive_metastoreConstants.META_TABLE_STORAGE, this.getClass().getName());

        // set configuration when direct work with HBase.
        jobProperties.put(HConstants.ZOOKEEPER_QUORUM, jobProperties.get
                (PhoenixStorageHandlerConstants.ZOOKEEPER_QUORUM));
        jobProperties.put(HConstants.ZOOKEEPER_CLIENT_PORT, jobProperties.get
                (PhoenixStorageHandlerConstants.ZOOKEEPER_PORT));
        jobProperties.put(HConstants.ZOOKEEPER_ZNODE_PARENT, jobProperties.get
                (PhoenixStorageHandlerConstants.ZOOKEEPER_PARENT));
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return PhoenixSerDe.class;
    }

    @Override
    public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer,
                                                  ExprNodeDesc predicate) {
        PhoenixSerDe phoenixSerDe = (PhoenixSerDe) deserializer;
        String tableName = phoenixSerDe.getTableProperties().getProperty
                (PhoenixStorageHandlerConstants.PHOENIX_TABLE_NAME);
        String predicateKey = PhoenixStorageHandlerUtil.getTableKeyOfSession(jobConf, tableName);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Decomposing predicate with predicateKey : " + predicateKey);
        }

        List<String> columnNameList = phoenixSerDe.getSerdeParams().getColumnNames();
        PhoenixPredicateDecomposer predicateDecomposer = PhoenixPredicateDecomposerManager
                .createPredicateDecomposer(predicateKey, columnNameList);

        return predicateDecomposer.decomposePredicate(predicate);
    }

    @Override
    public Estimation estimate(JobConf job, TableScanOperator ts, long remaining) throws
            HiveException {
        String hiveTableName = ts.getConf().getTableMetadata().getTableName();
        int reducerCount = job.getInt(hiveTableName + PhoenixStorageHandlerConstants
                .PHOENIX_REDUCER_NUMBER, 1);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Estimating input size for table: " + hiveTableName + " with reducer count " +
                    reducerCount + ". Remaining : " + remaining);
        }

        long bytesPerReducer = job.getLong(HiveConf.ConfVars.BYTESPERREDUCER.varname,
                Long.parseLong(HiveConf.ConfVars.BYTESPERREDUCER.getDefaultValue()));
        long totalLength = reducerCount * bytesPerReducer;

        return new Estimation(0, totalLength);
    }
}
