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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.phoenix.hive.PhoenixSerializer.DmlType;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.mapreduce.PhoenixResultWritable;
import org.apache.phoenix.hive.objectinspector.PhoenixObjectInspectorFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * SerDe implementation for Phoenix Hive Storage
 *
 */
public class PhoenixSerDe extends AbstractSerDe {

    public static final Log LOG = LogFactory.getLog(PhoenixSerDe.class);

    private PhoenixSerializer serializer;
    private ObjectInspector objectInspector;

    private LazySerDeParameters serdeParams;
    private PhoenixRow row;

    private Properties tableProperties;

    /**
     * @throws SerDeException
     */
    public PhoenixSerDe() throws SerDeException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("PhoenixSerDe created");
        }
    }

    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        tableProperties = tbl;

        if (LOG.isDebugEnabled()) {
            LOG.debug("SerDe initialize : " + tbl.getProperty("name"));
        }

        serdeParams = new LazySerDeParameters(conf, tbl, getClass().getName());
        objectInspector = createLazyPhoenixInspector(conf, tbl);

        String inOutWork = tbl.getProperty(PhoenixStorageHandlerConstants.IN_OUT_WORK);
        if (inOutWork == null) {
            return;
        }

        serializer = new PhoenixSerializer(conf, tbl);
        row = new PhoenixRow(serdeParams.getColumnNames());
    }

    @Override
    public Object deserialize(Writable result) throws SerDeException {
        if (!(result instanceof PhoenixResultWritable)) {
            throw new SerDeException(result.getClass().getName() + ": expects " +
                    "PhoenixResultWritable!");
        }

        return row.setResultRowMap(((PhoenixResultWritable) result).getResultMap());
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return PhoenixResultWritable.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        try {
            return serializer.serialize(obj, objInspector, DmlType.NONE);
        } catch (Exception e) {
            throw new SerDeException(e);
        }
    }

    @Override
    public SerDeStats getSerDeStats() {
        // no support for statistics
        return null;
    }

    public Properties getTableProperties() {
        return tableProperties;
    }

    public LazySerDeParameters getSerdeParams() {
        return serdeParams;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    private ObjectInspector createLazyPhoenixInspector(Configuration conf, Properties tbl) throws
            SerDeException {
        List<String> columnNameList = Arrays.asList(tbl.getProperty(serdeConstants.LIST_COLUMNS)
                .split(PhoenixStorageHandlerConstants.COMMA));
        List<TypeInfo> columnTypeList = TypeInfoUtils.getTypeInfosFromTypeString(tbl.getProperty
                (serdeConstants.LIST_COLUMN_TYPES));

        List<ObjectInspector> columnObjectInspectors = Lists.newArrayListWithExpectedSize
                (columnTypeList.size());

        for (TypeInfo typeInfo : columnTypeList) {
            columnObjectInspectors.add(PhoenixObjectInspectorFactory.createObjectInspector
                    (typeInfo, serdeParams));
        }

        return LazyObjectInspectorFactory.getLazySimpleStructObjectInspector(columnNameList,
                columnObjectInspectors, null, serdeParams.getSeparators()[0], serdeParams,
                ObjectInspectorOptions.JAVA);
    }
}
