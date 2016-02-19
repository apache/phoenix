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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.phoenix.hive.util.HiveConstants;
import org.apache.phoenix.hive.util.HiveTypeUtil;
import org.apache.phoenix.schema.types.PDataType;

/**
* PhoenixSerDe
* Hive SerializerDeserializer Class for Phoenix connection
*/

public class PhoenixSerde implements SerDe {
    static Log LOG = LogFactory.getLog(PhoenixSerde.class);
    private PhoenixHiveDBWritable phrecord;
    private List<String> columnNames;
    private List<TypeInfo> columnTypes;
    private ObjectInspector ObjectInspector;
    private int fieldCount;
    private List<Object> row;
    private List<ObjectInspector> fieldOIs;
    
    
    /**
     * This method initializes the Hive SerDe
     * incoming hive types.
     * @param conf conf job configuration
     * @param tblProps table properties
     */
    public void initialize(Configuration conf, Properties tblProps) throws SerDeException {
        if (conf != null) {
            conf.setClass("phoenix.input.class", PhoenixHiveDBWritable.class, DBWritable.class);
        }
        this.columnNames = Arrays.asList(tblProps.getProperty(HiveConstants.COLUMNS).split(","));
        this.columnTypes =
                TypeInfoUtils.getTypeInfosFromTypeString(tblProps
                        .getProperty(HiveConstants.COLUMNS_TYPES));
        LOG.debug("columnNames: " + this.columnNames);
        LOG.debug("columnTypes: " + this.columnTypes);
        this.fieldCount = this.columnTypes.size();
        PDataType[] types = HiveTypeUtil.hiveTypesToSqlTypes(this.columnTypes);
        this.phrecord = new PhoenixHiveDBWritable(types);
        this.fieldOIs = new ArrayList(this.columnNames.size());

        for (TypeInfo typeInfo : this.columnTypes) {
            this.fieldOIs.add(TypeInfoUtils
                    .getStandardWritableObjectInspectorFromTypeInfo(typeInfo));
        }
        this.ObjectInspector =
                ObjectInspectorFactory.getStandardStructObjectInspector(this.columnNames,
                    this.fieldOIs);
        this.row = new ArrayList(this.columnNames.size());
    }
    
    
    /**
     * This Deserializes a result from Phoenix to a Hive result
     * @param wr the phoenix writable Object here PhoenixHiveDBWritable
     * @return  Object for Hive
     */

    public Object deserialize(Writable wr) throws SerDeException {
        if (!(wr instanceof PhoenixHiveDBWritable)) throw new SerDeException(
                "Serialized Object is not of type PhoenixHiveDBWritable");
        try {
            this.row.clear();
            PhoenixHiveDBWritable phdbw = (PhoenixHiveDBWritable) wr;
            for (int i = 0; i < this.fieldCount; i++) {
                Object value = phdbw.get(this.columnNames.get(i));
                if (value != null) this.row.add(HiveTypeUtil.SQLType2Writable(
                    ( this.columnTypes.get(i)).getTypeName(), value));
                else {
                    this.row.add(null);
                }
            }
            return this.row;
        } catch (Exception e) {
            e.printStackTrace();
            throw new SerDeException(e.getCause());
        }
    }

    public ObjectInspector getObjectInspector() throws SerDeException {
        return this.ObjectInspector;
    }

    public SerDeStats getSerDeStats() {
        return null;
    }

    /**
     * This is a getter for the  serialized class to use with this SerDE
     * @return  The class PhoenixHiveDBWritable
     */
    
    public Class<? extends Writable> getSerializedClass() {
        return PhoenixHiveDBWritable.class;
    }

    
    /**
     * This serializes a Hive row to a Phoenix entry
     * incoming hive types.
     * @param row Hive row
     * @param inspector inspector for the Hive row
     */
    
    public Writable serialize(Object row, ObjectInspector inspector) throws SerDeException {
        final StructObjectInspector structInspector = (StructObjectInspector) inspector;
        final List<? extends StructField> fields = structInspector.getAllStructFieldRefs();

        if (fields.size() != fieldCount) {
            throw new SerDeException(String.format("Required %d columns, received %d.", fieldCount,
                fields.size()));
        }
        phrecord.clear();
        for (int i = 0; i < fieldCount; i++) {
            StructField structField = fields.get(i);
            if (structField != null) {
                Object field = structInspector.getStructFieldData(row, structField);
                ObjectInspector fieldOI = structField.getFieldObjectInspector();
                switch (fieldOI.getCategory()) {
                case PRIMITIVE:
                    Writable value =
                            (Writable) ((PrimitiveObjectInspector) fieldOI)
                                    .getPrimitiveWritableObject(field);
                    phrecord.add(value);
                    break;
                default:
                    // TODO add support for Array
                    new SerDeException("Phoenix Unsupported column type: " + fieldOI.getCategory());
                }
            }
        }

        return phrecord;
    }

}