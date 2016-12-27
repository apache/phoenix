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
package org.apache.phoenix.hive.objectinspector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Factory for object inspectors. Matches hive type to the corresponding Phoenix object inspector.
 */

public class PhoenixObjectInspectorFactory {

    private static final Log LOG = LogFactory.getLog(PhoenixObjectInspectorFactory.class);

    private PhoenixObjectInspectorFactory() {

    }

    public static LazySimpleStructObjectInspector createStructObjectInspector(TypeInfo type,
                                                                              LazySerDeParameters
                                                                                      serdeParams) {
        StructTypeInfo structTypeInfo = (StructTypeInfo) type;
        List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
        List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
        List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>
                (fieldTypeInfos.size());

        for (int i = 0; i < fieldTypeInfos.size(); i++) {
            fieldObjectInspectors.add(createObjectInspector(fieldTypeInfos.get(i), serdeParams));
        }

        return LazyObjectInspectorFactory.getLazySimpleStructObjectInspector(
                fieldNames, fieldObjectInspectors, null,
                serdeParams.getSeparators()[1],
                serdeParams, ObjectInspectorOptions.JAVA);
    }

    public static ObjectInspector createObjectInspector(TypeInfo type, LazySerDeParameters
            serdeParams) {
        ObjectInspector oi = null;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Type : " + type);
        }

        switch (type.getCategory()) {
            case PRIMITIVE:
                switch (((PrimitiveTypeInfo) type).getPrimitiveCategory()) {
                    case BOOLEAN:
                        oi = new PhoenixBooleanObjectInspector();
                        break;
                    case BYTE:
                        oi = new PhoenixByteObjectInspector();
                        break;
                    case SHORT:
                        oi = new PhoenixShortObjectInspector();
                        break;
                    case INT:
                        oi = new PhoenixIntObjectInspector();
                        break;
                    case LONG:
                        oi = new PhoenixLongObjectInspector();
                        break;
                    case FLOAT:
                        oi = new PhoenixFloatObjectInspector();
                        break;
                    case DOUBLE:
                        oi = new PhoenixDoubleObjectInspector();
                        break;
                    case VARCHAR:
                        // same string
                    case STRING:
                        oi = new PhoenixStringObjectInspector(serdeParams.isEscaped(),
                                serdeParams.getEscapeChar());
                        break;
                    case CHAR:
                        oi = new PhoenixCharObjectInspector((PrimitiveTypeInfo)type);
                        break;
                    case DATE:
                        oi = new PhoenixDateObjectInspector();
                        break;
                    case TIMESTAMP:
                        oi = new PhoenixTimestampObjectInspector();
                        break;
                    case DECIMAL:
                        oi = new PhoenixDecimalObjectInspector((PrimitiveTypeInfo) type);
                        break;
                    case BINARY:
                        oi = new PhoenixBinaryObjectInspector();
                        break;
                    default:
                        throw new RuntimeException("Hive internal error. not supported data type " +
                                ": " + type);
                }

                break;
            case LIST:
                if (LOG.isDebugEnabled()) {
                    LOG.debug("List type started");
                }

                ObjectInspector listElementObjectInspector = createObjectInspector((
                        (ListTypeInfo) type).getListElementTypeInfo(), serdeParams);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("List type ended");
                }

                oi = new PhoenixListObjectInspector(listElementObjectInspector, serdeParams
                        .getSeparators()[0], serdeParams);

                break;
            default:
                throw new RuntimeException("Hive internal error. not supported data type : " +
                        type);
        }

        return oi;
    }
}
