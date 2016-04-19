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

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.sql.Timestamp;

/**
 * ObjectInspector for timestamp type
 */
public class PhoenixTimestampObjectInspector extends
        AbstractPhoenixObjectInspector<TimestampWritable>
        implements TimestampObjectInspector {

    public PhoenixTimestampObjectInspector() {
        super(TypeInfoFactory.timestampTypeInfo);
    }

    @Override
    public Timestamp getPrimitiveJavaObject(Object o) {
        return (Timestamp) o;
    }

    @Override
    public Object copyObject(Object o) {
        return o == null ? null : new Timestamp(((Timestamp) o).getTime());
    }

    @Override
    public TimestampWritable getPrimitiveWritableObject(Object o) {
        TimestampWritable value = null;

        if (o != null) {
            try {
                value = new TimestampWritable((Timestamp) o);
            } catch (Exception e) {
                logExceptionMessage(o, "TIMESTAMP");
            }
        }

        return value;
    }
}
