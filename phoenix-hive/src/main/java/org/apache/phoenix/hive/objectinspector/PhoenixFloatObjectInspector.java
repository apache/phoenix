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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;

/**
 * ObjectInspector for float type
 */

public class PhoenixFloatObjectInspector extends AbstractPhoenixObjectInspector<FloatWritable>
        implements FloatObjectInspector {

    public PhoenixFloatObjectInspector() {
        super(TypeInfoFactory.floatTypeInfo);
    }

    @Override
    public Object copyObject(Object o) {
        return o == null ? null : new Float((Float) o);
    }

    @Override
    public FloatWritable getPrimitiveWritableObject(Object o) {
        return new FloatWritable(get(o));
    }

    @Override
    public float get(Object o) {
        Float value = null;

        if (o != null) {
            try {
                value = ((Float) o).floatValue();
            } catch (Exception e) {
                logExceptionMessage(o, "LONG");
            }
        }

        return value;
    }

}
