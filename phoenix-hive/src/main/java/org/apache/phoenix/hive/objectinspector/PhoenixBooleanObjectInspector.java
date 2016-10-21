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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;

public class PhoenixBooleanObjectInspector extends AbstractPhoenixObjectInspector<BooleanWritable>
        implements BooleanObjectInspector {

    public PhoenixBooleanObjectInspector() {
        super(TypeInfoFactory.booleanTypeInfo);
    }

    @Override
    public Object copyObject(Object o) {
        return o == null ? null : new Boolean((Boolean) o);
    }

    @Override
    public BooleanWritable getPrimitiveWritableObject(Object o) {
        return new BooleanWritable(get(o));
    }

    @Override
    public boolean get(Object o) {
        Boolean value = null;

        if (o != null) {
            try {
                value = (Boolean) o;
            } catch (Exception e) {
                logExceptionMessage(o, "BOOLEAN");
            }
        }

        return value;
    }
}
