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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;

public class PhoenixLongObjectInspector extends AbstractPhoenixObjectInspector<LongWritable>
        implements LongObjectInspector {

    public PhoenixLongObjectInspector() {
        super(TypeInfoFactory.longTypeInfo);
    }

    @Override
    public Object copyObject(Object o) {
        return o == null ? null : new Long((Long) o);
    }

    @Override
    public LongWritable getPrimitiveWritableObject(Object o) {
        return new LongWritable(get(o));
    }

    @Override
    public long get(Object o) {
        Long value = null;

        if (o != null) {
            try {
                value = ((Long) o).longValue();
            } catch (Exception e) {
                logExceptionMessage(o, "LONG");
            }
        }

        return value;
    }

}
