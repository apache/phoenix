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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;

/**
 * ObjectInspector for Binary type
 */

public class PhoenixBinaryObjectInspector extends AbstractPhoenixObjectInspector<BytesWritable>
        implements BinaryObjectInspector {

    public PhoenixBinaryObjectInspector() {
        super(TypeInfoFactory.binaryTypeInfo);
    }

    @Override
    public Object copyObject(Object o) {
        byte[] clone = null;

        if (o != null) {
            byte[] source = (byte[]) o;
            clone = new byte[source.length];
            System.arraycopy(source, 0, clone, 0, source.length);
        }

        return clone;
    }

    @Override
    public byte[] getPrimitiveJavaObject(Object o) {
        return (byte[]) o;
    }

    @Override
    public BytesWritable getPrimitiveWritableObject(Object o) {
        return new BytesWritable((byte[]) o);
    }

}
