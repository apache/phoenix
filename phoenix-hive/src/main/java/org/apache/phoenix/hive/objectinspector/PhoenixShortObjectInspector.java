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

import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class PhoenixShortObjectInspector extends AbstractPhoenixObjectInspector<ShortWritable>
        implements ShortObjectInspector {

    public PhoenixShortObjectInspector() {
        super(TypeInfoFactory.shortTypeInfo);
    }

    @Override
    public Object copyObject(Object o) {
        return o == null ? null : new Short((Short) o);
    }

    @Override
    public ShortWritable getPrimitiveWritableObject(Object o) {
        return new ShortWritable(get(o));
    }

    @Override
    public short get(Object o) {
        Short value = null;

        if (o != null) {
            try {
                value = ((Short) o).shortValue();
            } catch (Exception e) {
                logExceptionMessage(o, "SHORT");
            }
        }

        return value;
    }

}
