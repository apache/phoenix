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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.math.BigDecimal;

public class PhoenixDecimalObjectInspector extends
        AbstractPhoenixObjectInspector<HiveDecimalWritable>
        implements HiveDecimalObjectInspector {

    public PhoenixDecimalObjectInspector() {
        this(TypeInfoFactory.decimalTypeInfo);
    }

    public PhoenixDecimalObjectInspector(PrimitiveTypeInfo typeInfo) {
        super(typeInfo);
    }

    @Override
    public Object copyObject(Object o) {
        return o == null ? null : new BigDecimal(o.toString());
    }

    @Override
    public HiveDecimal getPrimitiveJavaObject(Object o) {
        if (o == null) {
            return null;
        }

        return HiveDecimalUtils.enforcePrecisionScale(HiveDecimal.create((BigDecimal) o),(DecimalTypeInfo)typeInfo);
    }

    @Override
    public HiveDecimalWritable getPrimitiveWritableObject(Object o) {
        HiveDecimalWritable value = null;

        if (o != null) {
            try {
                value = new HiveDecimalWritable(getPrimitiveJavaObject(o));
            } catch (Exception e) {
                logExceptionMessage(o, "DECIMAL");
            }
        }

        return value;
    }

}
