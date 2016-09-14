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
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive
        .AbstractPrimitiveLazyObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Writable;

/**
 * AbstractPhoenixObjectInspector for a LazyPrimitive object
 */
public abstract class AbstractPhoenixObjectInspector<T extends Writable>
        extends AbstractPrimitiveLazyObjectInspector<T> {

    private final Log log;

    public AbstractPhoenixObjectInspector() {
        super();

        log = LogFactory.getLog(getClass());
    }

    protected AbstractPhoenixObjectInspector(PrimitiveTypeInfo typeInfo) {
        super(typeInfo);

        log = LogFactory.getLog(getClass());
    }

    @Override
    public Object getPrimitiveJavaObject(Object o) {
        return o == null ? null : o;
    }

    public void logExceptionMessage(Object value, String dataType) {
        if (log.isDebugEnabled()) {
            log.debug("Data not in the " + dataType + " data type range so converted to null. " +
                    "Given data is :"
                    + value.toString(), new Exception("For debugging purposes"));
        }
    }
}