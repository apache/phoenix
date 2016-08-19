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
package org.apache.phoenix.schema;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.ReadOnlyProps;

public class PTableRefFactory {
    public PTableRef makePTableRef(PTable table, long lastAccessTime, long resolvedTime) {
        return new PTableRefImpl(table, lastAccessTime, resolvedTime, table.getEstimatedSize());
    }

    public PTableRef makePTableRef(PTableRef tableRef) {
        return new PTableRefImpl(tableRef);
    }

    private static final PTableRefFactory INSTANCE = new PTableRefFactory();

    public static enum Encoding {
        OBJECT, PROTOBUF
    };

    public static PTableRefFactory getFactory(ReadOnlyProps props) {
        String encodingEnumString =
                props.get(QueryServices.CLIENT_CACHE_ENCODING,
                    QueryServicesOptions.DEFAULT_CLIENT_CACHE_ENCODING);
        Encoding encoding = Encoding.valueOf(encodingEnumString.toUpperCase());
        switch (encoding) {
        case PROTOBUF:
            return SerializedPTableRefFactory.getFactory();
        case OBJECT:
        default:
            return INSTANCE;
        }
    }
}