/*
 * Copyright 2014 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

public class ArrayUpdateObserver extends BaseRegionObserver {

    public static final String ARRAY_INSERT_UNIQUE_ATTRIBUTE = "run_array_insert_unique_coprocessor";
    public static final String FAMILY_ATTRIBUTE_SUFFIX = "_FAMILY";
    public static final String QUALIFIER_ATTRIBUTE_SUFFIX = "_QUALIFIER";
    public static final String BASE_DATATYPE_ATTRIBUTE_SUFFIX = "_BASE_DATATYPE";
    public static final String SORTORDER_ATTRIBUTE_SUFFIX = "_SORTORDER";
    public static final int FUNCTION_IDENTIFIER_LENGTH = 4;

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> env, final Put put,
            final WALEdit edit, final Durability durability) throws IOException {

        //skip coprocessor unless put has mark to perform
        if (put.getAttribute(ARRAY_INSERT_UNIQUE_ATTRIBUTE) == null) {
            return;
        }

        byte[] callerIds = put.getAttribute(ARRAY_INSERT_UNIQUE_ATTRIBUTE);

        for (int i = 0; i < callerIds.length / FUNCTION_IDENTIFIER_LENGTH; i++) {
            new ArrayInsertUniqueCoprocessor(env, put, Bytes.toString(Bytes.copy(
                    callerIds, i * FUNCTION_IDENTIFIER_LENGTH, FUNCTION_IDENTIFIER_LENGTH)));
        }

    }

}
