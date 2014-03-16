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
package org.apache.phoenix.mapreduce;

import org.apache.hadoop.hbase.KeyValue;

import java.util.List;

/**
 * A listener hook to process KeyValues that are being written to HFiles for bulk import.
 * Implementing this interface and configuring it via the {@link
 * CsvToKeyValueMapper#UPSERT_HOOK_CLASS_CONFKEY} configuration key.
 * <p/>
 * The intention of such a hook is to allow coproccessor-style operations to be peformed on
 * data that is being bulk-loaded via MapReduce.
 */
public interface ImportPreUpsertKeyValueProcessor {

    /**
     * Process a list of KeyValues before they are written to an HFile. The supplied list of
     * KeyValues contain all data that is to be written for a single Phoenix row.
     * <p/>
     * Implementors can filter certain KeyValues from the list, augment the list, or return the
     * same list.
     *
     * @param rowKey the row key for the key values that are being passed in
     * @param keyValues list of KeyValues that are to be written to an HFile
     * @return the list that will actually be written
     */
    List<KeyValue> preUpsert(byte[] rowKey, List<KeyValue> keyValues);

}
