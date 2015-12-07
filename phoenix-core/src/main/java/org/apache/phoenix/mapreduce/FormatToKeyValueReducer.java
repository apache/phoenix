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

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.phoenix.mapreduce.bulkload.TableRowkeyPair;

/**
 * Reducer class for the bulkload jobs.
 * Performs similar functionality to {@link KeyValueSortReducer}
 */
public class FormatToKeyValueReducer
    extends Reducer<TableRowkeyPair,KeyValue,TableRowkeyPair,KeyValue> {

    @Override
    protected void reduce(TableRowkeyPair key, Iterable<KeyValue> values,
        Reducer<TableRowkeyPair, KeyValue, TableRowkeyPair, KeyValue>.Context context)
        throws IOException, InterruptedException {
        TreeSet<KeyValue> map = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
        for (KeyValue kv: values) {
            try {
                map.add(kv.clone());
            } catch (CloneNotSupportedException e) {
                throw new java.io.IOException(e);
            }
        }
        context.setStatus("Read " + map.getClass());
        int index = 0;
        for (KeyValue kv: map) {
            context.write(key, kv);
            if (++index % 100 == 0) context.setStatus("Wrote " + index);
        }
    }
}
