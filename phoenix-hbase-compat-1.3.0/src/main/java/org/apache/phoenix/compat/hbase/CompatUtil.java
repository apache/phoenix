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
package org.apache.phoenix.compat.hbase;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.client.Scan;

public class CompatUtil {

    public static Class[] getMrMetricsClasses() {
        return new Class[] {};
    }

    public static Scan setStartRow(Scan scan, byte[] indexRowKey, boolean inclusive) {
        if (inclusive) {
            return scan.setStartRow(indexRowKey);
        } else {
            byte[] nextIndexRowKey = new byte[indexRowKey.length + 1];
            System.arraycopy(indexRowKey, 0, nextIndexRowKey, 0, indexRowKey.length);
            nextIndexRowKey[indexRowKey.length] = 0;
            return scan.setStartRow(nextIndexRowKey);
        }
    }

    public static Scan setStopRow(Scan scan, byte[] indexRowKey, boolean inclusive) {
        if (inclusive) {
            return scan.setStopRow(indexRowKey);
        } else {
            throw new NotImplementedException();
        }
    }
}
