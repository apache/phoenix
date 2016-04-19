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
package org.apache.phoenix.hive;

import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * Hive's RecordIdentifier implementation.
 */

public class PhoenixRowKey extends RecordIdentifier {

    private Map<String, Object> rowKeyMap = Maps.newHashMap();

    public PhoenixRowKey() {

    }

    public void setRowKeyMap(Map<String, Object> rowKeyMap) {
        this.rowKeyMap = rowKeyMap;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);

        try (ObjectOutputStream oos = new ObjectOutputStream((OutputStream) dataOutput)) {
            oos.writeObject(rowKeyMap);
            oos.flush();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);

        try (ObjectInputStream ois = new ObjectInputStream((InputStream) dataInput)) {
            rowKeyMap = (Map<String, Object>) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
