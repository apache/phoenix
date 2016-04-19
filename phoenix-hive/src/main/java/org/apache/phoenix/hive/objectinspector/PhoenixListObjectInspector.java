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

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.phoenix.schema.types.PhoenixArray;

import java.util.List;

/**
 * ObjectInspector for list objects.
 */
public class PhoenixListObjectInspector implements ListObjectInspector {

    private ObjectInspector listElementObjectInspector;
    private byte separator;
    private LazyObjectInspectorParameters lazyParams;

    public PhoenixListObjectInspector(ObjectInspector listElementObjectInspector,
                                      byte separator, LazyObjectInspectorParameters lazyParams) {
        this.listElementObjectInspector = listElementObjectInspector;
        this.separator = separator;
        this.lazyParams = lazyParams;
    }

    @Override
    public String getTypeName() {
        return org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "<" +
                listElementObjectInspector.getTypeName() + ">";
    }

    @Override
    public Category getCategory() {
        return Category.LIST;
    }

    @Override
    public ObjectInspector getListElementObjectInspector() {
        return listElementObjectInspector;
    }

    @Override
    public Object getListElement(Object data, int index) {
        if (data == null) {
            return null;
        }

        PhoenixArray array = (PhoenixArray) data;

        return array.getElement(index);
    }

    @Override
    public int getListLength(Object data) {
        if (data == null) {
            return -1;
        }

        PhoenixArray array = (PhoenixArray) data;
        return array.getDimensions();
    }

    @Override
    public List<?> getList(Object data) {
        if (data == null) {
            return null;
        }

        PhoenixArray array = (PhoenixArray) data;
        int valueLength = array.getDimensions();
        List<Object> valueList = Lists.newArrayListWithExpectedSize(valueLength);

        for (int i = 0; i < valueLength; i++) {
            valueList.add(array.getElement(i));
        }

        return valueList;
    }

    public byte getSeparator() {
        return separator;
    }

    public LazyObjectInspectorParameters getLazyParams() {
        return lazyParams;
    }
}
