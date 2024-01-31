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
package org.apache.phoenix.coprocessorclient;

import static org.apache.phoenix.query.QueryConstants.VIEW_MODIFIED_PROPERTY_TAG_TYPE;

import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;

public class MetaDataEndpointImplConstants {
    // Column to track tables that have been upgraded based on PHOENIX-2067
    public static final String ROW_KEY_ORDER_OPTIMIZABLE = "ROW_KEY_ORDER_OPTIMIZABLE";
    public static final byte[] ROW_KEY_ORDER_OPTIMIZABLE_BYTES = Bytes.toBytes(ROW_KEY_ORDER_OPTIMIZABLE);
    // Used to add a tag to a cell when a view modifies a table property to indicate that this
    // property should not be derived from the base table
    public static final byte[] VIEW_MODIFIED_PROPERTY_BYTES = TagUtil.fromList(ImmutableList.<Tag>of(new ArrayBackedTag(VIEW_MODIFIED_PROPERTY_TAG_TYPE, Bytes.toBytes(1))));
}
