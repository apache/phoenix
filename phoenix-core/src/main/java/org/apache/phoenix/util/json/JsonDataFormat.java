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
package org.apache.phoenix.util.json;

import java.nio.ByteBuffer;

public interface JsonDataFormat {
    /**
     * Return the byte[] of the Json Object of the underlying format.
     * @param object
     * @return
     */
    byte[] toBytes(Object object);

    /**
     * Return the Object corresponding to the Data format in which JSON is stored
     * @param value
     * @return
     */
    Object toObject(String value);

    /**
     * Return the Object corresponding to the Data format in which JSON is stored
     * @param bytes
     * @param offset
     * @param length
     * @return
     */
    Object toObject(byte[] bytes, int offset, int length);

    /**
     * Get the estimated size of the object - Json
     * @param o
     * @return
     */
    int estimateByteSize(Object o);

    /**
     * Get the type of the value in the Json in the specified path. The type confirms to a
     * java.sql.Types
     * @param obj
     * @param jsonPathExprStr
     * @return
     */
    int getValueType(Object obj, String jsonPathExprStr);

    /**
     * Get the value from Json in the specified path
     * @param obj
     * @param jsonPathExprStr
     * @return
     */
    Object getValue(Object obj, String jsonPathExprStr);

    /**
     * Update the value in the Json path and return the ByteBuffer
     * @param top
     * @param jsonPathExprStr
     * @param newVal
     * @return
     */
    ByteBuffer updateValue(Object top, String jsonPathExprStr, String newVal);

    /**
     * Checks if the path is valid in a JSON document.
     * @param top
     * @param path
     * @return
     */
    boolean isPathValid(Object top, String path);
}