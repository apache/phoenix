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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.SchemaUtil;

public class PNameFactory {
    public static int getEstimatedSize(PName name) {
        return name == null ? 0 : name.getEstimatedSize();
    }

    private PNameFactory() {
    }

    public static PName newNormalizedName(String name) {
        return newName(SchemaUtil.normalizeIdentifier(name));
    }
    
    public static PName newName(String name) {
        return name == null || name.isEmpty() ? PName.EMPTY_NAME : 
            name.equals(QueryConstants.EMPTY_COLUMN_NAME ) ?  PName.EMPTY_COLUMN_NAME : 
                new PNameImpl(name);
    }
    
    public static PName newName(byte[] bytes) {
        return bytes == null || bytes.length == 0 ? PName.EMPTY_NAME : 
            Bytes.compareTo(bytes, QueryConstants.EMPTY_COLUMN_BYTES) == 0 ? PName.EMPTY_COLUMN_NAME :
                new PNameImpl(bytes);
    }

    public static PName newName(byte[] bytes, int offset, int length) {
        if (bytes == null || length == 0) {
            return PName.EMPTY_NAME;
        }
        byte[] buf = new byte[length];
        System.arraycopy(bytes, offset, buf, 0, length);
        return new PNameImpl(buf);
    }
}
