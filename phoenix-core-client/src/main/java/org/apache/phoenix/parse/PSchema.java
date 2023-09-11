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
package org.apache.phoenix.parse;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.coprocessor.generated.PSchemaProtos;
import org.apache.phoenix.schema.PMetaDataEntity;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.SizedUtil;

public class PSchema implements PMetaDataEntity {

    private final PName schemaName;
    private PTableKey schemaKey;
    private long timeStamp;
    private int estimatedSize;

    public PSchema(long timeStamp) { // For index delete marker
        this.timeStamp = timeStamp;
        this.schemaName = null;
    }

    public PSchema(String schemaName) {
        this(schemaName, HConstants.LATEST_TIMESTAMP);
    }

    public PSchema(String schemaName, long timeStamp) {
        this.schemaName = PNameFactory.newName(SchemaUtil.normalizeIdentifier(schemaName));
        this.schemaKey = new PTableKey(null, this.schemaName.getString());
        this.timeStamp = timeStamp;
        this.estimatedSize = SizedUtil.INT_SIZE + SizedUtil.LONG_SIZE + PNameFactory.getEstimatedSize(this.schemaName);
    }

    public PSchema(PSchema schema) {
        this(schema.getSchemaName().toString(), schema.getTimeStamp());
    }

    public String getSchemaName() {
        return schemaName == null ? null : schemaName.getString();
    }

    public PTableKey getSchemaKey() {
        return schemaKey;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public static PSchemaProtos.PSchema toProto(PSchema schema) {
        PSchemaProtos.PSchema.Builder builder = PSchemaProtos.PSchema.newBuilder();
        builder.setSchemaName(schema.getSchemaName());
        builder.setTimeStamp(schema.getTimeStamp());
        return builder.build();
    }

    public static PSchema createFromProto(PSchemaProtos.PSchema schema) {
        long timeStamp = schema.getTimeStamp();
        String schemaName = schema.getSchemaName();
        return new PSchema(schemaName, timeStamp);
    }

    public int getEstimatedSize() {
        return estimatedSize;
    }

}

