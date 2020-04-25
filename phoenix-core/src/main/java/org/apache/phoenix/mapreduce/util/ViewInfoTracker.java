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
package org.apache.phoenix.mapreduce.util;

import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ViewInfoTracker implements ViewInfoWritable {

    String tenantId;
    String viewName;
    long viewTtl;

    public ViewInfoTracker() {

    }
    public ViewInfoTracker(String tenantId, String viewName, long viewTtl) {
        setTenantId(tenantId);
        this.viewName = viewName;
        this.viewTtl = viewTtl;
    }

    private void setTenantId(String tenantId) {
        if (tenantId != null) {
            this.tenantId = tenantId;
        }
    }

    @Override
    public String getTenantId() {
        return tenantId;
    }

    @Override
    public String getViewName() {
        return viewName;
    }

    public Long getViewTtl() {
        return viewTtl;
    }

    @Override public void write(DataOutput output) throws IOException {
        WritableUtils.writeString(output, tenantId);
        WritableUtils.writeString(output, viewName);
        WritableUtils.writeVLong(output, viewTtl);
    }

    @Override public void readFields(DataInput input) throws IOException {
        setTenantId(WritableUtils.readString(input));
        viewName = WritableUtils.readString(input);
        viewTtl = WritableUtils.readVLong(input);
    }
}