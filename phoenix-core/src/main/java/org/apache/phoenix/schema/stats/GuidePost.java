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
package org.apache.phoenix.schema.stats;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.ByteUtil;

public final class GuidePost {
    public final static byte[] EMPTY_GUIDEPOST_KEY = ByteUtil.EMPTY_BYTE_ARRAY;

    private final byte[] guidePostKey;
    private final GuidePostEstimation estimation;

    public GuidePost(byte[] guidePostKey, GuidePostEstimation estimation) {
        this.guidePostKey = guidePostKey;
        this.estimation = estimation;
    }

    public byte[] getGuidePostKey() {
        return guidePostKey;
    }

    public GuidePostEstimation getEstimation() {
        return estimation;
    }

    public static boolean isEmptyGuidePostKey(byte[] key) {
        return Bytes.equals(key, GuidePost.EMPTY_GUIDEPOST_KEY);
    }
}