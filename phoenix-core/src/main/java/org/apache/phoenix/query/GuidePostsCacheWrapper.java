/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.query;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.apache.phoenix.util.SchemaUtil;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class GuidePostsCacheWrapper {

    private final GuidePostsCache guidePostsCache;

    GuidePostsCacheWrapper(GuidePostsCache guidePostsCache){
        this.guidePostsCache = guidePostsCache;
    }

    GuidePostsInfo get(GuidePostsKey key) throws ExecutionException {
        return guidePostsCache.get(key);
    }

    void put(GuidePostsKey key, GuidePostsInfo info){
        guidePostsCache.put(key,info);
    }

    void invalidate(GuidePostsKey key){
        guidePostsCache.invalidate(key);
    }

    void invalidateAll(){
        guidePostsCache.invalidateAll();
    }

    public void invalidateAll(TableDescriptor htableDesc) {
        Preconditions.checkNotNull(htableDesc);
        byte[] tableName = htableDesc.getTableName().getName();
        for (byte[] fam : htableDesc.getColumnFamilyNames()) {
            invalidate(new GuidePostsKey(tableName, fam));
        }
    }

    public void invalidateAll(PTable table) {
        Preconditions.checkNotNull(table);
        byte[] physicalName = table.getPhysicalName().getBytes();
        List<PColumnFamily> families = table.getColumnFamilies();
        if (families.isEmpty()) {
            invalidate(new GuidePostsKey(physicalName, SchemaUtil.getEmptyColumnFamily(table)));
        } else {
            for (PColumnFamily family : families) {
                invalidate(new GuidePostsKey(physicalName, family.getName().getBytes()));
            }
        }
    }
}
