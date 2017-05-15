/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.MetaDataUtil;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CatalogInfo {

    private List<Mutation> allMutations;

    public CatalogInfo(List<Mutation> mutations) {
        this.allMutations = mutations;
    }

    public LinkedList<MutationType> getGroupedMutations() {
        LinkedList<MutationType> result = Lists.newLinkedList();
        Iterator<Map.Entry<String, MutationType>> iterator = groupByTable().entrySet().iterator();
        while (iterator.hasNext()) {
            result.addLast(iterator.next().getValue());
        }
        return result;
    }

    private LinkedHashMap<String, MutationType> groupByTable() {
        LinkedHashMap<String, MutationType> map = Maps.newLinkedHashMap();
        for (Mutation dataMutation : allMutations) {
            String groupBy = Bytes.toString(MetaDataUtil.getTenantIdAndSchemaAndTableName(dataMutation));
            if (!map.containsKey(groupBy)) {
                map.put(groupBy, new MutationType());
            }
            map.get(groupBy).addMutation(dataMutation);
        }
        return map;
    }

    static class MutationType {
        private final List<Mutation> dataMutations;
        private final List<Mutation> linkMutations;

        public MutationType() {
            this.dataMutations = Lists.newArrayList();
            this.linkMutations = Lists.newArrayList();
        }

        public void addMutation(Mutation mutation) {
            if (MetaDataUtil.isLinkingRow(mutation)) {
                this.linkMutations.add(mutation);
            } else {
                this.dataMutations.add(mutation);
            }
        }

        public List<Mutation> getDataMutations() {
            return dataMutations;
        }

        public List<Mutation> getLinkMutations() {
            return linkMutations;
        }

        public List<Mutation> getAllMutations() {
            List<Mutation> result = Lists.newArrayList(dataMutations);
            result.addAll(linkMutations);
            return result;
        }
    }
}
