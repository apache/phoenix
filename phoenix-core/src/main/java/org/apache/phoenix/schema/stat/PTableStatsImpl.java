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
package org.apache.phoenix.schema.stat;
 import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

import com.google.common.collect.ImmutableMap;
 
 /**
  * Implementation for PTableStats.
  */
 public class PTableStatsImpl implements PTableStats {
 
   public static final PTableStats NO_STATS = new PTableStatsImpl();
   // The map for guide posts should be immutable. We only take the current
   // snapshot from outside
   // method call and store it.
   private Map<String, byte[]> regionGuidePosts = new HashMap<String, byte[]>();
   // TODO : see if a thread safe map would be needed here
   private Map<String, byte[]> minKey = new HashMap<String, byte[]>();
   private Map<String, byte[]> maxKey = new HashMap<String, byte[]>();
 
   public PTableStatsImpl() {
     this(new HashMap<String, byte[]>(), new HashMap<String, byte[]>(),
         new HashMap<String, byte[]>());
   }
 
   public PTableStatsImpl(Map<String, byte[]> guidePosts,
       Map<String, byte[]> minKey, Map<String, byte[]> maxKey) {
     this.regionGuidePosts = guidePosts;
     this.minKey = minKey;
     this.maxKey = maxKey;
   }
 
   @Override
   public byte[] getRegionGuidePostsOfARegion(HRegionInfo region) {
     return regionGuidePosts.get(Bytes.toString(region.getRegionName()));
   }
 
   @Override
   public Map<String, byte[]> getGuidePosts() {
     if (regionGuidePosts != null) {
       return ImmutableMap.copyOf(regionGuidePosts);
     }
 
     return null;
   }
 
   @Override
   public byte[] getMaxKeyOfARegion(HRegionInfo region) {
     return Bytes.copy(this.minKey.get(Bytes.toString(region.getRegionName())));
   }
 
   @Override
   public byte[] getMinKeyOfARegion(HRegionInfo region) {
     return Bytes.copy(this.maxKey.get(Bytes.toString(region.getRegionName())));
   }
 
   @Override
   public Map<String, byte[]> getMaxKey() {
     if (maxKey != null) {
       return ImmutableMap.copyOf(maxKey);
     }
     return null;
   }
 
   @Override
   public Map<String, byte[]> getMinKey() {
     if (minKey != null) {
       return ImmutableMap.copyOf(minKey);
     }
     return null;
   }
 
   @Override
   public void setMinKey(HRegionInfo region, byte[] minKey, int offset, int length) {
     ImmutableBytesPtr ptr = new ImmutableBytesPtr(minKey, offset, length);
     this.minKey.put(region.getRegionNameAsString(), ImmutableBytesPtr.copyBytesIfNecessary(ptr));
   }
 
   @Override
   public void setMaxKey(HRegionInfo region, byte[] maxKey, int offset, int length) {
     ImmutableBytesPtr ptr = new ImmutableBytesPtr(maxKey, offset, length);
     this.maxKey.put(region.getRegionNameAsString(), ImmutableBytesPtr.copyBytesIfNecessary(ptr));
   }
 
   @Override
   public void setGuidePosts(HRegionInfo region, byte[] guidePosts, int offset, int length) {
     ImmutableBytesPtr ptr = new ImmutableBytesPtr(guidePosts, offset, length);
     this.regionGuidePosts.put(region.getRegionNameAsString(),
         ImmutableBytesPtr.copyBytesIfNecessary(ptr));
   }
 
   @Override
   public void setMinKey(byte[] region, byte[] minKey, int offset, int length) {
     ImmutableBytesPtr ptr = new ImmutableBytesPtr(minKey, offset, length);
     this.minKey.put(Bytes.toString(region), ImmutableBytesPtr.copyBytesIfNecessary(ptr));
   }
 
   @Override
   public void setMaxKey(byte[] region, byte[] maxKey, int offset, int length) {
     ImmutableBytesPtr ptr = new ImmutableBytesPtr(maxKey, offset, length);
     this.maxKey.put(Bytes.toString(region), ImmutableBytesPtr.copyBytesIfNecessary(ptr));
   }
 
   @Override
   public void setGuidePosts(byte[] region, byte[] guidePosts, int offset, int length) {
     ImmutableBytesPtr ptr = new ImmutableBytesPtr(guidePosts, offset, length);
     this.regionGuidePosts.put(Bytes.toString(region), ImmutableBytesPtr.copyBytesIfNecessary(ptr));
   }
 
   @Override
   public boolean equals(Object obj) {
     if (!(obj instanceof PTableStats)) {
       return false;
     }
     PTableStats pTableStats = (PTableStats) obj;
     // compare the max keys
     Map<String, byte[]> thatMaxKey = pTableStats.getMaxKey();
     Map<String, byte[]> thisMaxKey = this.getMaxKey();
     if (thatMaxKey.size() != thisMaxKey.size()) {
       return false;
     } else {
       Set<Entry<String, byte[]>> thatEntrySet = thatMaxKey.entrySet();
       Set<Entry<String, byte[]>> thisEntrySet = thisMaxKey.entrySet();
       for (Entry<String, byte[]> thatEntry : thatEntrySet) {
         boolean matchFound = false;
         Entry<String, byte[]> temp = null;
         for (Entry<String, byte[]> thisEntry : thisEntrySet) {
           if (thatEntry.getKey().equals(thisEntry.getKey())) {
             matchFound = true;
             temp = thisEntry;
             break;
           }
         }
         if (!matchFound) {
           return false;
         } else {
           if (temp != null) {
             if (!Bytes.equals(thatEntry.getValue(), temp.getValue())) {
               return false;
             }
           } else {
             return false;
           }
         }
 
       }
     }
 
     // Check for min key
     Map<String, byte[]> thatMinKey = pTableStats.getMinKey();
     Map<String, byte[]> thisMinKey = this.getMinKey();
     if (thatMinKey.size() != thisMinKey.size()) {
       return false;
     } else {
       Set<Entry<String, byte[]>> thatEntrySet = thatMinKey.entrySet();
       Set<Entry<String, byte[]>> thisEntrySet = thisMinKey.entrySet();
       for (Entry<String, byte[]> thatEntry : thatEntrySet) {
         boolean matchFound = false;
         Entry<String, byte[]> temp = null;
         for (Entry<String, byte[]> thisEntry : thisEntrySet) {
           if (thatEntry.getKey().equals(thisEntry.getKey())) {
             matchFound = true;
             temp = thisEntry;
             break;
           }
         }
         if (!matchFound) {
           return false;
         } else {
           if (temp != null) {
             if (!Bytes.equals(thatEntry.getValue(), temp.getValue())) {
               return false;
             }
           } else {
             return false;
           }
         }
 
       }
     }
 
     // Check for guide posts
     Map<String, byte[]> thatGuidePosts = pTableStats.getGuidePosts();
     Map<String, byte[]> thisGuidePosts = this.getGuidePosts();
     if (thatGuidePosts.size() != thisGuidePosts.size()) {
       return false;
     } else {
       Set<Entry<String, byte[]>> thatEntrySet = thatGuidePosts.entrySet();
       Set<Entry<String, byte[]>> thisEntrySet = thisGuidePosts.entrySet();
       for (Entry<String, byte[]> thatEntry : thatEntrySet) {
         boolean matchFound = false;
         Entry<String, byte[]> temp = null;
         for (Entry<String, byte[]> thisEntry : thisEntrySet) {
           if (thatEntry.getKey().equals(thisEntry.getKey())) {
             matchFound = true;
             temp = thisEntry;
             break;
           }
         }
         if (!matchFound) {
           return false;
         } else {
           if (temp != null) {
             if (!Bytes.equals(thatEntry.getValue(), temp.getValue())) {
               return false;
             }
           } else {
             return false;
           }
         }
 
       }
     }
     return true;
   }

   @Override
   public int hashCode() {
     int hash = 1;
     Map<String, byte[]> maxKey = this.getMaxKey();
     Collection<byte[]> minValues = maxKey.values();
     for (byte[] value : minValues) {
       for (int i = 0; i < value.length; i++)
         hash += (31 * hash) + value[i];
     }
 
     Collection<byte[]> maxValues = maxKey.values();
     for (byte[] value : maxValues) {
       for (int i = 0; i < value.length; i++)
         hash += (31 * hash) + value[i];
     }
 
     Collection<byte[]> guidePostValues = maxKey.values();
     for (byte[] value : guidePostValues) {
       for (int i = 0; i < value.length; i++)
         hash += (31 * hash) + value[i];
     }
     return hash;
   }
}
