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
package org.apache.phoenix.iterate;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.iterate.OrderedResultIterator.ResultEntry;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.PhoenixKeyValueUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.MinMaxPriorityQueue;

public class PhoenixQueues {

    private PhoenixQueues() {
    }

    public static SizeAwareQueue<ResultEntry> newBufferedResultEntrySortedQueue(
            Comparator<ResultEntry> comparator, Integer limit, long thresholdBytes)
            throws IOException {
        return new BufferedSortedQueue(comparator, limit, thresholdBytes);
    }

    public static SizeAwareQueue<Tuple> newBufferedTupleQueue(long thresholdBytes) {
        return new BufferedTupleQueue(thresholdBytes);
    }

    public static SizeAwareQueue<ResultEntry> newSizeBoundResultEntrySortedQueue(
            Comparator<ResultEntry> comparator, Integer limit, long maxSizeBytes) {
        limit = limit == null ? -1 : limit;
        MinMaxPriorityQueue<ResultEntry> queue =
                limit < 0 ? MinMaxPriorityQueue.<ResultEntry> orderedBy(comparator).create()
                        : MinMaxPriorityQueue.<ResultEntry> orderedBy(comparator).maximumSize(limit)
                                .create();
        return new SizeBoundQueue<ResultEntry>(maxSizeBytes, queue) {
            @Override
            public long sizeOf(org.apache.phoenix.iterate.OrderedResultIterator.ResultEntry e) {
                return ResultEntry.sizeOf(e);
            }

        };
    }

    public static SizeAwareQueue<Tuple> newSizeBoundTupleQueue(long maxSizeBytes) {
        LinkedList<Tuple> results = Lists.newLinkedList();
        return new SizeBoundQueue<Tuple>(maxSizeBytes, results) {

            @Override
            public long sizeOf(Tuple e) {
                KeyValue kv = PhoenixKeyValueUtil.maybeCopyCell(e.getValue(0));
                return Bytes.SIZEOF_INT * 2 + kv.getLength();
            }

        };
    }

    public static SizeAwareQueue<ResultEntry> newResultEntrySortedQueue(
            Comparator<ResultEntry> comparator, Integer limit, boolean spoolingEnabled,
            long thresholdBytes) throws IOException {
        if (spoolingEnabled) {
            return newBufferedResultEntrySortedQueue(comparator, limit, thresholdBytes);
        } else {
            return newSizeBoundResultEntrySortedQueue(comparator, limit, thresholdBytes);
        }
    }

    public static SizeAwareQueue<Tuple> newTupleQueue(boolean spoolingEnabled,
            long thresholdBytes) {
        if (spoolingEnabled) {
            return newBufferedTupleQueue(thresholdBytes);
        } else {
            return newSizeBoundTupleQueue(thresholdBytes);
        }
    }

}
