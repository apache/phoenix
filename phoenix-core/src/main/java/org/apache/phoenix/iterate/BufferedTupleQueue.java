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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.ResultUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

public class BufferedTupleQueue extends BufferedQueue<Tuple> {

    public BufferedTupleQueue(long thresholdBytes) {
        super(thresholdBytes);
    }

    @Override
    protected BufferedSegmentQueue<Tuple> createSegmentQueue(int index, long thresholdBytes) {
        return new BufferedTupleSegmentQueue(index, thresholdBytes, false);
    }

    @Override
    protected Comparator<BufferedSegmentQueue<Tuple>> getSegmentQueueComparator() {
        return new Comparator<BufferedSegmentQueue<Tuple>>() {
            @Override
            public int compare(BufferedSegmentQueue<Tuple> q1, BufferedSegmentQueue<Tuple> q2) {
                return q1.index() - q2.index();
            }
        };
    }

    @Override
    public Iterator<Tuple> iterator() {
        return new Iterator<Tuple>() {
            private Iterator<BufferedSegmentQueue<Tuple>> queueIter;
            private Iterator<Tuple> currentIter;
            {
                this.queueIter = getSegmentQueues().iterator();
                this.currentIter = queueIter.hasNext() ? queueIter.next().iterator() : null;
            }

            @Override
            public boolean hasNext() {
                return currentIter != null && currentIter.hasNext();
            }

            @Override
            public Tuple next() {
                if (!hasNext()) return null;

                Tuple ret = currentIter.next();
                if (!currentIter.hasNext()) {
                    this.currentIter = queueIter.hasNext() ? queueIter.next().iterator() : null;
                }

                return ret;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    private static class BufferedTupleSegmentQueue extends BufferedSegmentQueue<Tuple> {
        private LinkedList<Tuple> results;

        public BufferedTupleSegmentQueue(int index, long thresholdBytes, boolean hasMaxQueueSize) {
            super(index, thresholdBytes, hasMaxQueueSize);
            this.results = Lists.newLinkedList();
        }

        @Override
        protected Queue<Tuple> getInMemoryQueue() {
            return results;
        }

        @Override
        protected long sizeOf(Tuple e) {
            KeyValue kv = PhoenixKeyValueUtil.maybeCopyCell(e.getValue(0));
            return Bytes.SIZEOF_INT * 2 + kv.getLength();
        }

        @Override
        protected void writeToStream(DataOutputStream out, Tuple e) throws IOException {
            KeyValue kv = PhoenixKeyValueUtil.maybeCopyCell(e.getValue(0));
            out.writeInt(kv.getLength() + Bytes.SIZEOF_INT);
            out.writeInt(kv.getLength());
            out.write(kv.getBuffer(), kv.getOffset(), kv.getLength());
        }

        @Override
        protected Tuple readFromStream(DataInputStream in) throws IOException {
            int length = in.readInt();
            if (length < 0) return null;

            byte[] b = new byte[length];
            in.readFully(b);
            Result result = ResultUtil.toResult(new ImmutableBytesWritable(b));
            return new ResultTuple(result);
        }

    }
}