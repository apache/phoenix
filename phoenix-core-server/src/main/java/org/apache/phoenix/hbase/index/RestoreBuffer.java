/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hbase.index;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Recovered edits of one region waiting to be re-shipped during WAL replay. An edit's embedded
 * watermark W is below its own sequence id, so the W that covers an edit only appears in a later
 * edit: edits are held until a later W covers them (dropped, already replicated) or the caller
 * drains them to ship. An edit without a watermark covers nothing but is pruned like any other once
 * a watermark covers it. Used only from the region's replay thread; not thread-safe.
 */
final class RestoreBuffer<E extends RestoreBuffer.Edit> {

  /** The watermark of an edit that carries none. */
  static final long NO_WATERMARK = Long.MIN_VALUE;

  /** A buffered edit. */
  interface Edit {
    long seqId();

    /** The edit's size, summed by {@link RestoreBuffer#bytes()}. */
    long bytes();
  }

  private ArrayDeque<E> buffered = new ArrayDeque<>();
  private long maxWatermark = NO_WATERMARK;
  private long bytes;

  /**
   * Takes the edit's watermark into account and buffers the edit unless a watermark covers it.
   * Replay offers a file's edits in sequence id order, so the covered edits are a prefix of the
   * buffer. If the order breaks, a covered edit behind an uncovered one stays and is re-shipped.
   * @return true if the edit was buffered, false if it is already replicated
   */
  boolean offer(E edit, long watermark) {
    if (watermark > maxWatermark) {
      maxWatermark = watermark;
      while (!buffered.isEmpty() && buffered.peekFirst().seqId() <= maxWatermark) {
        bytes -= buffered.pollFirst().bytes();
      }
    }
    if (edit.seqId() <= maxWatermark) {
      return false;
    }
    buffered.addLast(edit);
    bytes += edit.bytes();
    return true;
  }

  /**
   * Removes and returns the buffered edits, in the order they were offered. The largest watermark
   * seen is kept, so it still covers edits offered later in the open.
   */
  Deque<E> drain() {
    Deque<E> edits = buffered;
    buffered = new ArrayDeque<>();
    bytes = 0;
    return edits;
  }

  boolean isEmpty() {
    return buffered.isEmpty();
  }

  int size() {
    return buffered.size();
  }

  /** The summed size of the buffered edits. */
  long bytes() {
    return bytes;
  }

  /** The largest watermark seen so far, or {@link #NO_WATERMARK}. */
  long maxWatermark() {
    return maxWatermark;
  }
}
