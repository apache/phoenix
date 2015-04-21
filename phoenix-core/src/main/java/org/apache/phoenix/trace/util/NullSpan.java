/**
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
package org.apache.phoenix.trace.util;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.htrace.Span;
import org.apache.htrace.TimelineAnnotation;
import org.apache.phoenix.util.StringUtil;

/**
 * Fake {@link Span} that doesn't save any state, in place of <tt>null</tt> return values, to avoid
 * <tt>null</tt> check.
 */
public class NullSpan implements Span {

  public static Span INSTANCE = new NullSpan();

  /**
   * Private constructor to limit garbage
   */
  private NullSpan() {
  }

  @Override
  public void stop() {
  }

  @Override
  public long getStartTimeMillis() {
    return 0;
  }

  @Override
  public long getStopTimeMillis() {
    return 0;
  }

  @Override
  public long getAccumulatedMillis() {
    return 0;
  }

  @Override
  public boolean isRunning() {
    return false;
  }

  @Override
  public String getDescription() {
    return "NullSpan";
  }

  @Override
  public long getSpanId() {
    return 0;
  }

  @Override
  public long getTraceId() {
    return 0;
  }

  @Override
  public Span child(String description) {
    return INSTANCE;
  }

  @Override
  public long getParentId() {
    return 0;
  }

  @Override
  public void addKVAnnotation(byte[] key, byte[] value) {
  }

  @Override
  public void addTimelineAnnotation(String msg) {
  }

  @Override
  public Map<byte[], byte[]> getKVAnnotations() {
    return Collections.emptyMap();
  }

  @Override
  public List<TimelineAnnotation> getTimelineAnnotations() {
    return Collections.emptyList();
  }

  @Override
  public String getProcessId() {
    return null;
  }

  @Override
  public String toJson() {
    return StringUtil.EMPTY_STRING;
  }
}
