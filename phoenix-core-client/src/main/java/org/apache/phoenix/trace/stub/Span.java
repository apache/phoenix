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
package org.apache.phoenix.trace.stub;

import java.util.List;
import java.util.Map;

/**
 * Stub interface for HTrace Span. This is a no-op replacement for
 * {@code org.apache.htrace.Span} to remove the HTrace dependency
 * while preserving the existing tracing API surface.
 */
public interface Span {

  long ROOT_SPAN_ID = 0;

  void stop();

  long getStartTimeMillis();

  long getStopTimeMillis();

  long getAccumulatedMillis();

  boolean isRunning();

  String getDescription();

  long getSpanId();

  long getTraceId();

  Span child(String description);

  long getParentId();

  void addKVAnnotation(byte[] key, byte[] value);

  void addTimelineAnnotation(String msg);

  Map<byte[], byte[]> getKVAnnotations();

  List<TimelineAnnotation> getTimelineAnnotations();

  String getProcessId();

  String toJson();
}
