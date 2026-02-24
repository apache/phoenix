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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Stub class for HTrace MilliSpan. This is a no-op replacement for
 * {@code org.apache.htrace.impl.MilliSpan} to remove the HTrace dependency.
 * Used primarily in tests.
 */
public class MilliSpan implements Span {

  private final String description;
  private final long traceId;
  private final long spanId;
  private final long parentId;
  private final long startTime;
  private long stopTime;
  private boolean running;
  private final String processId;
  private final Map<byte[], byte[]> kvAnnotations = new TreeMap<>(
    (a, b) -> {
      int len = Math.min(a.length, b.length);
      for (int i = 0; i < len; i++) {
        if (a[i] != b[i]) return (a[i] & 0xff) - (b[i] & 0xff);
      }
      return a.length - b.length;
    });
  private final List<TimelineAnnotation> timelineAnnotations = new ArrayList<>();

  /**
   * Legacy constructor matching the original HTrace MilliSpan(String, int, int, int, String)
   * signature used in tests.
   */
  public MilliSpan(String description, long traceId, long spanId, long parentId,
    String processId) {
    this.description = description;
    this.traceId = traceId;
    this.spanId = spanId;
    this.parentId = parentId;
    this.startTime = System.currentTimeMillis();
    this.processId = processId;
    this.running = true;
  }

  private MilliSpan(Builder builder) {
    this.description = builder.description;
    this.traceId = builder.traceId;
    this.spanId = builder.spanId;
    this.parentId = builder.parents != null && builder.parents.length > 0
      ? builder.parents[0] : builder.parentId;
    this.startTime = builder.startTime > 0 ? builder.startTime : System.currentTimeMillis();
    this.stopTime = builder.endTime;
    this.processId = builder.processId;
    this.running = builder.endTime <= 0;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String description = "";
    private long traceId;
    private long spanId;
    private long parentId;
    private long[] parents;
    private long startTime;
    private long endTime;
    private String processId = "";

    public Builder description(String description) {
      this.description = description;
      return this;
    }

    public Builder traceId(long traceId) {
      this.traceId = traceId;
      return this;
    }

    public Builder spanId(long spanId) {
      this.spanId = spanId;
      return this;
    }

    public Builder parentId(long parentId) {
      this.parentId = parentId;
      return this;
    }

    public Builder parents(long[] parents) {
      this.parents = parents;
      if (parents != null && parents.length > 0) {
        this.parentId = parents[0];
      }
      return this;
    }

    public Builder begin(long startTime) {
      this.startTime = startTime;
      return this;
    }

    public Builder end(long endTime) {
      this.endTime = endTime;
      return this;
    }

    public Builder processId(String processId) {
      this.processId = processId;
      return this;
    }

    public MilliSpan build() {
      return new MilliSpan(this);
    }
  }

  @Override
  public void stop() {
    if (running) {
      stopTime = System.currentTimeMillis();
      running = false;
    }
  }

  @Override
  public long getStartTimeMillis() {
    return startTime;
  }

  @Override
  public long getStopTimeMillis() {
    return stopTime;
  }

  @Override
  public long getAccumulatedMillis() {
    return running ? System.currentTimeMillis() - startTime : stopTime - startTime;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public long getSpanId() {
    return spanId;
  }

  @Override
  public long getTraceId() {
    return traceId;
  }

  @Override
  public Span child(String description) {
    return MilliSpan.builder()
      .description(description)
      .traceId(traceId)
      .parentId(spanId)
      .processId(processId)
      .build();
  }

  @Override
  public long getParentId() {
    return parentId;
  }

  @Override
  public void addKVAnnotation(byte[] key, byte[] value) {
    kvAnnotations.put(key, value);
  }

  @Override
  public void addTimelineAnnotation(String msg) {
    timelineAnnotations.add(new TimelineAnnotation(System.currentTimeMillis(), msg));
  }

  @Override
  public Map<byte[], byte[]> getKVAnnotations() {
    return Collections.unmodifiableMap(kvAnnotations);
  }

  @Override
  public List<TimelineAnnotation> getTimelineAnnotations() {
    return Collections.unmodifiableList(timelineAnnotations);
  }

  @Override
  public String getProcessId() {
    return processId;
  }

  @Override
  public String toJson() {
    return "{\"description\":\"" + description + "\",\"traceId\":" + traceId
      + ",\"spanId\":" + spanId + ",\"parentId\":" + parentId + "}";
  }
}
