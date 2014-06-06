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
package org.apache.phoenix.trace;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.phoenix.metrics.MetricsWriter;
import org.apache.phoenix.metrics.PhoenixAbstractMetric;
import org.apache.phoenix.metrics.PhoenixMetricTag;
import org.apache.phoenix.metrics.PhoenixMetricsRecord;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

/**
 * Translate metrics from a Hadoop2 metrics2 metric to a generic PhoenixMetric that a
 * {@link MetricsWriter} can then write out.
 * <p>
 * This class becomes unnecessary once we drop Hadoop1 support.
 */
public class PhoenixMetricsWriter implements MetricsSink, TestableMetricsWriter {

  /**
   * Metrics configuration key for the class that should be used for writing the output
   */
  public static final String PHOENIX_METRICS_WRITER_CLASS = "phoenix.sink.writer-class";

  public static void setWriterClass(MetricsWriter writer, Configuration conf) {
    conf.setProperty(PHOENIX_METRICS_WRITER_CLASS, writer.getClass().getName());
  }

  private MetricsWriter writer;

  @Override
  public void init(SubsetConfiguration config) {
    // instantiate the configured writer class
    String clazz = config.getString(PHOENIX_METRICS_WRITER_CLASS);
    this.writer = TracingCompat.initializeWriter(clazz);
    Preconditions.checkNotNull(writer, "Could not correctly initialize metrics writer!");
  }

  @Override
  @VisibleForTesting
  public void setWriterForTesting(MetricsWriter writer) {
    this.writer = writer;
  }

  @Override
  public void putMetrics(MetricsRecord record) {
    writer.addMetrics(wrap(record));
  }

  @Override
  public void flush() {
    writer.flush();
  }

  /**
   * Convert the passed record to a {@link PhoenixMetricsRecord}
   * @param record to convert
   * @return a generic {@link PhoenixMetricsRecord} that delegates to the record in all things
   */
  private PhoenixMetricsRecord wrap(final MetricsRecord record) {
    return new PhoenixMetricsRecord() {

      @Override
      public String name() {
        return record.name();
      }

      @Override
      public String description() {
        return record.description();
      }

      @Override
      public Iterable<PhoenixAbstractMetric> metrics() {
        final Iterable<AbstractMetric> iterable = record.metrics();
        return new Iterable<PhoenixAbstractMetric>(){

          @Override
          public Iterator<PhoenixAbstractMetric> iterator() {
            final Iterator<AbstractMetric> iter = iterable.iterator();
            return Iterators.transform(iter, new Function<AbstractMetric, PhoenixAbstractMetric>() {

              @Override
              @Nullable
              public PhoenixAbstractMetric apply(@Nullable final AbstractMetric input) {
                if (input == null) {
                  return null;
                }
                return new PhoenixAbstractMetric() {

                  @Override
                  public Number value() {
                    return input.value();
                  }

                  @Override
                  public String getName() {
                    return input.name();
                  }

                  @Override
                  public String toString() {
                    return input.toString();
                  }
                };
              }
            });
          }
        };
      }

      @Override
      public Collection<PhoenixMetricTag> tags() {
        Collection<PhoenixMetricTag> tags = new ArrayList<PhoenixMetricTag>();
        Collection<MetricsTag> origTags = record.tags();
        for (final MetricsTag tag : origTags) {
          tags.add(new PhoenixMetricTag() {

            @Override
            public String name() {
              return tag.name();
            }

            @Override
            public String description() {
              return tag.description();
            }

            @Override
            public String value() {
              return tag.value();
            }

            @Override
            public String toString() {
              return tag.toString();
            }

          });
        }
        return tags;
      }

    };
  }
}