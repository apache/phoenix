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
package org.apache.phoenix.coprocessorclient.metrics;

/**
 * Factory object to create various metric sources for phoenix related coprocessors.
 */
public class MetricsPhoenixCoprocessorSourceFactory {

  private static final MetricsPhoenixCoprocessorSourceFactory INSTANCE =
    new MetricsPhoenixCoprocessorSourceFactory();
  private static volatile MetricsMetadataCachingSource metadataCachingSource;
  private static volatile MetricsPhoenixMasterSource phoenixMasterSource;

  public static MetricsPhoenixCoprocessorSourceFactory getInstance() {
    return INSTANCE;
  }

  public MetricsMetadataCachingSource getMetadataCachingSource() {
    if (INSTANCE.metadataCachingSource == null) {
      synchronized (MetricsMetadataCachingSource.class) {
        if (INSTANCE.metadataCachingSource == null) {
          INSTANCE.metadataCachingSource = new MetricsMetadataCachingSourceImpl();
        }
      }
    }
    return INSTANCE.metadataCachingSource;
  }

  public MetricsPhoenixMasterSource getPhoenixMasterSource() {
    if (INSTANCE.phoenixMasterSource == null) {
      synchronized (MetricsPhoenixMasterSource.class) {
        if (INSTANCE.phoenixMasterSource == null) {
          INSTANCE.phoenixMasterSource = new MetricsPhoenixMasterSourceImpl();
        }
      }
    }
    return INSTANCE.phoenixMasterSource;
  }
}
