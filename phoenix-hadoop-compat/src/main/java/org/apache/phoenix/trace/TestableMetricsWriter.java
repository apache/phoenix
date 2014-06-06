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

import org.apache.phoenix.metrics.MetricsWriter;

/**
 * Marker interface for a MetricsWriter that can be registered to the current metrics system. The
 * writer should convert from the metrics information it receives from the metrics system to Phoenix
 * records that the MetricsWriter can read (and subsequently write).
 */
public interface TestableMetricsWriter {

    public void setWriterForTesting(MetricsWriter writer);
}