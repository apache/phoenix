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
package org.apache.phoenix.metrics;

/**
 * Shared Hadoop Metrics2 constant definitions used across Phoenix metric sources.
 * <p>
 * Holds the {@code ha_group} tag registered by the per-HA-group sources (HAGroupStore and the
 * replication log sources) so their series can be sliced per HA group downstream. Lives in this
 * module-neutral package so both {@code phoenix-core-client} and {@code phoenix-core-server}
 * sources can reference it.
 */
public final class MetricConstants {

  /** Metrics2 tag name carrying the HA group name. */
  public static final String HA_GROUP_TAG_NAME = "ha_group";

  /** Description for the {@link #HA_GROUP_TAG_NAME} tag. */
  public static final String HA_GROUP_TAG_DESC = "HA group name";

  private MetricConstants() {
  }
}
