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
package org.apache.phoenix.optimize;

import java.util.Objects;

/**
 * An index the optimizer considered as a candidate but did not choose, paired with the reason it
 * was rejected. The reason is one of the closed-set {@code REASON_*} labels in
 * {@link OptimizerReasons}.
 */
public final class RejectedIndexEntry {
  private final String name;
  private final String reason;

  public RejectedIndexEntry(String name, String reason) {
    this.name = Objects.requireNonNull(name, "name");
    this.reason = Objects.requireNonNull(reason, "reason");
  }

  public String getName() {
    return name;
  }

  public String getReason() {
    return reason;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RejectedIndexEntry)) {
      return false;
    }
    RejectedIndexEntry that = (RejectedIndexEntry) o;
    return name.equals(that.name) && reason.equals(that.reason);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, reason);
  }

  @Override
  public String toString() {
    return "RejectedIndexEntry{name=" + name + ", reason=" + reason + "}";
  }
}
