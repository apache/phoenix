/**
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
package org.apache.phoenix.log;

import org.apache.phoenix.jdbc.PhoenixConnection;

import java.sql.SQLException;

/**
 * Default implementation of a ConnectionLimiter.
 */
public class DefaultConnectionLimiter extends BaseConnectionLimiter {
    private DefaultConnectionLimiter(Builder builder) {
        super(builder.profileName, builder.shouldThrottleNumConnections, builder.maxConnectionsAllowed, builder.maxInternalConnectionsAllowed);
    }

    public static class Builder {
        protected String profileName;
        protected int maxConnectionsAllowed;
        protected int maxInternalConnectionsAllowed;
        protected boolean shouldThrottleNumConnections;

        public Builder(boolean shouldThrottleNumConnections) {
            this.shouldThrottleNumConnections = shouldThrottleNumConnections;
        }

        public DefaultConnectionLimiter.Builder withConnectionProfile(String profileName) {
            this.profileName = profileName;
            return this;
        }

        public DefaultConnectionLimiter.Builder withMaxAllowed(int maxAllowed) {
            this.maxConnectionsAllowed = maxAllowed;
            return this;
        }

        public DefaultConnectionLimiter.Builder withMaxInternalAllowed(int maxInternalAllowed) {
            this.maxInternalConnectionsAllowed = maxInternalAllowed;
            return this;
        }


        public ConnectionLimiter build() {
            return new DefaultConnectionLimiter(this);
        }

    }

}
