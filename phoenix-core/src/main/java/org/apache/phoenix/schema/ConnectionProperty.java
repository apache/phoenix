/*
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
package org.apache.phoenix.schema;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;

public enum ConnectionProperty {
    /**
     * Connection level property phoenix.default.update.cache.frequency
     */
    UPDATE_CACHE_FREQUENCY() {
        @Override
        public Object getValue(String value) {
            if (value == null) {
                return QueryServicesOptions.DEFAULT_UPDATE_CACHE_FREQUENCY;
            }

            if ("ALWAYS".equalsIgnoreCase(value)) {
                return 0L;
            }

            if ("NEVER".equalsIgnoreCase(value)) {
                return Long.MAX_VALUE;
            }

            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Connection's " +
                        QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB +
                        " can only be set to 'ALWAYS', 'NEVER' or a millisecond numeric value.");
            }
        }
    };

    public Object getValue(String value) {
        return value;
    }
}
