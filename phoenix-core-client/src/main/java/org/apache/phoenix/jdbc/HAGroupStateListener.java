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
package org.apache.phoenix.jdbc;

import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;

/**
 * Interface for external clients who want to be notified of HA group state transitions.
 *
 * <p>Listeners can subscribe to be notified when:</p>
 * <ul>
 *   <li>Specific state transitions occur (from one state to another)</li>
 *   <li>Any transition to a target state occurs (from any state to a specific state)</li>
 * </ul>
 *
 * <p>Notifications are provided for both local and peer cluster state changes,
 * distinguished by the {@link ClusterType} parameter.</p>
 *
 * @see HAGroupStoreManager#subscribeToTargetState
 */
public interface HAGroupStateListener {

    /**
     * Called when an HA group state transition occurs.
     *
     * <p>Implementations should be fast and non-blocking to avoid impacting
     * the HA group state management system. If heavy processing is required,
     * consider delegating to a separate thread.</p>
     *
     * @param haGroupName the name of the HA group that transitioned
     * @param toState the new state after the transition
     * @param modifiedTime the time the state transition occurred
     * @param clusterType whether this transition occurred on the local or peer cluster
     *
     * @throws Exception implementations may throw exceptions, but they will be
     *                   logged and will not prevent other listeners from being notified
     */
    void onStateChange(String haGroupName,
                       HAGroupState toState,
                       long modifiedTime,
                       ClusterType clusterType);
}
