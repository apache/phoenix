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
package org.apache.phoenix.exception;

import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;

/**
 * Exception thrown when attempting an invalid cluster role transition.
 */
public class InvalidClusterRoleTransitionException extends Exception {
  private static final long serialVersionUID = 1L;

  private final HAGroupState fromState;
  private final HAGroupState toState;

  /**
   * @param msg reason for the exception
   */
  public InvalidClusterRoleTransitionException(String msg) {
    this(msg, null, null);
  }

  /**
   * @param msg   reason for the exception
   * @param cause the underlying cause
   */
  public InvalidClusterRoleTransitionException(String msg, Throwable cause) {
    super(msg, cause);
    this.fromState = null;
    this.toState = null;
  }

  /**
   * @param msg       reason for the exception
   * @param fromState the state the transition was rejected from
   * @param toState   the requested target state
   */
  public InvalidClusterRoleTransitionException(String msg, HAGroupState fromState,
    HAGroupState toState) {
    super(msg);
    this.fromState = fromState;
    this.toState = toState;
  }

  /** Returns the state the transition was rejected from, or null if not known. */
  public HAGroupState getFromState() {
    return fromState;
  }

  /** Returns the requested target state, or null if not known. */
  public HAGroupState getToState() {
    return toState;
  }
}
