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

/**
 * Stub class for HTrace Sampler. This is a no-op replacement for
 * {@code org.apache.htrace.Sampler} to remove the HTrace dependency.
 */
public interface Sampler<T> {

  boolean next(T info);

  Sampler ALWAYS = new Sampler() {
    @Override
    public boolean next(Object info) {
      return true;
    }

    @Override
    public String toString() {
      return "ALWAYS";
    }
  };

  Sampler NEVER = new Sampler() {
    @Override
    public boolean next(Object info) {
      return false;
    }

    @Override
    public String toString() {
      return "NEVER";
    }
  };
}
