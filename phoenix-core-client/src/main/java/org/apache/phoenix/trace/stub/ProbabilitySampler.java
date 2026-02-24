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

import java.util.concurrent.ThreadLocalRandom;

/**
 * Stub class for HTrace ProbabilitySampler. This is a no-op replacement for
 * {@code org.apache.htrace.impl.ProbabilitySampler} to remove the HTrace dependency.
 */
public class ProbabilitySampler implements Sampler<Object> {

  public static final String SAMPLER_FRACTION_CONF_KEY = "sampler.fraction";

  private final double threshold;

  public ProbabilitySampler(HTraceConfiguration conf) {
    this.threshold = Double.parseDouble(conf.get(SAMPLER_FRACTION_CONF_KEY, "0.0"));
  }

  @Override
  public boolean next(Object info) {
    return ThreadLocalRandom.current().nextDouble() < threshold;
  }
}
