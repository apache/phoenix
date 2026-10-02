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

/**
 * Built-in Phoenix SQL functions, including vector distance functions for nearest-neighbor search.
 *
 * <h2>Vector Distance Functions</h2>
 *
 * <p>Distance functions ({@link org.apache.phoenix.expression.function.L2DistanceFunction
 * L2_DISTANCE}, {@link org.apache.phoenix.expression.function.L2DistanceSquaredFunction
 * L2_DISTANCE_SQUARED}, {@link org.apache.phoenix.expression.function.CosineDistanceFunction
 * COSINE_DISTANCE}, and {@link org.apache.phoenix.expression.function.InnerProductDistanceFunction
 * INNER_PRODUCT}) evaluate similarity between vector expressions. Implementations operate directly
 * on packed byte representations and support bounded evaluation for early termination during
 * index scans.</p>
 *
 * <h2>SIMD Acceleration Architecture</h2>
 *
 * <p>Vector distance computation supports optional hardware acceleration via multi-release JAR
 * packaging. When built under the {@code java21} profile and executed in environments providing
 * the Panama Vector API ({@code jdk.incubator.vector}), SIMD-vectorized execution paths are
 * selected dynamically. Environments lacking incubator module support or running on baseline Java
 * runtimes execute equivalent scalar routines transparently.</p>
 */
package org.apache.phoenix.expression.function;
