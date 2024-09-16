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
package org.apache.phoenix.compile;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;

/**
 * CompiledOffset represents the result of the Compiler on the OFFSET clause.
 */
public class CompiledOffset {
    public static final CompiledOffset EMPTY_COMPILED_OFFSET =
            new CompiledOffset(Optional.<Integer>absent(), Optional.<byte[]>absent());
    private final Optional<Integer> integerOffset;
    private final Optional<byte[]> byteOffset;

    public CompiledOffset(Optional<Integer> integerOffset, Optional<byte[]> byteOffset) {
        this.integerOffset = integerOffset;
        this.byteOffset = byteOffset;
    }

    public Optional<Integer> getIntegerOffset() {
        return integerOffset;
    }

    public Optional<byte[]> getByteOffset() {
        return byteOffset;
    }
}
