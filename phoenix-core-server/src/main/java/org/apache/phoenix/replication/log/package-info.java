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
 * Provides classes for managing the custom binary log format used in Phoenix replication. This
 * package defines the structure of the replication log file, including its header, blocks,
 * trailer, and individual records representing mutations.
 * <p>
 * Key components include:
 * <ul>
 *   <li>{@link org.apache.phoenix.replication.log.LogFile}: Defines the core interfaces for the
 *       log file structure ({@code Header}, {@code BlockHeader}, {@code Trailer}, {@code Record})
 *       and its I/O operations ({@code Writer}, {@code Reader}, {@code Codec}).</li>
 *   <li>{@link org.apache.phoenix.replication.log.LogFileWriter}: Implementation for writing
 *       replication log files.</li>
 *   <li>{@link org.apache.phoenix.replication.log.LogFileReader}: Implementation for reading
 *       replication log files.</li>
 *   <li>{@link org.apache.phoenix.replication.log.LogFileFormatWriter}: Handles low-level writing
 *       of blocks, including compression and checksums.</li>
 *   <li>{@link org.apache.phoenix.replication.log.LogFileFormatReader}: Handles low-level reading
 *       of blocks, including decompression and checksum validation.</li>
 *   <li>{@link org.apache.phoenix.replication.log.LogFileCodec}: Default implementation for
 *       encoding/decoding {@code Record} objects within blocks.</li>
 *   <li>{@link org.apache.phoenix.replication.log.LogFileRecord}: Concrete implementation of a
 *       log {@code Record}.</li>
 *   <li>{@link org.apache.phoenix.replication.log.LogFileHeader},
 *       {@link org.apache.phoenix.replication.log.LogBlockHeader},
 *       {@link org.apache.phoenix.replication.log.LogFileTrailer}: Concrete implementations for
 *       file/block metadata.</li>
 *   <li>{@link org.apache.phoenix.replication.log.LogFileWriterContext},
 *       {@link org.apache.phoenix.replication.log.LogFileReaderContext}: Context objects holding
 *       configuration and state for writers and readers.</li>
 *   <li>{@link org.apache.phoenix.replication.log.SyncableDataOutput},
 *       {@link org.apache.phoenix.replication.log.SeekableDataInput}: Abstractions for underlying
 *       storage output/input streams.</li>
 * </ul>
 */
package org.apache.phoenix.replication.log;