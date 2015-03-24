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

package org.apache.phoenix.hbase.index.scanner;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;

/**
 * Scan the primary table. This is similar to HBase's scanner, but ensures that you will never see
 * deleted columns/rows
 */
public interface Scanner extends Closeable {

  /**
   * @return the next keyvalue in the scanner or <tt>null</tt> if there is no next {@link KeyValue}
   * @throws IOException if there is an underlying error reading the data
   */
  public Cell next() throws IOException;

  /**
   * Seek to immediately before the given {@link KeyValue}. If that exact {@link KeyValue} is
   * present in <tt>this</tt>, it will be returned by the next call to {@link #next()}. Otherwise,
   * returns the next {@link KeyValue} after the seeked {@link KeyValue}.
   * @param next {@link KeyValue} to seek to. Doesn't need to already be present in <tt>this</tt>
   * @return <tt>true</tt> if there are values left in <tt>this</tt>, <tt>false</tt> otherwise
   * @throws IOException if there is an error reading the underlying data.
   */
  public boolean seek(KeyValue next) throws IOException;

  /**
   * Read the {@link KeyValue} at the top of <tt>this</tt> without 'popping' it off the top of the
   * scanner.
   * @return the next {@link KeyValue} or <tt>null</tt> if there are no more values in <tt>this</tt>
   * @throws IOException if there is an error reading the underlying data.
   */
  public Cell peek() throws IOException;
}