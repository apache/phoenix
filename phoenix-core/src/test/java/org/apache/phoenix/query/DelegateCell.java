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
package org.apache.phoenix.query;

import org.apache.hadoop.hbase.Cell;

public class DelegateCell implements Cell {
  private final Cell delegate;
  private final String name;

  public DelegateCell(Cell delegate, String name) {
    this.delegate = delegate;
    this.name = name;
  }

  @Override
  public int getValueOffset() {
    return delegate.getValueOffset();
  }

  @Override
  public int getValueLength() {
    return delegate.getValueLength();
  }

  @Override
  public byte[] getValueArray() {
    return delegate.getValueArray();
  }

  //Removed from Cell in Hbase 3
  public byte getTypeByte() {
      throw new UnsupportedOperationException();
  }

  @Override
  public long getTimestamp() {
    return delegate.getTimestamp();
  }

  //Removed from Cell in Hbase 3
  public int getTagsOffset() {
    throw new UnsupportedOperationException();
  }

  //Removed from Cell in Hbase 3
  public byte[] getTagsArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getRowOffset() {
    return delegate.getRowOffset();
  }

  @Override
  public short getRowLength() {
    return delegate.getRowLength();
  }

  @Override
  public byte[] getRowArray() {
    return delegate.getRowArray();
  }

  @Override
  public int getQualifierOffset() {
    return delegate.getQualifierOffset();
  }

  @Override
  public int getQualifierLength() {
    return delegate.getQualifierLength();
  }

  @Override
  public byte[] getQualifierArray() {
    return delegate.getQualifierArray();
  }

  @Override
  public int getFamilyOffset() {
    return delegate.getFamilyOffset();
  }

  @Override
  public byte getFamilyLength() {
    return delegate.getFamilyLength();
  }

  @Override
  public byte[] getFamilyArray() {
    return delegate.getFamilyArray();
  }

  @Override
  public String toString() {
    return name;
  }

//Removed from Cell in Hbase 3
  public long getSequenceId() {
      throw new UnsupportedOperationException();
  }

//Removed from Cell in Hbase 3
  public int getTagsLength() {
      throw new UnsupportedOperationException();
  }

  @Override
  public Type getType() {
    return delegate.getType();
  }

  @Override
  public long heapSize() {
    return delegate.heapSize();
  }

  @Override
  public int getSerializedSize() {
    return delegate.getSerializedSize();
  }
}
