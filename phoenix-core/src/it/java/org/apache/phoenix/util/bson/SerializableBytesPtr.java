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

package org.apache.phoenix.util.bson;

import java.io.Serializable;

import org.apache.hadoop.hbase.util.Bytes;

public class SerializableBytesPtr
    implements Serializable, Comparable<SerializableBytesPtr> {

  private byte[] b;

  public SerializableBytesPtr() {
  }

  public SerializableBytesPtr(byte[] b) {
    this.b = b;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SerializableBytesPtr that = (SerializableBytesPtr) o;
    return Bytes.equals(b, that.b);
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(b);
  }

  @Override
  public int compareTo(SerializableBytesPtr o) {
    return Bytes.compareTo(this.b, o.b);
  }

  public void setB(byte[] b) {
    this.b = b;
  }

  public byte[] getB() {
    return b;
  }
}
