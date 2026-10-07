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
package org.apache.phoenix.hbase.index.hnsw;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;
import org.junit.Test;

/**
 * Unit tests verifying direct buffer serialization, dynamic capacity expansion, and fidelity
 * against DataOutputStream.
 */
public class HnswBufferWriterTest {

  // Exercises all DataOutput primitives, string formats, and bulk float/byte arrays triggering
  // buffer expansion
  private static void writeAll(DataOutput out, boolean floatsAsRun) throws IOException {
    Random random = new Random(3);
    out.write(7);
    out.writeBoolean(true);
    out.writeByte(-2);
    out.writeShort(-30000);
    out.writeChar('é');
    out.writeInt(0x12345678);
    out.writeLong(-1234567890123L);
    out.writeFloat(1.5f);
    out.writeDouble(-2.25);
    out.writeBytes("ascii");
    out.writeChars("chars");
    out.writeUTF("utf é中");
    float[] floats = new float[1000];
    for (int i = 0; i < floats.length; i++) {
      floats[i] = random.nextFloat();
    }
    if (floatsAsRun) {
      ((HnswBufferWriter) out).writeFloats(floats, 10, 900);
    } else {
      for (int i = 10; i < 910; i++) {
        out.writeFloat(floats[i]);
      }
    }
    byte[] bytes = new byte[5000];
    random.nextBytes(bytes);
    out.write(bytes, 100, 4000);
    out.write(bytes);
  }

  @Test
  public void testMatchesDataOutputStream() throws IOException {
    ByteArrayOutputStream expected = new ByteArrayOutputStream();
    writeAll(new DataOutputStream(expected), false);
    HnswBufferWriter writer = new HnswBufferWriter(16);
    writeAll(writer, true);
    assertArrayEquals(expected.toByteArray(), writer.toByteArray());
    assertEquals(expected.size(), writer.position());
    assertTrue("writer should have grown", writer.capacity() >= expected.size());
  }

  @Test
  public void testGrowthPreservesBytes() throws IOException {
    HnswBufferWriter small = new HnswBufferWriter(1);
    HnswBufferWriter large = new HnswBufferWriter(1 << 20);
    writeAll(small, true);
    writeAll(large, true);
    assertArrayEquals(large.toByteArray(), small.toByteArray());
    assertEquals(1 << 20, large.capacity());
  }
}
