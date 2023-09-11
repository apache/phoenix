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
package org.apache.phoenix.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

public final class PhoenixIndexBuilderHelper {
    private static final byte[] ON_DUP_KEY_IGNORE_BYTES = new byte[] {1}; // boolean true
    private static final int ON_DUP_KEY_HEADER_BYTE_SIZE = Bytes.SIZEOF_SHORT + Bytes.SIZEOF_BOOLEAN;
    public static final String ATOMIC_OP_ATTRIB = "_ATOMIC_OP_ATTRIB";
    public static byte[] serializeOnDupKeyIgnore() {
        return ON_DUP_KEY_IGNORE_BYTES;
    }

    /**
     * Serialize ON DUPLICATE KEY UPDATE info with the following format:
     * 1) Boolean value tracking whether or not to execute the first ON DUPLICATE KEY clause.
     *    We know the clause should be executed when there are other UPSERT VALUES clauses earlier in
     *    the same batch for this row key. We need this for two main cases:
     *       UPSERT VALUES followed by UPSERT VALUES ON DUPLICATE KEY UPDATE
     *       UPSERT VALUES ON DUPLICATE KEY IGNORE followed by UPSERT VALUES ON DUPLICATE KEY UPDATE
     * 2) Short value tracking how many times the next first clause should be executed. This
     *    optimizes the same clause be executed many times by only serializing it once.
     * 3) Repeating {@code List<Expression>, PTable } pairs that encapsulate the ON DUPLICATE KEY clause.
     * @param table table representing columns being updated
     * @param expressions list of expressions to evaluate for updating columns
     * @return serialized byte array representation of ON DUPLICATE KEY UPDATE info
     */
    public static byte[] serializeOnDupKeyUpdate(PTable table, List<Expression> expressions) {
        PTableProtos.PTable ptableProto = PTableImpl.toProto(table);
        int size = ptableProto.getSerializedSize();
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream(size * 2)) {
            DataOutputStream output = new DataOutputStream(stream);
            output.writeBoolean(true); // Skip this ON DUPLICATE KEY clause if row already exists
            output.writeShort(1); // Execute this ON DUPLICATE KEY once
            WritableUtils.writeVInt(output, expressions.size());
            for (int i = 0; i < expressions.size(); i++) {
                Expression expression = expressions.get(i);
                WritableUtils.writeVInt(output, ExpressionType.valueOf(expression).ordinal());
                expression.write(output);
            }
            ptableProto.writeDelimitedTo(output);
            return stream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] doNotSkipFirstOnDupKey(byte[] oldOnDupKeyBytes) {
        byte[] newOnDupKeyBytes = Arrays.copyOf(oldOnDupKeyBytes, oldOnDupKeyBytes.length);
        newOnDupKeyBytes[0] = 0; // false means do not skip first ON DUPLICATE KEY
        return newOnDupKeyBytes;
    }

    public static byte[] combineOnDupKey(byte[] oldOnDupKeyBytes, byte[] newOnDupKeyBytes) {
        // If old ON DUPLICATE KEY is null, then the new value always takes effect
        // If new ON DUPLICATE KEY is null, then reset back to null
        if (oldOnDupKeyBytes == null || newOnDupKeyBytes == null) {
            if (newOnDupKeyBytes == null) {
                return newOnDupKeyBytes;
            }
            return doNotSkipFirstOnDupKey(newOnDupKeyBytes);
        }
        // If the new UPSERT VALUES statement has an ON DUPLICATE KEY IGNORE, and there
        // is an already existing UPSERT VALUES statement with an ON DUPLICATE KEY clause,
        // then we can just keep that one as the new one has no impact.
        if (isDupKeyIgnore(newOnDupKeyBytes)) {
            return oldOnDupKeyBytes;
        }
        boolean isOldDupKeyIgnore = isDupKeyIgnore(oldOnDupKeyBytes);
        try (TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(Math.max(0, oldOnDupKeyBytes.length - ON_DUP_KEY_HEADER_BYTE_SIZE) + newOnDupKeyBytes.length);
             ByteArrayInputStream oldStream = new ByteArrayInputStream(oldOnDupKeyBytes);
             ByteArrayInputStream newStream = new ByteArrayInputStream(newOnDupKeyBytes);
             DataOutputStream output = new DataOutputStream(stream);
             DataInputStream oldInput = new DataInputStream(oldStream);
             DataInputStream newInput = new DataInputStream(newStream)) {

            boolean execute1 = oldInput.readBoolean();
            newInput.readBoolean(); // ignore
            int repeating2 = newInput.readShort();
            if (isOldDupKeyIgnore) {
                output.writeBoolean(false); // Will force subsequent ON DUPLICATE KEY UPDATE statement to execute
                output.writeShort(repeating2);
                output.write(newOnDupKeyBytes, ON_DUP_KEY_HEADER_BYTE_SIZE, newOnDupKeyBytes.length - ON_DUP_KEY_HEADER_BYTE_SIZE);
            } else {
                int repeating1 = oldInput.readShort();
                if (Bytes.compareTo(
                        oldOnDupKeyBytes, ON_DUP_KEY_HEADER_BYTE_SIZE, oldOnDupKeyBytes.length - ON_DUP_KEY_HEADER_BYTE_SIZE,
                        newOnDupKeyBytes, Bytes.SIZEOF_SHORT + Bytes.SIZEOF_BOOLEAN, newOnDupKeyBytes.length - ON_DUP_KEY_HEADER_BYTE_SIZE) == 0) {
                    // If both old and new ON DUPLICATE KEY UPDATE clauses match,
                    // reduce the size of data we're sending over the wire.
                    // TODO: optimization size of RPC more.
                    output.writeBoolean(execute1);
                    output.writeShort(repeating1 + repeating2);
                    output.write(newOnDupKeyBytes, ON_DUP_KEY_HEADER_BYTE_SIZE, newOnDupKeyBytes.length - ON_DUP_KEY_HEADER_BYTE_SIZE);
                } else {
                    output.writeBoolean(execute1);
                    output.writeShort(repeating1); // retain first ON DUPLICATE KEY UPDATE having repeated
                    output.write(oldOnDupKeyBytes, ON_DUP_KEY_HEADER_BYTE_SIZE, oldOnDupKeyBytes.length - ON_DUP_KEY_HEADER_BYTE_SIZE);
                    // If the new ON DUPLICATE KEY UPDATE was repeating, we need to write it multiple times as only the first
                    // statement is effected by the repeating amount
                    for (int i = 0; i < repeating2; i++) {
                        output.write(newOnDupKeyBytes, ON_DUP_KEY_HEADER_BYTE_SIZE, newOnDupKeyBytes.length - ON_DUP_KEY_HEADER_BYTE_SIZE);
                    }
                }
            }
            return stream.toByteArray();
        } catch (IOException e) { // Shouldn't be possible with ByteInput/Output streams
            throw new RuntimeException(e);
        }
    }

    public static boolean isDupKeyIgnore(byte[] onDupKeyBytes) {
        return onDupKeyBytes != null && Bytes.compareTo(ON_DUP_KEY_IGNORE_BYTES, onDupKeyBytes) == 0;
    }
}
