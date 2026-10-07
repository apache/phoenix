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
package org.apache.phoenix.protobuf.descriptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.DescriptorValidationException;
import org.apache.phoenix.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * Resolves FileDescriptorSet bytes plus a fully-qualified message name into a
 * {@link Descriptors.Descriptor}. Topologically builds imported files, then finds the top-level
 * message. Stateless and thread-safe.
 */
public final class ProtoDescriptorResolver {

  /**
   * Resolves the given raw {@link FileDescriptorSet} bytes against
   * {@code fullyQualifiedMessageName}, returning the matching {@link Descriptors.Descriptor}
   * (top-level or nested; its containing file is available via
   * {@link Descriptors.Descriptor#getFile()}) or {@code null} if the name does not correspond to
   * any message in the set.
   * @throws IOException if the bytes don't parse as a valid {@code FileDescriptorSet}
   *                     ({@link InvalidProtocolBufferException}) or if the descriptor set's
   *                     internal cross-file references are inconsistent
   *                     ({@link DescriptorValidationException} — missing imports, type-name
   *                     collisions, etc.)
   */
  public Descriptors.Descriptor resolve(byte[] fileDescriptorSetBytes,
    String fullyQualifiedMessageName) throws IOException {
    if (fileDescriptorSetBytes == null) {
      throw new IllegalArgumentException("fileDescriptorSetBytes must not be null");
    }
    if (fullyQualifiedMessageName == null || fullyQualifiedMessageName.isEmpty()) {
      throw new IllegalArgumentException("fullyQualifiedMessageName must not be null or empty");
    }

    FileDescriptorSet set;
    try {
      set = FileDescriptorSet.parseFrom(fileDescriptorSetBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IOException("FileDescriptorSet bytes do not parse (expected "
        + fileDescriptorSetBytes.length + " bytes of valid google.protobuf.FileDescriptorSet)", e);
    }

    Map<String, Descriptors.FileDescriptor> resolvedFiles = resolveAllFiles(set);

    for (Descriptors.FileDescriptor file : resolvedFiles.values()) {
      for (Descriptors.Descriptor topLevel : file.getMessageTypes()) {
        Descriptors.Descriptor matched = findMessageByFullName(topLevel, fullyQualifiedMessageName);
        if (matched != null) {
          return matched;
        }
      }
    }
    // Name not in this set — caller (registry) maps this to a not-found result.
    return null;
  }

  /**
   * Depth-first walk of a message + its nested types looking for one whose
   * {@link Descriptors.Descriptor#getFullName()} equals {@code fullyQualifiedMessageName}. Returns
   * {@code null} if not found in this subtree.
   * <p>
   * Short-circuits on the first match. Bounded by the file's static nested-type depth (no cycles
   * possible — protobuf nested types form a tree by construction).
   */
  private static Descriptors.Descriptor findMessageByFullName(Descriptors.Descriptor message,
    String fullyQualifiedMessageName) {
    if (fullyQualifiedMessageName.equals(message.getFullName())) {
      return message;
    }
    // Fast prefix check: a nested type's FQ name starts with the outer's FQ name +
    // '.'; if the target isn't under this subtree, skip recursion entirely.
    String outerPrefix = message.getFullName() + ".";
    if (!fullyQualifiedMessageName.startsWith(outerPrefix)) {
      return null;
    }
    for (Descriptors.Descriptor nested : message.getNestedTypes()) {
      Descriptors.Descriptor found = findMessageByFullName(nested, fullyQualifiedMessageName);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /**
   * Walks the {@link FileDescriptorSet}'s files in dependency order and builds each one via
   * {@code FileDescriptor.buildFrom}. Returns the resulting map keyed by file name so the caller
   * can look up dependents.
   * <p>
   * The order matters: {@code FileDescriptor.buildFrom(proto, deps[])} requires the dependencies
   * passed in {@code deps} to already be built. We use a depth-first traversal with cycle-detection
   * (proto schemas shouldn't have import cycles, but we guard against malformed input).
   */
  private static Map<String, Descriptors.FileDescriptor> resolveAllFiles(FileDescriptorSet set)
    throws IOException {
    Map<String, FileDescriptorProto> protosByName = new HashMap<>();
    for (FileDescriptorProto proto : set.getFileList()) {
      protosByName.put(proto.getName(), proto);
    }
    Map<String, Descriptors.FileDescriptor> resolved = new HashMap<>();
    Set<String> resolving = new LinkedHashSet<>(); // for cycle detection
    for (FileDescriptorProto proto : set.getFileList()) {
      buildFileRecursive(proto, protosByName, resolved, resolving);
    }
    return resolved;
  }

  private static Descriptors.FileDescriptor buildFileRecursive(FileDescriptorProto proto,
    Map<String, FileDescriptorProto> protosByName, Map<String, Descriptors.FileDescriptor> resolved,
    Set<String> resolving) throws IOException {
    String name = proto.getName();
    Descriptors.FileDescriptor existing = resolved.get(name);
    if (existing != null) {
      return existing;
    }
    if (!resolving.add(name)) {
      // Already on the resolution stack — proto import cycle.
      throw new IOException(
        "Import cycle detected involving '" + name + "' (resolution stack: " + resolving + ")");
    }
    try {
      Descriptors.FileDescriptor[] deps =
        new Descriptors.FileDescriptor[proto.getDependencyCount()];
      for (int i = 0; i < proto.getDependencyCount(); i++) {
        String depName = proto.getDependency(i);
        FileDescriptorProto depProto = protosByName.get(depName);
        if (depProto == null) {
          throw new IOException("FileDescriptorSet for '" + name + "' references '" + depName
            + "' but that file is not included in the set. Regenerate the "
            + ".desc with `protoc --include_imports` to inline transitive dependencies.");
        }
        deps[i] = buildFileRecursive(depProto, protosByName, resolved, resolving);
      }
      try {
        Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(proto, deps);
        resolved.put(name, fd);
        return fd;
      } catch (DescriptorValidationException e) {
        throw new IOException(
          "FileDescriptor validation failed for '" + name + "': " + e.getMessage(), e);
      }
    } finally {
      resolving.remove(name);
    }
  }
}
