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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;

import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.apache.phoenix.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * Fetches and parses the {@code FROM '<url>'} FileDescriptorSet for protobuf DDL. Only local
 * {@code file:///} URLs are accepted; they are read on the JDBC client and shipped inline.
 */
public final class ProtobufDescriptorFetcher {

  /**
   * Hard cap on the number of bytes the fetcher will read from a single URL — matches HBase's
   * default per-cell size limit ({@code hbase.client.keyvalue.maxsize}, 10 MiB). Any descriptor
   * larger than this won't fit in {@code SYSTEM.PROTO_DESCRIPTORS} so we reject early rather than
   * reading megabytes only to fail at the HBase write.
   */
  public static final int MAX_BYTES = 10 * 1024 * 1024;

  private static Path toLocalPath(String sourceUrl) {
    if (sourceUrl == null) {
      return null;
    }
    try {
      URI uri = new URI(sourceUrl);
      return "file".equalsIgnoreCase(uri.getScheme()) ? Paths.get(uri) : null;
    } catch (URISyntaxException | IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * Reads the local {@code file:///} URL and checks that the bytes parse as a
   * {@link FileDescriptorSet}, so a bad file fails on the client before any RPC is sent.
   * @param sourceUrl the URL from the DDL's {@code FROM '<url>'} clause, e.g.
   *                  {@code "file:///etc/phoenix/descriptors/order.desc"}
   * @return the raw bytes read from the file; owned by the caller
   * @throws SQLException with {@link SQLExceptionCode#PROTOBUF_DESCRIPTOR_URL_NOT_ALLOWED} if the
   *                      URL is not a local {@code file:///} URL; with
   *                      {@link SQLExceptionCode#PROTOBUF_DESCRIPTOR_FETCH_FAILED} if the bytes
   *                      can't be read or exceed the size cap; with
   *                      {@link SQLExceptionCode#PROTOBUF_DESCRIPTOR_PARSE_FAILED} if the bytes
   *                      don't parse as a valid {@code FileDescriptorSet}
   */
  public byte[] fetch(String sourceUrl) throws SQLException {
    Path path = toLocalPath(sourceUrl);
    if (path == null) {
      throw urlNotAllowed(sourceUrl);
    }
    byte[] bytes;
    try {
      bytes = readAllBytes(path, sourceUrl);
    } catch (IOException e) {
      throw fetchFailed(sourceUrl, e.getClass().getSimpleName() + ": " + e.getMessage(), e);
    }
    parseBytes(sourceUrl, bytes);
    return bytes;
  }

  /**
   * Builds the error raised for a {@code FROM} URL that is not a local {@code file:///} URL.
   */
  public static SQLException urlNotAllowed(String sourceUrl) {
    return new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_DESCRIPTOR_URL_NOT_ALLOWED)
      .setMessage(buildFetchMessage(sourceUrl, "only local file:/// URLs are allowed")).build()
      .buildException();
  }

  /**
   * Parses pre-fetched bytes as a {@link FileDescriptorSet}. Used by:
   * <ul>
   * <li>Server-side descriptor-DDL handlers to re-validate inline bytes shipped by a client that
   * fetched a local URL (defense-in-depth — never trust the client's claim that the bytes
   * parse).</li>
   * <li>Client-side {@code MetaDataClient} to pre-validate before shipping the RPC, so the user
   * gets a fast local failure for the common "uploaded the .proto instead of the .desc" /
   * "truncated download" mistakes.</li>
   * </ul>
   * @param sourceUrl the URL the bytes came from — used only for error-message attribution; pass
   *                  {@code "<inline>"} or similar if there's no URL
   * @param bytes     the byte payload (may be the result of a prior fetch, or read directly from an
   *                  RPC field)
   * @return the parsed descriptor set
   * @throws SQLException with {@link SQLExceptionCode#PROTOBUF_DESCRIPTOR_PARSE_FAILED} if the
   *                      bytes don't parse as a valid {@code FileDescriptorSet}; with
   *                      {@link SQLExceptionCode#PROTOBUF_DESCRIPTOR_FETCH_FAILED} if {@code bytes}
   *                      exceeds the configured size cap
   */
  public FileDescriptorSet parseBytes(String sourceUrl, byte[] bytes) throws SQLException {
    if (bytes == null) {
      throw fetchFailed(sourceUrl, "byte payload is null", null);
    }
    if (bytes.length > MAX_BYTES) {
      throw fetchFailed(sourceUrl,
        "byte payload (" + bytes.length + " bytes) exceeds the " + MAX_BYTES
          + "-byte cap. The protobuf descriptor must fit in a single HBase cell "
          + "(hbase.client.keyvalue.maxsize, default 10 MiB).",
        null);
    }
    try {
      return FileDescriptorSet.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw parseFailed(sourceUrl, bytes.length, e);
    }
  }

  /**
   * Reads the input stream into a byte array, capped at {@link #MAX_BYTES}. If the source is larger
   * than the cap, raises a fetch-failed exception identifying the cap rather than silently
   * truncating (truncated bytes would later fail at the parse step with a confusing
   * "InvalidProtocolBufferException: corrupt" message).
   */
  private byte[] readAllBytes(Path path, String sourceUrl) throws IOException, SQLException {
    try (InputStream in = Files.newInputStream(path)) {
      return readBoundedStream(in, sourceUrl);
    }
  }

  /**
   * Buffered read loop that enforces the {@link #MAX_BYTES} cap. Returns the bytes actually read,
   * or throws if the stream exceeds the cap.
   */
  private byte[] readBoundedStream(InputStream in, String sourceUrl)
    throws IOException, SQLException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int totalRead = 0;
    int n;
    while ((n = in.read(buf)) > 0) {
      totalRead += n;
      if (totalRead > MAX_BYTES) {
        throw fetchFailed(sourceUrl,
          "source exceeds the " + MAX_BYTES + "-byte cap; refusing to read further. "
            + "The protobuf descriptor must fit in a single HBase cell "
            + "(hbase.client.keyvalue.maxsize, default 10 MiB). Regenerate the .desc "
            + "from a smaller subset of .proto files, or split into multiple descriptors.",
          null);
      }
      baos.write(buf, 0, n);
    }
    return baos.toByteArray();
  }

  private static SQLException fetchFailed(String sourceUrl, String detail, Throwable cause) {
    SQLExceptionInfo.Builder b =
      new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_DESCRIPTOR_FETCH_FAILED)
        .setMessage(buildFetchMessage(sourceUrl, detail));
    if (cause != null) {
      b.setRootCause(cause);
    }
    return b.build().buildException();
  }

  private static SQLException parseFailed(String sourceUrl, int byteCount,
    InvalidProtocolBufferException cause) {
    return new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_DESCRIPTOR_PARSE_FAILED)
      .setMessage("URL: '" + sourceUrl + "' (" + byteCount
        + " bytes fetched); InvalidProtocolBufferException: " + cause.getMessage())
      .setRootCause(cause).build().buildException();
  }

  private static String buildFetchMessage(String sourceUrl, String detail) {
    StringBuilder sb = new StringBuilder("URL: '");
    if (sourceUrl != null) {
      sb.append(sourceUrl);
    } else {
      sb.append("<null>");
    }
    sb.append("'; ").append(detail);
    return sb.toString();
  }
}
