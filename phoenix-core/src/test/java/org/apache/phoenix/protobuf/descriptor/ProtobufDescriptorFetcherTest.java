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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.junit.Test;

public class ProtobufDescriptorFetcherTest {
  private static final String BUNDLE_RESOURCE = "protobuf/acmewidgets/acmewidgets_v1.desc";

  private static String fixtureFileUri() {
    URL res = ProtobufDescriptorFetcherTest.class.getClassLoader().getResource(BUNDLE_RESOURCE);
    assertNotNull(
      "acmewidgets_v1.desc not on test classpath — did mvn test-compile copy "
        + "src/test/resources/protobuf/acmewidgets/acmewidgets_v1.desc to target/test-classes?",
      res);
    return res.toString();
  }

  private static String fileUriOf(Path path) {
    return path.toUri().toString();
  }

  @Test
  public void nullSourceUrlFailsFast() {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    assertUrlNotAllowed(fetcher, null);
  }

  @Test
  public void emptySourceUrlFailsFast() {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    assertUrlNotAllowed(fetcher, "");
  }

  @Test
  public void nonExistentFileFailsWithFetchFailed() throws Exception {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    Path nope = Paths.get("/tmp", "phoenix-protobuf-nonexistent-" + System.nanoTime() + ".desc");
    String uri = fileUriOf(nope);
    SQLException ex = expectSQLException(() -> fetcher.fetch(uri));
    assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_FETCH_FAILED.getErrorCode(),
      ex.getErrorCode());

    assertTrue("expected URL in fetch-failed message: " + ex.getMessage(),
      ex.getMessage().contains(uri));
  }

  @Test
  public void oversizeSourceFailsWithFetchFailed() throws Exception {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    Path big = Files.createTempFile("phoenix-protobuf-oversize-", ".desc");
    try {
      Files.write(big, new byte[ProtobufDescriptorFetcher.MAX_BYTES + 1], StandardOpenOption.WRITE);
      SQLException ex = expectSQLException(() -> fetcher.fetch(fileUriOf(big)));
      assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_FETCH_FAILED.getErrorCode(),
        ex.getErrorCode());
      assertTrue("expected size-cap message: " + ex.getMessage(),
        ex.getMessage().contains(ProtobufDescriptorFetcher.MAX_BYTES + "-byte cap"));
    } finally {
      Files.deleteIfExists(big);
    }
  }

  @Test
  public void garbageBytesFailsWithParseFailed() throws Exception {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    Path garbage = writeTempBytes("phoenix-protobuf-garbage-",
      new byte[] { (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef, 0x42, 0x42 });
    try {
      SQLException ex = expectSQLException(() -> fetcher.fetch(fileUriOf(garbage)));
      assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_PARSE_FAILED.getErrorCode(),
        ex.getErrorCode());
      assertTrue("expected URL in parse-failed message: " + ex.getMessage(),
        ex.getMessage().contains(fileUriOf(garbage)));
    } finally {
      Files.deleteIfExists(garbage);
    }
  }

  @Test
  public void truncatedDescriptorFailsWithParseFailed() throws Exception {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    URL fixture = ProtobufDescriptorFetcherTest.class.getClassLoader().getResource(BUNDLE_RESOURCE);
    assertNotNull(fixture);
    byte[] full = Files.readAllBytes(Paths.get(fixture.toURI()));
    byte[] truncated = new byte[full.length / 2];
    System.arraycopy(full, 0, truncated, 0, truncated.length);

    Path tmp = writeTempBytes("phoenix-protobuf-truncated-", truncated);
    try {
      SQLException ex = expectSQLException(() -> fetcher.fetch(fileUriOf(tmp)));
      assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_PARSE_FAILED.getErrorCode(),
        ex.getErrorCode());
    } finally {
      Files.deleteIfExists(tmp);
    }
  }

  @Test
  public void allowsOnlyLocalFileUrls() throws Exception {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    Path fixture = Paths.get(new URL(fixtureFileUri()).toURI());
    byte[] expected = Files.readAllBytes(fixture);
    String path = fixture.toString();
    for (String url : new String[] { "file://" + path, "file:" + path, "FILE://" + path }) {
      assertArrayEquals(url, expected, fetcher.fetch(url));
    }
  }

  @Test
  public void rejectsSchemelessPaths() {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    assertUrlNotAllowed(fetcher, "foo.desc");
    assertUrlNotAllowed(fetcher, "/tmp/foo.desc");
    assertUrlNotAllowed(fetcher, "./descriptors/foo.desc");
  }

  @Test
  public void rejectsDistributedAndRemoteSchemes() {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    for (String url : new String[] { "hdfs:///descriptors/foo.desc",
      "hdfs://namenode:8020/foo.desc", "s3a://bucket/foo.desc", "s3://bucket/foo.desc",
      "gs://bucket/foo.desc", "abfs://container@account/foo.desc",
      "abfss://container@account/foo.desc", "wasb://c@a/foo.desc", "oss://bucket/foo.desc",
      "viewfs://cluster/foo.desc", "webhdfs://nn/foo.desc", "har:///foo.har/foo.desc",
      "http://example.com/foo.desc", "https://example.com/foo.desc",
      "jar:file:///tmp/foo.jar!/foo.desc" }) {
      assertUrlNotAllowed(fetcher, url);
    }
  }

  @Test
  public void rejectsFileUrlsWithHostQueryOrFragment() {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    assertUrlNotAllowed(fetcher, "file://tmp/foo.desc");
    assertUrlNotAllowed(fetcher, "file://otherhost/tmp/foo.desc");
    assertUrlNotAllowed(fetcher, "file://localhost/tmp/foo.desc");
    assertUrlNotAllowed(fetcher, "file://localhost:99/tmp/f.desc");
    assertUrlNotAllowed(fetcher, "file:///tmp/foo.desc?x=1");
    assertUrlNotAllowed(fetcher, "file:///tmp/foo.desc#frag");
    assertUrlNotAllowed(fetcher, "file:foo.desc");
  }

  @Test
  public void rejectsMalformedUrls() {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    assertUrlNotAllowed(fetcher, "file:///has space/foo.desc");
    assertUrlNotAllowed(fetcher, "file:///foo<bar.desc");
  }

  @Test
  public void parseBytesRejectsGarbageBytes() {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    byte[] garbage = new byte[] { (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef, 0x42, 0x42 };
    SQLException ex = expectSQLException(() -> fetcher.parseBytes("<inline>", garbage));
    assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_PARSE_FAILED.getErrorCode(),
      ex.getErrorCode());
  }

  @Test
  public void parseBytesRejectsNullPayload() {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    SQLException ex = expectSQLException(() -> fetcher.parseBytes("<inline>", null));
    assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_FETCH_FAILED.getErrorCode(),
      ex.getErrorCode());
  }

  @Test
  public void parseBytesEnforcesSizeCap() {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    SQLException ex = expectSQLException(
      () -> fetcher.parseBytes("<inline>", new byte[ProtobufDescriptorFetcher.MAX_BYTES + 1]));
    assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_FETCH_FAILED.getErrorCode(),
      ex.getErrorCode());
    assertTrue("expected size-cap message: " + ex.getMessage(),
      ex.getMessage().contains(ProtobufDescriptorFetcher.MAX_BYTES + "-byte cap"));
  }

  @Test
  public void parseBytesAcceptsEmptyPayloadAsEmptyDescriptorSet() throws Exception {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    assertEquals("empty input yields empty set", 0,
      fetcher.parseBytes("<inline>", new byte[0]).getFileCount());
  }

  @Test
  public void emptyFileParsesAsEmptyDescriptorSet() throws Exception {
    ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();
    Path empty = Files.createTempFile("phoenix-protobuf-empty-", ".desc");
    try {
      assertEquals("zero bytes fetched", 0, fetcher.fetch(fileUriOf(empty)).length);
    } finally {
      Files.deleteIfExists(empty);
    }
  }

  private static Path writeTempBytes(String prefix, byte[] bytes) throws IOException {
    Path tmp = Files.createTempFile(prefix, ".desc");
    Files.write(tmp, bytes, StandardOpenOption.WRITE);
    return tmp;
  }

  private void assertUrlNotAllowed(ProtobufDescriptorFetcher fetcher, String sourceUrl) {
    SQLException e = expectSQLException(() -> fetcher.fetch(sourceUrl));
    assertEquals("for URL: " + sourceUrl,
      SQLExceptionCode.PROTOBUF_DESCRIPTOR_URL_NOT_ALLOWED.getErrorCode(), e.getErrorCode());
  }

  @FunctionalInterface
  private interface SqlThrowingRunnable {
    void run() throws SQLException;
  }

  private SQLException expectSQLException(SqlThrowingRunnable r) {
    try {
      r.run();
      fail("Expected SQLException");
      return null;
    } catch (SQLException e) {
      return e;
    }
  }
}
