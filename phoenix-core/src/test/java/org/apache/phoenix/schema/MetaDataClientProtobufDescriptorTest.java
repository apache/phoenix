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
package org.apache.phoenix.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.AlterProtobufDescriptorRequest;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.CreateProtobufBundleRequest;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.CreateProtobufDescriptorRequest;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.DropProtobufDescriptorRequest;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.ProtobufDescriptorMutationResponse;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.ProtobufDescriptorService;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.AlterProtobufDescriptorStatement;
import org.apache.phoenix.parse.CreateProtobufBundleStatement;
import org.apache.phoenix.parse.CreateProtobufDescriptorStatement;
import org.apache.phoenix.parse.DropProtobufDescriptorStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.Test;

public class MetaDataClientProtobufDescriptorTest {
  private static final String BUNDLE_RESOURCE = "protobuf/acmewidgets/acmewidgets_v1.desc";
  private static final String DESCRIPTOR_NAME = "com.example.acmewidgets.account.v1.Account";

  private PhoenixConnection connection;
  private ConnectionQueryServices services;
  private Table table;
  private Configuration configuration;
  private MetaDataClient client;

  private AtomicReference<Message> capturedRequest;

  private ProtobufDescriptorMutationResponse cannedResponse;

  @Before
  public void setUp() throws Throwable {
    connection = mock(PhoenixConnection.class);
    services = mock(ConnectionQueryServices.class);
    table = mock(Table.class);
    configuration = new Configuration(false);
    capturedRequest = new AtomicReference<>();
    cannedResponse = null;

    when(connection.getQueryServices()).thenReturn(services);
    when(services.getProps()).thenReturn(ReadOnlyProps.EMPTY_PROPS);
    when(services.getConfiguration()).thenReturn(configuration);
    when(services.getTable(any(byte[].class))).thenReturn(table);

    doAnswer(invocation -> {
      @SuppressWarnings("unchecked")
      Batch.Call<ProtobufDescriptorService, ProtobufDescriptorMutationResponse> call =
        (Batch.Call<ProtobufDescriptorService,
          ProtobufDescriptorMutationResponse>) invocation.getArguments()[3];
      ProtobufDescriptorService stub = buildStubService();
      ProtobufDescriptorMutationResponse response = call.call(stub);
      return Collections.singletonMap(new byte[] { 0x01 }, response);
    }).when(table).coprocessorService(eq(ProtobufDescriptorService.class), any(byte[].class),
      any(byte[].class), any(Batch.Call.class));

    client = new MetaDataClient(connection);
  }

  @Test
  public void createDescriptorSuccessReturnsOneRowMutationState() throws Exception {
    cannedResponse = okResponse(DESCRIPTOR_NAME);
    MutationState state = client.createProtobufDescriptor(
      new CreateProtobufDescriptorStatement(DESCRIPTOR_NAME, false, fixtureFileUri()));

    assertEquals("real registration should report 1-row mutation count for executeUpdate()", 1,
      state.getMaxSize());
    CreateProtobufDescriptorRequest req = (CreateProtobufDescriptorRequest) capturedRequest.get();
    assertNotNull(req);
    assertEquals(DESCRIPTOR_NAME, req.getProtobufName());
    assertEquals(fixtureFileUri(), req.getSourceUrl());
    assertFalse(req.getIfNotExists());
    assertTrue("file:/// URL must ship inline bytes", req.hasInlineDescriptorBytes());
  }

  @Test
  public void createDescriptorWithIfNotExistsHitReturnsZeroRowMutationState() throws Exception {
    cannedResponse = okResponse();
    MutationState state = client.createProtobufDescriptor(
      new CreateProtobufDescriptorStatement(DESCRIPTOR_NAME, true, fixtureFileUri()));

    assertEquals("no-op IF NOT EXISTS hit should report 0-row mutation count", 0,
      state.getMaxSize());
    CreateProtobufDescriptorRequest req = (CreateProtobufDescriptorRequest) capturedRequest.get();
    assertTrue(req.getIfNotExists());
  }

  @Test
  public void createDescriptorWithClientLocalUrlShipsInlineBytes() throws Exception {
    cannedResponse = okResponse(DESCRIPTOR_NAME);
    String fileUrl = fixtureFileUri();

    client.createProtobufDescriptor(
      new CreateProtobufDescriptorStatement(DESCRIPTOR_NAME, false, fileUrl));

    CreateProtobufDescriptorRequest req = (CreateProtobufDescriptorRequest) capturedRequest.get();
    assertEquals(fileUrl, req.getSourceUrl());
    assertTrue("client-local URL must ship inline bytes", req.hasInlineDescriptorBytes());
    assertTrue("inline bytes must be non-empty for a real fixture",
      req.getInlineDescriptorBytes().size() > 0);

    byte[] expectedBytes = Files.readAllBytes(fixturePath());
    assertEquals(expectedBytes.length, req.getInlineDescriptorBytes().size());
  }

  @Test
  public void nonLocalUrlsAreRejectedBeforeAnyRpc() {
    for (String url : new String[] { "hdfs:///descriptors/dummy.desc", "s3a://bucket/x.desc",
      "abfs://container@account/x.desc", "/tmp/x.desc", "x.desc", "file://otherhost/tmp/x.desc" }) {
      assertUrlNotAllowed(() -> client.createProtobufDescriptor(
        new CreateProtobufDescriptorStatement(DESCRIPTOR_NAME, false, url)), url);
      assertUrlNotAllowed(() -> client
        .alterProtobufDescriptor(new AlterProtobufDescriptorStatement(DESCRIPTOR_NAME, false, url)),
        url);
      assertUrlNotAllowed(() -> client.createProtobufBundle(
        new CreateProtobufBundleStatement(Collections.singletonList(DESCRIPTOR_NAME), false, url)),
        url);
    }
  }

  @Test
  public void serverUrlNotAllowedStatusMapsToSqlException() {
    cannedResponse =
      statusResponse(SQLExceptionCode.PROTOBUF_DESCRIPTOR_URL_NOT_ALLOWED, DESCRIPTOR_NAME, null);
    SQLException ex = expectSQLException(() -> client.createProtobufDescriptor(
      new CreateProtobufDescriptorStatement(DESCRIPTOR_NAME, false, fixtureFileUri())));
    assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_URL_NOT_ALLOWED.getErrorCode(),
      ex.getErrorCode());
  }

  private void assertUrlNotAllowed(SqlThrowingRunnable r, String url) {
    capturedRequest.set(null);
    SQLException ex = expectSQLException(r);
    assertEquals("for URL: " + url,
      SQLExceptionCode.PROTOBUF_DESCRIPTOR_URL_NOT_ALLOWED.getErrorCode(), ex.getErrorCode());
    assertNull("no RPC may be sent for URL: " + url, capturedRequest.get());
  }

  @Test
  public void createDescriptorAlreadyExistsRaisesSQLException() {
    cannedResponse =
      statusResponse(SQLExceptionCode.PROTOBUF_DESCRIPTOR_ALREADY_EXISTS, DESCRIPTOR_NAME, null);
    SQLException ex = expectSQLException(() -> client.createProtobufDescriptor(
      new CreateProtobufDescriptorStatement(DESCRIPTOR_NAME, false, fixtureFileUri())));
    assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_ALREADY_EXISTS.getErrorCode(),
      ex.getErrorCode());
    assertTrue("message must reference the descriptor name: " + ex.getMessage(),
      ex.getMessage().contains(DESCRIPTOR_NAME));
  }

  @Test
  public void createDescriptorTypeNotFoundIncludesOffendingNameIfDistinct() {
    String requestName = DESCRIPTOR_NAME;
    String offending = "com.example.acmewidgets.account.v1.NotInTheBundle";
    cannedResponse = statusResponse(SQLExceptionCode.PROTOBUF_DESCRIPTOR_TYPE_NOT_FOUND_IN_FILE,
      offending, "No top-level protobuf message named '" + offending + "'");
    SQLException ex = expectSQLException(() -> client.createProtobufDescriptor(
      new CreateProtobufDescriptorStatement(requestName, false, fixtureFileUri())));
    assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_TYPE_NOT_FOUND_IN_FILE.getErrorCode(),
      ex.getErrorCode());
    assertTrue("message should carry the offending name distinct from the request name",
      ex.getMessage().contains(offending));
  }

  @Test
  public void createDescriptorFetchFailedSurfacesServerErrorMessage() {
    cannedResponse = statusResponse(SQLExceptionCode.PROTOBUF_DESCRIPTOR_FETCH_FAILED,
      DESCRIPTOR_NAME, "URL: 'file:///dummy/test.desc'; NoSuchFileException: /dummy/test.desc");
    SQLException ex = expectSQLException(() -> client.createProtobufDescriptor(
      new CreateProtobufDescriptorStatement(DESCRIPTOR_NAME, false, fixtureFileUri())));
    assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_FETCH_FAILED.getErrorCode(),
      ex.getErrorCode());
    assertTrue("FETCH_FAILED detail must propagate from the server's errorMessage",
      ex.getMessage().contains("NoSuchFileException"));
  }

  @Test
  public void unknownServerErrorCodeBecomesInternalError() {
    cannedResponse = ProtobufDescriptorMutationResponse.newBuilder().setErrorCode(-12345)
      .setOffendingName(DESCRIPTOR_NAME).build();
    SQLException ex = expectSQLException(() -> client.createProtobufDescriptor(
      new CreateProtobufDescriptorStatement(DESCRIPTOR_NAME, false, fixtureFileUri())));
    assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_INTERNAL_ERROR.getErrorCode(),
      ex.getErrorCode());
  }

  @Test
  public void createDescriptorInternalErrorDoesNotLeakUnderlyingDetail() {
    cannedResponse =
      statusResponse(SQLExceptionCode.PROTOBUF_DESCRIPTOR_INTERNAL_ERROR, DESCRIPTOR_NAME, null);
    SQLException ex = expectSQLException(() -> client.createProtobufDescriptor(
      new CreateProtobufDescriptorStatement(DESCRIPTOR_NAME, false, fixtureFileUri())));
    assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_INTERNAL_ERROR.getErrorCode(),
      ex.getErrorCode());

    assertTrue("INTERNAL_ERROR message should reference the descriptor name",
      ex.getMessage().contains(DESCRIPTOR_NAME));
  }

  @Test
  public void alterDescriptorBuildsCorrectRequest() throws Exception {
    cannedResponse = okResponse(DESCRIPTOR_NAME);
    client.alterProtobufDescriptor(
      new AlterProtobufDescriptorStatement(DESCRIPTOR_NAME, true, fixtureFileUri()));

    AlterProtobufDescriptorRequest req = (AlterProtobufDescriptorRequest) capturedRequest.get();
    assertEquals(DESCRIPTOR_NAME, req.getProtobufName());
    assertEquals(fixtureFileUri(), req.getSourceUrl());
    assertTrue(req.getIfExists());
    assertTrue(req.hasInlineDescriptorBytes());
  }

  @Test
  public void alterDescriptorIncompatibleEvolutionPropagatesServerDetail() {
    cannedResponse = statusResponse(SQLExceptionCode.PROTOBUF_TAG_RENAMED, DESCRIPTOR_NAME,
      "Field 'email' tag changed from 2 to 5 (PROTOBUF_TAG_RENAMED)");
    SQLException ex = expectSQLException(() -> client.alterProtobufDescriptor(
      new AlterProtobufDescriptorStatement(DESCRIPTOR_NAME, false, fixtureFileUri())));
    assertEquals(SQLExceptionCode.PROTOBUF_TAG_RENAMED.getErrorCode(), ex.getErrorCode());
    assertTrue("evolution-rule detail must propagate from the server",
      ex.getMessage().contains("PROTOBUF_TAG_RENAMED"));
  }

  @Test
  public void dropDescriptorBuildsCorrectRequest() throws Exception {
    cannedResponse = okResponse(DESCRIPTOR_NAME);
    client.dropProtobufDescriptor(new DropProtobufDescriptorStatement(DESCRIPTOR_NAME, false));

    DropProtobufDescriptorRequest req = (DropProtobufDescriptorRequest) capturedRequest.get();
    assertEquals(DESCRIPTOR_NAME, req.getProtobufName());
    assertFalse(req.getIfExists());

    for (java.lang.reflect.Method m : DropProtobufDescriptorRequest.class.getMethods()) {
      assertFalse("DropProtobufDescriptorRequest must not have inlineDescriptorBytes",
        m.getName().equals("hasInlineDescriptorBytes")
          || m.getName().equals("getInlineDescriptorBytes"));
    }
  }

  @Test
  public void dropDescriptorIfExistsHitReturnsZeroRowMutationState() throws Exception {
    cannedResponse = okResponse();
    MutationState state =
      client.dropProtobufDescriptor(new DropProtobufDescriptorStatement(DESCRIPTOR_NAME, true));
    assertEquals(0, state.getMaxSize());
  }

  @Test
  public void dropDescriptorRealDropReturnsOneRowMutationState() throws Exception {
    cannedResponse = okResponse(DESCRIPTOR_NAME);
    MutationState state =
      client.dropProtobufDescriptor(new DropProtobufDescriptorStatement(DESCRIPTOR_NAME, false));
    assertEquals(1, state.getMaxSize());
  }

  @Test
  public void createBundleBuildsCorrectRequest() throws Exception {
    String n1 = "com.example.acmewidgets.account.v1.Account";
    String n2 = "com.example.acmewidgets.account.v1.AccountSyncResponse";
    cannedResponse = okResponse(n1, n2);
    client.createProtobufBundle(
      new CreateProtobufBundleStatement(Arrays.asList(n1, n2), false, fixtureFileUri()));

    CreateProtobufBundleRequest req = (CreateProtobufBundleRequest) capturedRequest.get();
    assertEquals(2, req.getProtobufNamesCount());
    assertEquals(n1, req.getProtobufNames(0));
    assertEquals(n2, req.getProtobufNames(1));
    assertEquals(fixtureFileUri(), req.getSourceUrl());
    assertFalse(req.getIfNotExists());
    assertTrue(req.hasInlineDescriptorBytes());
  }

  @Test
  public void createBundleWithClientLocalUrlShipsInlineBytes() throws Exception {
    cannedResponse = okResponse(DESCRIPTOR_NAME);
    String fileUrl = fixtureFileUri();
    client.createProtobufBundle(new CreateProtobufBundleStatement(
      Collections.singletonList(DESCRIPTOR_NAME), false, fileUrl));

    CreateProtobufBundleRequest req = (CreateProtobufBundleRequest) capturedRequest.get();
    assertTrue(req.hasInlineDescriptorBytes());
    assertTrue(req.getInlineDescriptorBytes().size() > 0);
  }

  @Test
  public void createBundleAtomicRejectionRaisesSQLException() {
    cannedResponse = statusResponse(SQLExceptionCode.PROTOBUF_DESCRIPTOR_TYPE_NOT_FOUND_IN_FILE,
      "com.example.acmewidgets.account.v1.NotInTheBundle",
      "No top-level protobuf message named 'NotInTheBundle'");
    SQLException ex =
      expectSQLException(() -> client.createProtobufBundle(new CreateProtobufBundleStatement(
        Arrays.asList(DESCRIPTOR_NAME, "com.example.acmewidgets.account.v1.NotInTheBundle"), false,
        fixtureFileUri())));
    assertEquals(SQLExceptionCode.PROTOBUF_DESCRIPTOR_TYPE_NOT_FOUND_IN_FILE.getErrorCode(),
      ex.getErrorCode());
    assertTrue("message should name the offending entry that failed bundle resolution",
      ex.getMessage().contains("NotInTheBundle"));
  }

  @Test
  public void createBundleAllSkippedUnderIfNotExistsReturnsZeroRowMutationState() throws Exception {
    cannedResponse = okResponse();
    MutationState state = client.createProtobufBundle(new CreateProtobufBundleStatement(
      Arrays.asList(DESCRIPTOR_NAME, "com.example.acmewidgets.account.v1.AccountSyncResponse"),
      true, fixtureFileUri()));
    assertEquals(0, state.getMaxSize());
  }

  private static Path fixturePath() throws Exception {
    return Paths.get(MetaDataClientProtobufDescriptorTest.class.getClassLoader()
      .getResource(BUNDLE_RESOURCE).toURI());
  }

  private static String fixtureFileUri() {
    return MetaDataClientProtobufDescriptorTest.class.getClassLoader().getResource(BUNDLE_RESOURCE)
      .toString();
  }

  private static ProtobufDescriptorMutationResponse okResponse(String... registeredNames) {
    ProtobufDescriptorMutationResponse.Builder b = ProtobufDescriptorMutationResponse.newBuilder();
    for (String n : registeredNames) {
      b.addRegisteredNames(n);
    }
    return b.build();
  }

  private static ProtobufDescriptorMutationResponse statusResponse(SQLExceptionCode code,
    String offendingName, String errorMessage) {
    ProtobufDescriptorMutationResponse.Builder b = ProtobufDescriptorMutationResponse.newBuilder()
      .setErrorCode(code.getErrorCode()).setOffendingName(offendingName);
    if (errorMessage != null) {
      b.setErrorMessage(errorMessage);
    }
    return b.build();
  }

  private ProtobufDescriptorService buildStubService() {
    return new ProtobufDescriptorService() {
      @Override
      public void createProtobufDescriptor(RpcController controller,
        CreateProtobufDescriptorRequest request,
        RpcCallback<ProtobufDescriptorMutationResponse> done) {
        capturedRequest.set(request);
        done.run(cannedResponse);
      }

      @Override
      public void alterProtobufDescriptor(RpcController controller,
        AlterProtobufDescriptorRequest request,
        RpcCallback<ProtobufDescriptorMutationResponse> done) {
        capturedRequest.set(request);
        done.run(cannedResponse);
      }

      @Override
      public void dropProtobufDescriptor(RpcController controller,
        DropProtobufDescriptorRequest request,
        RpcCallback<ProtobufDescriptorMutationResponse> done) {
        capturedRequest.set(request);
        done.run(cannedResponse);
      }

      @Override
      public void createProtobufBundle(RpcController controller,
        CreateProtobufBundleRequest request, RpcCallback<ProtobufDescriptorMutationResponse> done) {
        capturedRequest.set(request);
        done.run(cannedResponse);
      }
    };
  }

  @FunctionalInterface
  private interface SqlThrowingRunnable {
    void run() throws SQLException;
  }

  private static SQLException expectSQLException(SqlThrowingRunnable r) {
    try {
      r.run();
      fail("Expected SQLException");
      return null;
    } catch (SQLException e) {
      return e;
    }
  }
}
