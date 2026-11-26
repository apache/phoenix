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
package org.apache.phoenix.protobuf.text;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import java.lang.reflect.Method;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.AlterProtobufDescriptorRequest;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.CreateProtobufBundleRequest;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.CreateProtobufDescriptorRequest;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.DropProtobufDescriptorRequest;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.ProtobufDescriptorMutationResponse;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.ProtobufDescriptorService;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.junit.Test;

public class ProtobufDescriptorServiceWireTest {
  @Test
  public void createDescriptorRequestRoundTrips() throws Exception {
    CreateProtobufDescriptorRequest original = CreateProtobufDescriptorRequest.newBuilder()
      .setProtobufName("com.example.acmewidgets.account.v1.Account")
      .setSourceUrl("file:///phoenix/descriptors/acmewidgets_v1.desc").setIfNotExists(true).build();

    byte[] wire = original.toByteArray();
    CreateProtobufDescriptorRequest decoded = CreateProtobufDescriptorRequest.parseFrom(wire);

    assertEquals(original, decoded);
    assertEquals("com.example.acmewidgets.account.v1.Account", decoded.getProtobufName());
    assertEquals("file:///phoenix/descriptors/acmewidgets_v1.desc", decoded.getSourceUrl());
    assertTrue(decoded.getIfNotExists());
  }

  @Test
  public void alterDescriptorRequestRoundTrips() throws Exception {
    AlterProtobufDescriptorRequest original = AlterProtobufDescriptorRequest.newBuilder()
      .setProtobufName("com.example.acmewidgets.account.v1.Account")
      .setSourceUrl("file:///etc/phoenix/order_v2.desc").setIfExists(false).build();

    AlterProtobufDescriptorRequest decoded =
      AlterProtobufDescriptorRequest.parseFrom(original.toByteArray());
    assertEquals(original, decoded);
    assertFalse(decoded.getIfExists());
  }

  @Test
  public void dropDescriptorRequestRoundTrips() throws Exception {
    DropProtobufDescriptorRequest original = DropProtobufDescriptorRequest.newBuilder()
      .setProtobufName("com.example.acmewidgets.delivery.v1.DeliveryWindowRecord").setIfExists(true)
      .build();

    DropProtobufDescriptorRequest decoded =
      DropProtobufDescriptorRequest.parseFrom(original.toByteArray());
    assertEquals(original, decoded);
  }

  @Test
  public void createBundleRequestRoundTrips() throws Exception {
    CreateProtobufBundleRequest original = CreateProtobufBundleRequest.newBuilder()
      .setSourceUrl("file:///phoenix/descriptors/acmewidgets_v1.desc").setIfNotExists(true)
      .addProtobufNames("com.example.acmewidgets.account.v1.Account")
      .addProtobufNames("com.example.acmewidgets.account.v1.AccountSyncResponse")
      .addProtobufNames("com.example.acmewidgets.common.v1.CatalogSyncHeader").build();

    CreateProtobufBundleRequest decoded =
      CreateProtobufBundleRequest.parseFrom(original.toByteArray());
    assertEquals(original, decoded);
    assertEquals(3, decoded.getProtobufNamesCount());
    assertEquals("com.example.acmewidgets.common.v1.CatalogSyncHeader",
      decoded.getProtobufNames(2));
  }

  @Test
  public void mutationResponseRoundTripsOnSuccess() throws Exception {
    ProtobufDescriptorMutationResponse original = ProtobufDescriptorMutationResponse.newBuilder()
      .addRegisteredNames("com.example.acmewidgets.account.v1.Account").build();

    ProtobufDescriptorMutationResponse decoded =
      ProtobufDescriptorMutationResponse.parseFrom(original.toByteArray());
    assertEquals(original, decoded);
    assertFalse(decoded.hasErrorCode());
  }

  @Test
  public void mutationResponseCarriesOffendingNameOnTypeNotFound() throws Exception {
    ProtobufDescriptorMutationResponse original = ProtobufDescriptorMutationResponse.newBuilder()
      .setErrorCode(SQLExceptionCode.PROTOBUF_DESCRIPTOR_TYPE_NOT_FOUND_IN_FILE.getErrorCode())
      .setOffendingName("com.example.acmewidgets.account.v1.NotInTheBundle").setErrorMessage(
        "No top-level message named 'NotInTheBundle' in the loaded FileDescriptorSet")
      .build();

    ProtobufDescriptorMutationResponse decoded =
      ProtobufDescriptorMutationResponse.parseFrom(original.toByteArray());
    assertEquals(original, decoded);
    assertEquals("com.example.acmewidgets.account.v1.NotInTheBundle", decoded.getOffendingName());
  }

  @Test
  public void mutationResponseCarriesErrorMessageOnFetchFailed() throws Exception {
    ProtobufDescriptorMutationResponse original = ProtobufDescriptorMutationResponse.newBuilder()
      .setErrorCode(SQLExceptionCode.PROTOBUF_DESCRIPTOR_FETCH_FAILED.getErrorCode())
      .setErrorMessage("java.nio.file.NoSuchFileException: /nonexistent/path.desc").build();

    ProtobufDescriptorMutationResponse decoded =
      ProtobufDescriptorMutationResponse.parseFrom(original.toByteArray());
    assertEquals(original, decoded);
    assertTrue(decoded.getErrorMessage().contains("NoSuchFileException"));
  }

  @Test
  public void createDescriptorRequestRoundTripsInlineBytes() throws Exception {
    byte[] payload = { 0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f };
    CreateProtobufDescriptorRequest original = CreateProtobufDescriptorRequest.newBuilder()
      .setProtobufName("com.example.acmewidgets.account.v1.Account")
      .setSourceUrl("file:///home/alice/account.desc").setIfNotExists(false)
      .setInlineDescriptorBytes(ByteString.copyFrom(payload)).build();

    CreateProtobufDescriptorRequest decoded =
      CreateProtobufDescriptorRequest.parseFrom(original.toByteArray());
    assertEquals(original, decoded);
    assertTrue("inlineDescriptorBytes must be set after round-trip",
      decoded.hasInlineDescriptorBytes());
    assertArrayEquals(payload, decoded.getInlineDescriptorBytes().toByteArray());
  }

  @Test
  public void alterDescriptorRequestRoundTripsInlineBytes() throws Exception {
    byte[] payload = { 0x12, 0x03, 0x66, 0x6f, 0x6f };
    AlterProtobufDescriptorRequest original = AlterProtobufDescriptorRequest.newBuilder()
      .setProtobufName("com.example.acmewidgets.account.v1.Account")
      .setSourceUrl("file:///home/alice/account_v2.desc").setIfExists(true)
      .setInlineDescriptorBytes(ByteString.copyFrom(payload)).build();

    AlterProtobufDescriptorRequest decoded =
      AlterProtobufDescriptorRequest.parseFrom(original.toByteArray());
    assertEquals(original, decoded);
    assertTrue(decoded.hasInlineDescriptorBytes());
    assertArrayEquals(payload, decoded.getInlineDescriptorBytes().toByteArray());
  }

  @Test
  public void createBundleRequestRoundTripsInlineBytes() throws Exception {
    byte[] payload = new byte[256];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (i & 0x7f);
    }
    CreateProtobufBundleRequest original = CreateProtobufBundleRequest.newBuilder()
      .setSourceUrl("file:///home/alice/acmewidgets_v1.desc").setIfNotExists(true)
      .addProtobufNames("com.example.acmewidgets.account.v1.Account")
      .addProtobufNames("com.example.acmewidgets.common.v1.CatalogSyncHeader")
      .setInlineDescriptorBytes(ByteString.copyFrom(payload)).build();

    CreateProtobufBundleRequest decoded =
      CreateProtobufBundleRequest.parseFrom(original.toByteArray());
    assertEquals(original, decoded);
    assertTrue(decoded.hasInlineDescriptorBytes());
    assertEquals(payload.length, decoded.getInlineDescriptorBytes().size());
    assertArrayEquals(payload, decoded.getInlineDescriptorBytes().toByteArray());
  }

  @Test
  public void mutationRequestsWithoutInlineBytesOmitTheField() throws Exception {
    CreateProtobufDescriptorRequest noInline =
      CreateProtobufDescriptorRequest.newBuilder().setProtobufName("my.org.X")
        .setSourceUrl("file:///descriptors/x.desc").setIfNotExists(false).build();
    assertFalse("request built without inline bytes must not set the field",
      noInline.hasInlineDescriptorBytes());
    CreateProtobufDescriptorRequest decoded =
      CreateProtobufDescriptorRequest.parseFrom(noInline.toByteArray());
    assertFalse(decoded.hasInlineDescriptorBytes());
  }

  @Test
  public void dropRequestHasNoInlineDescriptorBytesField() {
    boolean hasField = false;
    for (Method m : DropProtobufDescriptorRequest.class.getMethods()) {
      if (
        m.getName().equals("hasInlineDescriptorBytes")
          || m.getName().equals("getInlineDescriptorBytes")
      ) {
        hasField = true;
        break;
      }
    }
    assertFalse("DropProtobufDescriptorRequest must not declare inlineDescriptorBytes", hasField);
  }

  @Test
  public void wireBytesAreDeterministic() throws Exception {
    CreateProtobufDescriptorRequest a = CreateProtobufDescriptorRequest.newBuilder()
      .setProtobufName("my.org.X").setSourceUrl("file:///x.desc").setIfNotExists(false).build();
    CreateProtobufDescriptorRequest b = CreateProtobufDescriptorRequest.newBuilder()
      .setProtobufName("my.org.X").setSourceUrl("file:///x.desc").setIfNotExists(false).build();
    assertArrayEquals(a.toByteArray(), b.toByteArray());
  }

  @Test
  public void serviceDeclaresExpectedRpcs() {
    String[] expected = { "createProtobufDescriptor", "alterProtobufDescriptor",
      "dropProtobufDescriptor", "createProtobufBundle" };
    for (String rpcName : expected) {
      assertTrue(
        "expected RPC method '" + rpcName + "' missing from " + "ProtobufDescriptorService",
        hasRpcMethod(rpcName));
    }
  }

  @Test
  public void servicePackageAndNameMatchSpec() {
    assertEquals("org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos$"
      + "ProtobufDescriptorService", ProtobufDescriptorService.class.getName());
  }

  private static boolean hasRpcMethod(String name) {
    for (Method m : ProtobufDescriptorService.class.getDeclaredMethods()) {
      if (m.getName().equals(name)) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void serviceCanBeSubclassed() {
    ProtobufDescriptorService stub = new ProtobufDescriptorService() {
      @Override
      public void createProtobufDescriptor(RpcController c, CreateProtobufDescriptorRequest r,
        RpcCallback<ProtobufDescriptorMutationResponse> done) {
        done.run(
          ProtobufDescriptorMutationResponse.newBuilder().addRegisteredNames("my.org.X").build());
      }

      @Override
      public void alterProtobufDescriptor(RpcController c, AlterProtobufDescriptorRequest r,
        RpcCallback<ProtobufDescriptorMutationResponse> done) {
        fail("not exercised in this test");
      }

      @Override
      public void dropProtobufDescriptor(RpcController c, DropProtobufDescriptorRequest r,
        RpcCallback<ProtobufDescriptorMutationResponse> done) {
        fail("not exercised in this test");
      }

      @Override
      public void createProtobufBundle(RpcController c, CreateProtobufBundleRequest r,
        RpcCallback<ProtobufDescriptorMutationResponse> done) {
        fail("not exercised in this test");
      }
    };
    assertNotNull(stub);

    ProtobufDescriptorMutationResponse[] sink = new ProtobufDescriptorMutationResponse[1];
    stub.createProtobufDescriptor(null,
      CreateProtobufDescriptorRequest.newBuilder().setProtobufName("my.org.X")
        .setSourceUrl("file:///x.desc").setIfNotExists(false).build(),
      response -> sink[0] = response);
    assertNotNull(sink[0]);
    assertFalse(sink[0].hasErrorCode());
  }
}
