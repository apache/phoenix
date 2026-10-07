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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.protobuf.ByteString;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.DescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.Descriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.DynamicMessage;

public class AcmeWidgetsFixturesTest {

  private static final int EXPECTED_FILE_COUNT = 11;

  private static final int EXPECTED_MESSAGE_COUNT_AT_LEAST = 37;

  private static final Set<String> EXPECTED_FILE_NAMES = new HashSet<>(
    Arrays.asList("widget_common.proto", "account_sync.proto", "widget_endpoint_types.proto",
      "widget_governance.proto", "delivery_window_sync.proto", "storefront_sync.proto",
      "api_credential_sync.proto", "entitlement_sync.proto", "voucher_revocation_sync.proto",
      "widget_order_graph.proto", "google/protobuf/timestamp.proto"));

  private static FileDescriptorSet loadBundle() throws Exception {
    FileDescriptorSet set = AcmeWidgetsFixtures.loadBundle();
    assertNotNull("bundle must parse", set);
    return set;
  }

  private static Map<String, FileDescriptor> buildAllFileDescriptors(FileDescriptorSet set)
    throws Exception {
    return AcmeWidgetsFixtures.buildAllFileDescriptors(set);
  }

  @Test
  public void bundleParsesAsValidFileDescriptorSet() throws Exception {
    FileDescriptorSet set = loadBundle();
    assertNotNull(set);
    assertTrue("bundle must contain at least one file", set.getFileCount() > 0);
  }

  @Test
  public void bundleContainsExpectedFileCount() throws Exception {
    FileDescriptorSet set = loadBundle();
    assertEquals(
      "Expected " + EXPECTED_FILE_COUNT + " files (9 acmewidgets + 1 bundled WKT). "
        + "If this changed, regenerate per the fixtures README and update EXPECTED_FILE_COUNT.",
      EXPECTED_FILE_COUNT, set.getFileCount());
  }

  @Test
  public void bundleContainsExpectedFileNames() throws Exception {
    FileDescriptorSet set = loadBundle();
    Set<String> actualNames = new HashSet<>();
    for (FileDescriptorProto file : set.getFileList()) {
      actualNames.add(file.getName());
    }
    for (String expected : EXPECTED_FILE_NAMES) {
      assertTrue("Bundle missing expected file: " + expected + ". Actual contents: " + actualNames,
        actualNames.contains(expected));
    }
  }

  @Test
  public void bundleMixesProto3AndProto2Syntaxes() throws Exception {
    FileDescriptorSet set = loadBundle();
    int proto3Count = 0;
    int proto2Count = 0;
    for (FileDescriptorProto file : set.getFileList()) {
      String syntax = file.hasSyntax() ? file.getSyntax() : "proto2";
      if ("proto3".equals(syntax)) {
        proto3Count++;
      } else if ("proto2".equals(syntax)) {
        proto2Count++;
      }
    }
    assertTrue("expected at least 1 proto3 file, got " + proto3Count, proto3Count >= 1);
    assertEquals(
      "expected exactly 1 proto2 file (voucher_revocation_sync.proto), got " + proto2Count, 1,
      proto2Count);
  }

  @Test
  public void bundleHasExpectedMessageInventory() throws Exception {
    FileDescriptorSet set = loadBundle();
    int totalTopLevelMessages = 0;
    Set<String> proto3FilesWithMessages = new HashSet<>();
    for (FileDescriptorProto file : set.getFileList()) {
      totalTopLevelMessages += file.getMessageTypeCount();
      if (
        file.getMessageTypeCount() > 0 && (!file.hasSyntax() || "proto3".equals(file.getSyntax()))
      ) {
        proto3FilesWithMessages.add(file.getName());
      }
    }
    assertTrue(
      "expected at least " + EXPECTED_MESSAGE_COUNT_AT_LEAST
        + " total top-level messages across the bundle, got " + totalTopLevelMessages,
      totalTopLevelMessages >= EXPECTED_MESSAGE_COUNT_AT_LEAST);
  }

  @Test
  public void allFilesBuildIntoFileDescriptorGraph() throws Exception {
    FileDescriptorSet set = loadBundle();
    Map<String, FileDescriptor> graph = buildAllFileDescriptors(set);
    assertEquals("graph size should match file count", set.getFileCount(), graph.size());
  }

  @Test
  public void crossFileTypeReferenceResolves() throws Exception {
    FileDescriptorSet set = loadBundle();
    Map<String, FileDescriptor> graph = buildAllFileDescriptors(set);

    FileDescriptor accountSync = graph.get("account_sync.proto");
    assertNotNull(accountSync);
    Descriptor accountSyncResponse = accountSync.findMessageTypeByName("AccountSyncResponse");
    assertNotNull("AccountSyncResponse message must exist in account_sync.proto",
      accountSyncResponse);
    Descriptors.FieldDescriptor headerField = accountSyncResponse.findFieldByName("header");
    assertNotNull("AccountSyncResponse.header field must exist", headerField);
    assertEquals("header field must resolve to CatalogSyncHeader in widget_common.proto",
      "com.example.acmewidgets.common.v1.CatalogSyncHeader",
      headerField.getMessageType().getFullName());
  }

  @Test
  public void widgetGovernanceCrossFileFqRefsResolve() throws Exception {
    FileDescriptorSet set = loadBundle();
    Map<String, FileDescriptor> graph = buildAllFileDescriptors(set);

    FileDescriptor governance = graph.get("widget_governance.proto");
    assertNotNull(governance);
    Descriptor widgetEndpoint = governance.findMessageTypeByName("WidgetEndpoint");
    assertNotNull(widgetEndpoint);

    String[] expectedOneofAlternativeTypes =
      { "com.example.acmewidgets.governance.v1.endpointtypes.CatalogQuery",
        "com.example.acmewidgets.governance.v1.endpointtypes.BulkOrder",
        "com.example.acmewidgets.governance.v1.endpointtypes.FlashSale",
        "com.example.acmewidgets.governance.v1.endpointtypes.PreSaleSignup" };
    Set<String> actualOneofTypes = new HashSet<>();
    for (Descriptors.FieldDescriptor field : widgetEndpoint.getFields()) {
      if (
        field.getContainingOneof() != null
          && field.getContainingOneof().getName().equals("endpoint_kind")
      ) {
        actualOneofTypes.add(field.getMessageType().getFullName());
      }
    }
    for (String expected : expectedOneofAlternativeTypes) {
      assertTrue("WidgetEndpoint.endpoint_kind oneof missing alternative: " + expected
        + ". Actual: " + actualOneofTypes, actualOneofTypes.contains(expected));
    }
  }

  @Test
  public void entitlementSyncHasMapsAndNestedEnums() throws Exception {
    FileDescriptorSet set = loadBundle();
    Map<String, FileDescriptor> graph = buildAllFileDescriptors(set);

    FileDescriptor entitlement = graph.get("entitlement_sync.proto");
    assertNotNull(entitlement);

    Descriptor entitlementMsg = entitlement.findMessageTypeByName("Entitlement");
    assertNotNull(entitlementMsg);
    int mapFieldCount = 0;
    for (Descriptors.FieldDescriptor field : entitlementMsg.getFields()) {
      if (field.isMapField()) {
        mapFieldCount++;
      }
    }
    assertTrue("expected at least 4 map fields on Entitlement, got " + mapFieldCount,
      mapFieldCount >= 4);

    Descriptor stateWrapper = entitlement.findMessageTypeByName("EntitlementStateWrapper");
    assertNotNull(stateWrapper);
    assertNotNull("EntitlementStateWrapper.StateEnum must exist as a nested enum",
      stateWrapper.findEnumTypeByName("StateEnum"));
    Descriptor meteredWrapper = entitlement.findMessageTypeByName("MeteredStatusWrapper");
    assertNotNull(meteredWrapper);
    assertNotNull("MeteredStatusWrapper.StatusEnum must exist as a nested enum",
      meteredWrapper.findEnumTypeByName("StatusEnum"));
  }

  @Test
  public void entitlementSyncReservedFieldsArePreserved() throws Exception {
    FileDescriptorSet set = loadBundle();
    Descriptor entitlementMsg = null;
    for (FileDescriptorProto file : set.getFileList()) {
      if ("entitlement_sync.proto".equals(file.getName())) {
        for (DescriptorProto msg : file.getMessageTypeList()) {
          if ("Entitlement".equals(msg.getName())) {
            assertTrue("Entitlement should declare 2+ reserved ranges (for tags 4 and 11)",
              msg.getReservedRangeCount() >= 2);
            assertTrue("Entitlement should declare 2+ reserved names (enabling_org_perm + "
              + "is_metered_pricing_allowed)", msg.getReservedNameCount() >= 2);
            return;
          }
        }
      }
    }
    fail("Entitlement message not found in entitlement_sync.proto");
  }

  @Test
  public void voucherRevocationSyncIsProto2WithExtensions() throws Exception {
    FileDescriptorSet set = loadBundle();
    FileDescriptorProto voucher = null;
    for (FileDescriptorProto file : set.getFileList()) {
      if ("voucher_revocation_sync.proto".equals(file.getName())) {
        voucher = file;
        break;
      }
    }
    assertNotNull("voucher_revocation_sync.proto must be in the bundle", voucher);
    String syntax = voucher.hasSyntax() ? voucher.getSyntax() : "proto2";
    assertEquals("voucher_revocation_sync.proto must be proto2", "proto2", syntax);

    boolean foundTokenRevocation = false;
    for (DescriptorProto msg : voucher.getMessageTypeList()) {
      if ("TokenRevocation".equals(msg.getName())) {
        foundTokenRevocation = true;
        assertEquals("TokenRevocation should have 2 extensions ranges", 2,
          msg.getExtensionRangeCount());
        break;
      }
    }
    assertTrue("TokenRevocation message must be in voucher_revocation_sync.proto",
      foundTokenRevocation);

    assertEquals("voucher_revocation_sync.proto should have 2 file-scope extend fields", 2,
      voucher.getExtensionCount());
  }

  @Test
  public void dynamicMessageBuildsAndSerializesAccount() throws Exception {
    FileDescriptorSet set = loadBundle();
    Map<String, FileDescriptor> graph = buildAllFileDescriptors(set);
    Descriptor accountDesc = graph.get("account_sync.proto").findMessageTypeByName("Account");

    DynamicMessage account = DynamicMessage.newBuilder(accountDesc)
      .setField(accountDesc.findFieldByName("tenant_key"), "tenant-xyz-001")
      .setField(accountDesc.findFieldByName("display_name"), "Acme Test Tenant")
      .setField(accountDesc.findFieldByName("active"), true)
      .setField(accountDesc.findFieldByName("home_warehouse"), "warehouse-east-1").build();

    byte[] wireBytes = account.toByteArray();
    assertTrue("serialized Account should be non-empty", wireBytes.length > 0);

    DynamicMessage roundTripped =
      DynamicMessage.parseFrom(accountDesc, ByteString.copyFrom(wireBytes));
    assertEquals("tenant-xyz-001",
      roundTripped.getField(accountDesc.findFieldByName("tenant_key")));
    assertEquals("Acme Test Tenant",
      roundTripped.getField(accountDesc.findFieldByName("display_name")));
    assertEquals(Boolean.TRUE, roundTripped.getField(accountDesc.findFieldByName("active")));
  }

  @Test
  public void dynamicMessageBuildsAndSerializesWidgetEndpointWithOneof() throws Exception {
    FileDescriptorSet set = loadBundle();
    Map<String, FileDescriptor> graph = buildAllFileDescriptors(set);

    Descriptor catalogQueryDesc =
      graph.get("widget_endpoint_types.proto").findMessageTypeByName("CatalogQuery");
    Descriptor widgetEndpointDesc =
      graph.get("widget_governance.proto").findMessageTypeByName("WidgetEndpoint");

    DynamicMessage catalogQuery = DynamicMessage.newBuilder(catalogQueryDesc).build();
    DynamicMessage widgetEndpoint = DynamicMessage.newBuilder(widgetEndpointDesc)
      .setField(widgetEndpointDesc.findFieldByName("tenant_key"), "tenant-001")
      .setField(widgetEndpointDesc.findFieldByName("name"), "test-endpoint")
      .setField(widgetEndpointDesc.findFieldByName("catalog_query"), catalogQuery).build();

    byte[] wireBytes = widgetEndpoint.toByteArray();
    DynamicMessage roundTripped =
      DynamicMessage.parseFrom(widgetEndpointDesc, ByteString.copyFrom(wireBytes));
    assertEquals("tenant-001",
      roundTripped.getField(widgetEndpointDesc.findFieldByName("tenant_key")));
    Descriptors.OneofDescriptor endpointKind =
      widgetEndpointDesc.findFieldByName("catalog_query").getContainingOneof();
    assertNotNull(endpointKind);
    Descriptors.FieldDescriptor activeArm = roundTripped.getOneofFieldDescriptor(endpointKind);
    assertNotNull("oneof must have an active arm after round-trip", activeArm);
    assertEquals("catalog_query", activeArm.getName());
  }

  @Test
  public void dynamicMessageBuildsAndSerializesProto2TokenRevocation() throws Exception {
    FileDescriptorSet set = loadBundle();
    Map<String, FileDescriptor> graph = buildAllFileDescriptors(set);
    Descriptor tokenRevocationDesc =
      graph.get("voucher_revocation_sync.proto").findMessageTypeByName("TokenRevocation");

    DynamicMessage tokenRevocation = DynamicMessage.newBuilder(tokenRevocationDesc)
      .setField(tokenRevocationDesc.findFieldByName("tenant_id"), "tenant-001")
      .setField(tokenRevocationDesc.findFieldByName("token_id"), "tok-abc-123")
      .setField(tokenRevocationDesc.findFieldByName("reason"), "rotated").build();

    byte[] wireBytes = tokenRevocation.toByteArray();
    DynamicMessage roundTripped =
      DynamicMessage.parseFrom(tokenRevocationDesc, ByteString.copyFrom(wireBytes));
    assertEquals("tenant-001",
      roundTripped.getField(tokenRevocationDesc.findFieldByName("tenant_id")));
    assertEquals("tok-abc-123",
      roundTripped.getField(tokenRevocationDesc.findFieldByName("token_id")));
    assertEquals("rotated", roundTripped.getField(tokenRevocationDesc.findFieldByName("reason")));
    assertFalse("soft_revoke should be unset (default = false)",
      roundTripped.hasField(tokenRevocationDesc.findFieldByName("soft_revoke")));
  }

  @Test
  public void bundleParseIsDeterministic() throws Exception {
    FileDescriptorSet first = loadBundle();
    FileDescriptorSet second = loadBundle();
    assertEquals("two parses of the same bundle must produce equal FileDescriptorSet objects",
      first, second);
  }
}
