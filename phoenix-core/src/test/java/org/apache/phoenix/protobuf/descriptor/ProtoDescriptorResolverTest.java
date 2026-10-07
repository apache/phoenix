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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Assert;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.DescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors;

public class ProtoDescriptorResolverTest {
  private final ProtoDescriptorResolver resolver = new ProtoDescriptorResolver();

  @Test
  public void resolveSingleFileFindsTopLevelMessage() throws Exception {
    FileDescriptorSet set = singleFileSet("test.proto", "test", "Account");

    Descriptors.Descriptor resolved = resolver.resolve(set.toByteArray(), "test.Account");

    assertNotNull(resolved);
    assertEquals("test.Account", resolved.getFullName());
    assertEquals("Account", resolved.getName());
    assertEquals("test.proto", resolved.getFile().getName());
  }

  @Test
  public void resolveSingleFileWithMultipleMessages() throws Exception {
    FileDescriptorSet set = FileDescriptorSet.newBuilder()
      .addFile(FileDescriptorProto.newBuilder().setName("test.proto").setPackage("test")
        .setSyntax("proto3").addMessageType(DescriptorProto.newBuilder().setName("Foo"))
        .addMessageType(DescriptorProto.newBuilder().setName("Bar"))
        .addMessageType(DescriptorProto.newBuilder().setName("Baz")))
      .build();

    assertEquals("Bar", resolver.resolve(set.toByteArray(), "test.Bar").getName());
    assertEquals("Foo", resolver.resolve(set.toByteArray(), "test.Foo").getName());
    assertNull("name not in set returns null", resolver.resolve(set.toByteArray(), "test.Missing"));
  }

  @Test
  public void resolveUnpackagedMessage() throws Exception {
    FileDescriptorSet set = FileDescriptorSet
      .newBuilder().addFile(FileDescriptorProto.newBuilder().setName("test.proto")
        .setSyntax("proto3").addMessageType(DescriptorProto.newBuilder().setName("Unpackaged")))
      .build();

    Descriptors.Descriptor resolved = resolver.resolve(set.toByteArray(), "Unpackaged");

    assertNotNull(resolved);
    assertEquals("Unpackaged", resolved.getFullName());
  }

  @Test
  public void resolveMultiFileWithDependencyOrder() throws Exception {
    FileDescriptorProto common =
      FileDescriptorProto.newBuilder().setName("common.proto").setPackage("test.common")
        .setSyntax("proto3").addMessageType(DescriptorProto.newBuilder().setName("Header")).build();
    FileDescriptorProto schema = FileDescriptorProto.newBuilder().setName("schema.proto")
      .setPackage("test").setSyntax("proto3").addDependency("common.proto")
      .addMessageType(DescriptorProto.newBuilder().setName("Order")
        .addField(FieldDescriptorProto.newBuilder().setName("header").setNumber(1)
          .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".test.common.Header")
          .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
      .build();

    FileDescriptorSet set = FileDescriptorSet.newBuilder().addFile(schema).addFile(common).build();

    Descriptors.Descriptor resolvedOrder = resolver.resolve(set.toByteArray(), "test.Order");
    assertNotNull(resolvedOrder);

    assertEquals("test.common.Header",
      resolvedOrder.findFieldByName("header").getMessageType().getFullName());

    Descriptors.Descriptor resolvedHeader =
      resolver.resolve(set.toByteArray(), "test.common.Header");
    assertNotNull(resolvedHeader);
    assertEquals("Header", resolvedHeader.getName());
  }

  @Test
  public void resolveMissingDependencyThrowsIOException() {
    FileDescriptorProto schema = FileDescriptorProto.newBuilder().setName("schema.proto")
      .setPackage("test").setSyntax("proto3").addDependency("common.proto")
      .addMessageType(DescriptorProto.newBuilder().setName("Order")).build();
    FileDescriptorSet set = FileDescriptorSet.newBuilder().addFile(schema).build();

    try {
      resolver.resolve(set.toByteArray(), "test.Order");
      fail("Expected IOException for missing dependency");
    } catch (IOException e) {
      assertTrue("Error message should name the missing dependency: " + e.getMessage(),
        e.getMessage().contains("common.proto"));
      assertTrue("Error message should suggest --include_imports: " + e.getMessage(),
        e.getMessage().contains("--include_imports"));
    }
  }

  @Test
  public void resolveImportCycleDetected() {
    FileDescriptorProto a =
      FileDescriptorProto.newBuilder().setName("a.proto").setPackage("p").setSyntax("proto3")
        .addDependency("b.proto").addMessageType(DescriptorProto.newBuilder().setName("A")).build();
    FileDescriptorProto b =
      FileDescriptorProto.newBuilder().setName("b.proto").setPackage("p").setSyntax("proto3")
        .addDependency("a.proto").addMessageType(DescriptorProto.newBuilder().setName("B")).build();
    FileDescriptorSet set = FileDescriptorSet.newBuilder().addFile(a).addFile(b).build();

    try {
      resolver.resolve(set.toByteArray(), "p.A");
      fail("Expected IOException for import cycle");
    } catch (IOException e) {
      assertTrue("Error message should mention 'cycle': " + e.getMessage(),
        e.getMessage().toLowerCase().contains("cycle"));
    }
  }

  @Test
  public void resolveCorruptBytesThrowsIOException() {
    byte[] garbage = new byte[] { (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef };
    try {
      resolver.resolve(garbage, "test.Foo");
      fail("Expected IOException for malformed FileDescriptorSet bytes");
    } catch (IOException e) {
      assertTrue("Error message should describe what failed: " + e.getMessage(),
        e.getMessage().contains("FileDescriptorSet bytes"));
    }
  }

  @Test
  public void resolveNameNotInSetReturnsNull() throws Exception {
    FileDescriptorSet set = singleFileSet("test.proto", "test", "Account");
    assertNull(resolver.resolve(set.toByteArray(), "test.NotThere"));
    assertNull(resolver.resolve(set.toByteArray(), "other.Account"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void resolveNullBytesRejected() throws Exception {
    resolver.resolve(null, "test.Foo");
  }

  @Test(expected = IllegalArgumentException.class)
  public void resolveNullNameRejected() throws Exception {
    resolver.resolve(singleFileSet("test.proto", "test", "Account").toByteArray(), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void resolveEmptyNameRejected() throws Exception {
    resolver.resolve(singleFileSet("test.proto", "test", "Account").toByteArray(), "");
  }

  @Test
  public void resolveOneLevelNestedMessage() throws Exception {
    FileDescriptorSet set = FileDescriptorSet
      .newBuilder().addFile(FileDescriptorProto.newBuilder().setName("test.proto")
        .setPackage("test").setSyntax("proto3").addMessageType(DescriptorProto.newBuilder()
          .setName("Container").addNestedType(DescriptorProto.newBuilder().setName("Inner"))))
      .build();

    Descriptors.Descriptor outer = resolver.resolve(set.toByteArray(), "test.Container");
    assertNotNull(outer);
    assertEquals("Container", outer.getName());

    Descriptors.Descriptor inner = resolver.resolve(set.toByteArray(), "test.Container.Inner");
    assertNotNull("Nested message must be resolvable by its full dotted name", inner);
    assertEquals("test.Container.Inner", inner.getFullName());
    assertEquals("Inner", inner.getName());

    assertEquals(outer.getFile().getName(), inner.getFile().getName());
  }

  @Test
  public void resolveDeeplyNestedMessage() throws Exception {
    FileDescriptorSet set = FileDescriptorSet.newBuilder()
      .addFile(FileDescriptorProto.newBuilder().setName("test.proto").setPackage("test")
        .setSyntax("proto3").addMessageType(
          DescriptorProto.newBuilder().setName("Outer").addNestedType(DescriptorProto.newBuilder()
            .setName("Middle").addNestedType(DescriptorProto.newBuilder().setName("Inner")))))
      .build();

    assertEquals("Outer", resolver.resolve(set.toByteArray(), "test.Outer").getName());
    assertEquals("Middle", resolver.resolve(set.toByteArray(), "test.Outer.Middle").getName());
    assertEquals("Inner", resolver.resolve(set.toByteArray(), "test.Outer.Middle.Inner").getName());
  }

  @Test
  public void resolveSiblingNestedMessages() throws Exception {
    FileDescriptorSet set = FileDescriptorSet.newBuilder()
      .addFile(FileDescriptorProto.newBuilder().setName("test.proto").setPackage("test")
        .setSyntax("proto3")
        .addMessageType(DescriptorProto.newBuilder().setName("Container")
          .addNestedType(DescriptorProto.newBuilder().setName("A"))
          .addNestedType(DescriptorProto.newBuilder().setName("B"))))
      .build();

    assertEquals("test.Container.A",
      resolver.resolve(set.toByteArray(), "test.Container.A").getFullName());
    assertEquals("test.Container.B",
      resolver.resolve(set.toByteArray(), "test.Container.B").getFullName());
    assertNull("Sibling that doesn't exist must return null",
      resolver.resolve(set.toByteArray(), "test.Container.C"));
  }

  @Test
  public void resolveNestedMessageDoesNotCollideWithSimilarlyNamedTopLevel() throws Exception {
    FileDescriptorSet set = FileDescriptorSet.newBuilder()
      .addFile(FileDescriptorProto.newBuilder().setName("test.proto").setPackage("test")
        .setSyntax("proto3")
        .addMessageType(DescriptorProto.newBuilder().setName("Outer")
          .addNestedType(DescriptorProto.newBuilder().setName("Inner")))
        .addMessageType(DescriptorProto.newBuilder().setName("OuterInner")))
      .build();

    Descriptors.Descriptor topLevel = resolver.resolve(set.toByteArray(), "test.OuterInner");
    Descriptors.Descriptor nested = resolver.resolve(set.toByteArray(), "test.Outer.Inner");
    assertNotNull(topLevel);
    assertNotNull(nested);
    assertEquals("OuterInner", topLevel.getName());
    assertEquals("Inner", nested.getName());
  }

  @Test
  public void resolveNestedMessageInMultiFileSet() throws Exception {
    FileDescriptorProto common = FileDescriptorProto.newBuilder().setName("common.proto")
      .setPackage("common").setSyntax("proto3").addMessageType(DescriptorProto.newBuilder()
        .setName("Address").addNestedType(DescriptorProto.newBuilder().setName("PostalCode")))
      .build();
    FileDescriptorProto schema = FileDescriptorProto.newBuilder().setName("schema.proto")
      .setPackage("test").setSyntax("proto3").addDependency("common.proto")
      .addMessageType(DescriptorProto.newBuilder().setName("Order")).build();
    FileDescriptorSet set = FileDescriptorSet.newBuilder().addFile(schema).addFile(common).build();

    Descriptors.Descriptor nested =
      resolver.resolve(set.toByteArray(), "common.Address.PostalCode");
    assertNotNull(nested);
    assertEquals("PostalCode", nested.getName());
    assertEquals("common.proto", nested.getFile().getName());
  }

  @Test
  public void resolveNestedMessageWithUnpackagedOuter() throws Exception {
    FileDescriptorSet set = FileDescriptorSet.newBuilder()
      .addFile(FileDescriptorProto.newBuilder().setName("test.proto").setSyntax("proto3")
        .addMessageType(DescriptorProto.newBuilder().setName("Outer")
          .addNestedType(DescriptorProto.newBuilder().setName("Inner"))))
      .build();

    Descriptors.Descriptor inner = resolver.resolve(set.toByteArray(), "Outer.Inner");
    assertNotNull(inner);
    assertEquals("Outer.Inner", inner.getFullName());
  }

  @Test
  public void resolveNestedMessageThatDoesNotExistUnderRealOuterReturnsNull() throws Exception {
    FileDescriptorSet set =
      FileDescriptorSet
        .newBuilder().addFile(FileDescriptorProto.newBuilder().setName("test.proto")
          .setPackage("test").setSyntax("proto3").addMessageType(DescriptorProto.newBuilder()
            .setName("Container").addNestedType(DescriptorProto.newBuilder().setName("A"))))
        .build();

    assertNull(resolver.resolve(set.toByteArray(), "test.Container.Missing"));

    assertNull(resolver.resolve(set.toByteArray(), "test.NoSuchOuter.Inner"));
  }

  private static final String ACMEWIDGETS_BUNDLE_RESOURCE =
    "protobuf/acmewidgets/acmewidgets_v1.desc";
  private static final String ACCOUNT_PKG = "com.example.acmewidgets.account.v1";
  private static final String COMMON_PKG = "com.example.acmewidgets.common.v1";
  private static final String BILLING_PKG = "com.example.acmewidgets.billing.v1";
  private static final String GOV_PKG = "com.example.acmewidgets.governance.v1";
  private static final String ENDPOINT_TYPES_PKG =
    "com.example.acmewidgets.governance.v1.endpointtypes";
  private static final String PROTO2_AUTH_PKG = "com.example.acmewidgets.auth.v1.proto2";
  private static final String WKT_TIMESTAMP = "google.protobuf.Timestamp";

  private static byte[] loadAcmeWidgetsBundle() throws IOException {
    ClassLoader cl = ProtoDescriptorResolverTest.class.getClassLoader();
    try (InputStream in = cl.getResourceAsStream(ACMEWIDGETS_BUNDLE_RESOURCE)) {
      assertNotNull(
        "acmewidgets_v1.desc not on test classpath — did mvn test-compile copy "
          + "src/test/resources/protobuf/acmewidgets/acmewidgets_v1.desc to target/test-classes?",
        in);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[8192];
      int n;
      while ((n = in.read(buf)) > 0) {
        baos.write(buf, 0, n);
      }
      return baos.toByteArray();
    }
  }

  @Test
  public void resolveTopLevelMessagesInRealBundle() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();

    assertResolvesToFullName(bundle, ACCOUNT_PKG + ".Account");
    assertResolvesToFullName(bundle, ACCOUNT_PKG + ".AccountSyncResponse");
    assertResolvesToFullName(bundle, COMMON_PKG + ".CatalogSyncHeader");
    assertResolvesToFullName(bundle, COMMON_PKG + ".WarehouseEndpoint");
    assertResolvesToFullName(bundle, COMMON_PKG + ".PartnerEndpoint");
    assertResolvesToFullName(bundle, BILLING_PKG + ".Entitlement");
    assertResolvesToFullName(bundle, BILLING_PKG + ".EntitlementSyncResponse");
    assertResolvesToFullName(bundle, GOV_PKG + ".Policy");
    assertResolvesToFullName(bundle, GOV_PKG + ".WidgetEndpoint");
    assertResolvesToFullName(bundle, GOV_PKG + ".WidgetGovernanceSyncResponse");
    assertResolvesToFullName(bundle, ENDPOINT_TYPES_PKG + ".CatalogQuery");
    assertResolvesToFullName(bundle, ENDPOINT_TYPES_PKG + ".BulkOrder");
    assertResolvesToFullName(bundle, ENDPOINT_TYPES_PKG + ".FlashSale");
    assertResolvesToFullName(bundle, ENDPOINT_TYPES_PKG + ".PreSaleSignup");
  }

  @Test
  public void resolveNestedMessagesInRealBundle() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();

    assertResolvesToFullName(bundle, ENDPOINT_TYPES_PKG + ".CatalogQuery.NamedCatalogConnection");
    assertResolvesToFullName(bundle, ENDPOINT_TYPES_PKG + ".CatalogQuery.QueryOp");

    assertResolvesToFullName(bundle, ENDPOINT_TYPES_PKG + ".BulkOrder.NamedCatalogConnectionRef");
    assertResolvesToFullName(bundle, ENDPOINT_TYPES_PKG + ".BulkOrder.Key");

    assertResolvesToFullName(bundle, ENDPOINT_TYPES_PKG + ".FlashSale.PromotionId");
    assertResolvesToFullName(bundle, ENDPOINT_TYPES_PKG + ".FlashSale.Key");
  }

  @Test
  public void resolveNestedMessagePreservesSiblingSimpleNameDistinction() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();

    Descriptors.Descriptor bulkKey =
      resolver.resolve(bundle, ENDPOINT_TYPES_PKG + ".BulkOrder.Key");
    Descriptors.Descriptor flashKey =
      resolver.resolve(bundle, ENDPOINT_TYPES_PKG + ".FlashSale.Key");

    assertNotNull(bulkKey);
    assertNotNull(flashKey);
    assertEquals("BulkOrder", bulkKey.getContainingType().getName());
    assertEquals("FlashSale", flashKey.getContainingType().getName());

    assertEquals("Key", bulkKey.getName());
    assertEquals("Key", flashKey.getName());

    Assert.assertNotSame(bulkKey, flashKey);
  }

  @Test
  public void resolveNestedSiblingMessageReferenceWiresUpCorrectly() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();

    Descriptors.Descriptor queryOp =
      resolver.resolve(bundle, ENDPOINT_TYPES_PKG + ".CatalogQuery.QueryOp");
    assertNotNull(queryOp);
    Descriptors.FieldDescriptor defaultConnField = queryOp.findFieldByName("default_connection");
    assertNotNull("QueryOp.default_connection field must exist", defaultConnField);
    assertEquals("QueryOp.default_connection must point at the sibling NamedCatalogConnection",
      ENDPOINT_TYPES_PKG + ".CatalogQuery.NamedCatalogConnection",
      defaultConnField.getMessageType().getFullName());
  }

  @Test
  public void resolveCrossFileDependencyInRealBundle() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();

    Descriptors.Descriptor response =
      resolver.resolve(bundle, ACCOUNT_PKG + ".AccountSyncResponse");
    assertNotNull(response);
    Descriptors.FieldDescriptor header = response.findFieldByName("header");
    assertNotNull("AccountSyncResponse.header must exist", header);
    assertEquals("Cross-file dependency must resolve to CatalogSyncHeader from widget_common.proto",
      COMMON_PKG + ".CatalogSyncHeader", header.getMessageType().getFullName());
  }

  @Test
  public void resolveWellKnownTypeInRealBundle() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor ts = resolver.resolve(bundle, WKT_TIMESTAMP);
    assertNotNull("Well-known google.protobuf.Timestamp must resolve", ts);
    assertEquals("Timestamp", ts.getName());

    assertNotNull(ts.findFieldByName("seconds"));
    assertNotNull(ts.findFieldByName("nanos"));
  }

  @Test
  public void resolveFieldOfWellKnownTypeInRealBundle() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor ent = resolver.resolve(bundle, BILLING_PKG + ".Entitlement");
    assertNotNull(ent);
    Descriptors.FieldDescriptor deleted = ent.findFieldByName("deleted_time");
    assertNotNull(deleted);
    assertEquals(WKT_TIMESTAMP, deleted.getMessageType().getFullName());
  }

  @Test
  public void resolveOneofWithCrossFileMessageArmsInRealBundle() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor widgetEndpoint = resolver.resolve(bundle, GOV_PKG + ".WidgetEndpoint");
    assertNotNull(widgetEndpoint);
    Descriptors.OneofDescriptor kind =
      widgetEndpoint.findFieldByName("catalog_query").getContainingOneof();
    assertNotNull("catalog_query field must belong to a oneof", kind);
    assertEquals("endpoint_kind", kind.getName());
    assertEquals("Oneof must have exactly 4 arms", 4, kind.getFieldCount());

    for (Descriptors.FieldDescriptor arm : kind.getFields()) {
      assertTrue(
        "Oneof arm " + arm.getName() + " must resolve to a widget_endpoint_types.proto type, got "
          + arm.getMessageType().getFullName(),
        arm.getMessageType().getFullName().startsWith(ENDPOINT_TYPES_PKG + "."));
    }
  }

  @Test
  public void resolveMapFieldEntryInRealBundle() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor ent = resolver.resolve(bundle, BILLING_PKG + ".Entitlement");
    assertNotNull(ent);
    Descriptors.FieldDescriptor policyToggles = ent.findFieldByName("policy_toggles");
    assertNotNull(policyToggles);
    assertTrue("policy_toggles must be recognized as a map field", policyToggles.isMapField());

    String entryFqName = BILLING_PKG + ".Entitlement.PolicyTogglesEntry";
    Descriptors.Descriptor entry = resolver.resolve(bundle, entryFqName);
    assertNotNull("Synthetic map-entry descriptor " + entryFqName + " must be resolvable", entry);

    assertNotNull(entry.findFieldByName("key"));
    assertNotNull(entry.findFieldByName("value"));
    assertEquals(PROTO_MAP_ENTRY_KEY_TAG, entry.findFieldByName("key").getNumber());
    assertEquals(PROTO_MAP_ENTRY_VALUE_TAG, entry.findFieldByName("value").getNumber());
  }

  @Test
  public void resolveProto2MessageInRealBundle() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    assertResolvesToFullName(bundle, PROTO2_AUTH_PKG + ".TokenRevocation");
    assertResolvesToFullName(bundle, PROTO2_AUTH_PKG + ".AppUserRevocation");
    assertResolvesToFullName(bundle, PROTO2_AUTH_PKG + ".TokenRevocationSyncResponse");
  }

  @Test
  public void resolveMissingNameInRealBundleReturnsNull() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    assertNull(resolver.resolve(bundle, ACCOUNT_PKG + ".NonExistentMessage"));
    assertNull(resolver.resolve(bundle, "some.random.pkg.NoSuchType"));

    assertNull(resolver.resolve(bundle, ENDPOINT_TYPES_PKG + ".CatalogQuery.MissingInner"));
  }

  private static final String ORDERS_PKG = "com.example.acmewidgets.orders.v1";

  @Test
  public void resolveDeepFixtureAllTopLevelMessages() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();

    for (String simpleName : new String[] { "Money", "Category", "ContactInfo", "LineItem",
      "PaymentMethod", "Customer", "Order" }) {
      assertResolvesToFullName(bundle, ORDERS_PKG + "." + simpleName);
    }
  }

  @Test
  public void resolveDeepFixtureFourLevelNestedGeoPoint() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor geo =
      resolver.resolve(bundle, ORDERS_PKG + ".ContactInfo.Address.GeoPoint");
    assertNotNull(geo);
    assertEquals("GeoPoint", geo.getName());

    assertEquals("Address", geo.getContainingType().getName());
    assertEquals("ContactInfo", geo.getContainingType().getContainingType().getName());
    assertNull("ContactInfo is top-level (no containing type)",
      geo.getContainingType().getContainingType().getContainingType());

    assertEquals(Descriptors.FieldDescriptor.Type.DOUBLE,
      geo.findFieldByName("latitude").getType());
  }

  @Test
  public void resolveDeepFixtureRecursiveSelfReferenceCategory() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor category = resolver.resolve(bundle, ORDERS_PKG + ".Category");
    assertNotNull(category);
    Descriptors.FieldDescriptor parent = category.findFieldByName("parent");
    Descriptors.FieldDescriptor children = category.findFieldByName("children");
    assertEquals(ORDERS_PKG + ".Category", parent.getMessageType().getFullName());
    assertEquals(ORDERS_PKG + ".Category", children.getMessageType().getFullName());
    assertTrue("children must be repeated (tree of same-type)", children.isRepeated());
    assertTrue("parent must NOT be repeated (single link up)", !parent.isRepeated());
  }

  @Test
  public void resolveDeepFixtureRecursiveRepeatedNestedModifier() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor modifier = resolver.resolve(bundle, ORDERS_PKG + ".LineItem.Modifier");
    assertNotNull(modifier);
    Descriptors.FieldDescriptor subMods = modifier.findFieldByName("sub_modifiers");
    assertEquals(ORDERS_PKG + ".LineItem.Modifier", subMods.getMessageType().getFullName());
    assertTrue(subMods.isRepeated());
    assertNotNull("Modifier.Kind nested enum must be findable on the resolved descriptor",
      modifier.findEnumTypeByName("Kind"));
  }

  @Test
  public void resolveDeepFixtureThreeLevelDeepPathThroughFields() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor customer = resolver.resolve(bundle, ORDERS_PKG + ".Customer");

    Descriptors.FieldDescriptor contact = customer.findFieldByName("contact");
    Descriptors.FieldDescriptor addresses = contact.getMessageType().findFieldByName("addresses");
    assertTrue("addresses must be repeated", addresses.isRepeated());
    Descriptors.FieldDescriptor geo = addresses.getMessageType().findFieldByName("geo");
    Descriptors.FieldDescriptor latitude = geo.getMessageType().findFieldByName("latitude");
    assertEquals(Descriptors.FieldDescriptor.Type.DOUBLE, latitude.getType());

    assertEquals(ORDERS_PKG + ".ContactInfo", contact.getMessageType().getFullName());
    assertEquals(ORDERS_PKG + ".ContactInfo.Address", addresses.getMessageType().getFullName());
    assertEquals(ORDERS_PKG + ".ContactInfo.Address.GeoPoint", geo.getMessageType().getFullName());
  }

  @Test
  public void resolveDeepFixtureMapOfMessageValues() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor entry =
      resolver.resolve(bundle, ORDERS_PKG + ".LineItem.RegionalPricesEntry");
    assertNotNull(entry);
    Descriptors.FieldDescriptor value = entry.findFieldByName("value");
    assertEquals(ORDERS_PKG + ".Money", value.getMessageType().getFullName());

    Descriptors.Descriptor lineItem = resolver.resolve(bundle, ORDERS_PKG + ".LineItem");
    Descriptors.FieldDescriptor regional = lineItem.findFieldByName("regional_prices");
    assertTrue(regional.isMapField());
  }

  @Test
  public void resolveDeepFixtureMapOfRecursiveTypeValues() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor entry =
      resolver.resolve(bundle, ORDERS_PKG + ".LineItem.NamedModifiersEntry");
    assertNotNull(entry);
    Descriptors.FieldDescriptor value = entry.findFieldByName("value");
    assertEquals(ORDERS_PKG + ".LineItem.Modifier", value.getMessageType().getFullName());

    Descriptors.FieldDescriptor subMods = value.getMessageType().findFieldByName("sub_modifiers");
    assertEquals(ORDERS_PKG + ".LineItem.Modifier", subMods.getMessageType().getFullName());
  }

  @Test
  public void resolveDeepFixturePaymentOneofArmsAllResolve() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    for (String arm : new String[] { "CreditCard", "BankAccount", "DigitalWallet" }) {
      assertResolvesToFullName(bundle, ORDERS_PKG + ".PaymentMethod." + arm);
    }
    Descriptors.Descriptor payment = resolver.resolve(bundle, ORDERS_PKG + ".PaymentMethod");
    Descriptors.OneofDescriptor kind = payment.findFieldByName("credit_card").getContainingOneof();
    assertEquals("method_kind", kind.getName());
    assertEquals(3, kind.getFieldCount());

    for (String armField : new String[] { "credit_card", "bank_account", "digital_wallet" }) {
      Descriptors.FieldDescriptor f = payment.findFieldByName(armField);
      assertEquals("arm " + armField + " must belong to method_kind oneof", kind,
        f.getContainingOneof());
    }
  }

  @Test
  public void resolveDeepFixtureCrossFileBackReferenceToWidgetCommon() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor customer = resolver.resolve(bundle, ORDERS_PKG + ".Customer");
    Descriptors.FieldDescriptor homeWarehouse = customer.findFieldByName("home_warehouse");
    assertEquals("com.example.acmewidgets.common.v1.WarehouseEndpoint",
      homeWarehouse.getMessageType().getFullName());
  }

  @Test
  public void resolveNestedArraysTwoLevelDistinctTypeCompositionOrderLineItemsModifiers()
    throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor order = resolver.resolve(bundle, ORDERS_PKG + ".Order");
    assertNotNull(order);

    Descriptors.FieldDescriptor lineItems = order.findFieldByName("line_items");
    assertNotNull("Order.line_items must exist", lineItems);
    assertTrue("Level 1 (line_items) must be repeated", lineItems.isRepeated());
    assertEquals(ORDERS_PKG + ".LineItem", lineItems.getMessageType().getFullName());

    Descriptors.FieldDescriptor modifiers = lineItems.getMessageType().findFieldByName("modifiers");
    assertNotNull("LineItem.modifiers must exist", modifiers);
    assertTrue("Level 2 (modifiers) must ALSO be repeated (nested array)", modifiers.isRepeated());
    assertEquals("Level 2 element type must differ from level 1 element type",
      ORDERS_PKG + ".LineItem.Modifier", modifiers.getMessageType().getFullName());
    Assert.assertNotEquals("Two nested-array element types must be distinct",
      lineItems.getMessageType().getFullName(), modifiers.getMessageType().getFullName());
  }

  @Test
  public void resolveNestedArraysThreeLevelChainOrderLineItemsModifiersSubModifiers()
    throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor order = resolver.resolve(bundle, ORDERS_PKG + ".Order");
    Descriptors.FieldDescriptor level1 = order.findFieldByName("line_items");
    Descriptors.FieldDescriptor level2 = level1.getMessageType().findFieldByName("modifiers");
    Descriptors.FieldDescriptor level3 = level2.getMessageType().findFieldByName("sub_modifiers");

    assertTrue("Level 1 (line_items) must be repeated", level1.isRepeated());
    assertTrue("Level 2 (modifiers) must be repeated", level2.isRepeated());
    assertTrue("Level 3 (sub_modifiers) must be repeated", level3.isRepeated());
    assertEquals(ORDERS_PKG + ".LineItem", level1.getMessageType().getFullName());
    assertEquals(ORDERS_PKG + ".LineItem.Modifier", level2.getMessageType().getFullName());
    assertEquals("Level 3 recurses back to Modifier (same as level 2 type)",
      ORDERS_PKG + ".LineItem.Modifier", level3.getMessageType().getFullName());

    Descriptors.FieldDescriptor level4 = level3.getMessageType().findFieldByName("sub_modifiers");
    assertTrue(level4.isRepeated());
    Descriptors.FieldDescriptor amountUnits =
      level4.getMessageType().findFieldByName("amount").getMessageType().findFieldByName("units");
    assertEquals(Descriptors.FieldDescriptor.Type.INT64, amountUnits.getType());
  }

  @Test
  public void resolveNestedArraysThroughMapValueOrderLineItemsNamedModifiersSubModifiers()
    throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor order = resolver.resolve(bundle, ORDERS_PKG + ".Order");
    Descriptors.FieldDescriptor lineItems = order.findFieldByName("line_items");
    assertTrue("line_items must be repeated", lineItems.isRepeated());
    Descriptors.FieldDescriptor namedMods =
      lineItems.getMessageType().findFieldByName("named_modifiers");
    assertTrue("named_modifiers must be a map field", namedMods.isMapField());

    Descriptors.FieldDescriptor mapValue = namedMods.getMessageType().findFieldByName("value");
    assertEquals(ORDERS_PKG + ".LineItem.Modifier", mapValue.getMessageType().getFullName());

    Descriptors.FieldDescriptor subMods =
      mapValue.getMessageType().findFieldByName("sub_modifiers");
    assertTrue("sub_modifiers on map-value's message type must be repeated", subMods.isRepeated());
    assertEquals(ORDERS_PKG + ".LineItem.Modifier", subMods.getMessageType().getFullName());
  }

  @Test
  public void resolveNestedArraysCrossMessageChainOrderLineItemsCategoryChildren()
    throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor order = resolver.resolve(bundle, ORDERS_PKG + ".Order");
    Descriptors.FieldDescriptor lineItems = order.findFieldByName("line_items");
    assertTrue(lineItems.isRepeated());
    Descriptors.FieldDescriptor category = lineItems.getMessageType().findFieldByName("category");
    assertNotNull("LineItem.category must exist", category);
    assertTrue("Intermediate hop (category) must NOT be repeated — proves the walk can pass "
      + "through a singular bridge between two arrays", !category.isRepeated());
    assertEquals(ORDERS_PKG + ".Category", category.getMessageType().getFullName());
    Descriptors.FieldDescriptor children = category.getMessageType().findFieldByName("children");
    assertTrue("Bottom of chain (Category.children) must be repeated", children.isRepeated());
    assertEquals(ORDERS_PKG + ".Category", children.getMessageType().getFullName());

    Descriptors.FieldDescriptor grandchildren =
      children.getMessageType().findFieldByName("children");
    assertTrue("Recursive tree descent must remain repeated at every level",
      grandchildren.isRepeated());
  }

  @Test
  public void resolveDeepFixtureCompositeRepeatedAndMapOnSameMessage() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor lineItem = resolver.resolve(bundle, ORDERS_PKG + ".LineItem");
    Descriptors.FieldDescriptor modifiers = lineItem.findFieldByName("modifiers");
    Descriptors.FieldDescriptor named = lineItem.findFieldByName("named_modifiers");
    assertTrue("modifiers is repeated, not map", modifiers.isRepeated() && !modifiers.isMapField());
    assertTrue("named_modifiers is map", named.isMapField());

    assertEquals(ORDERS_PKG + ".LineItem.Modifier", modifiers.getMessageType().getFullName());
    assertEquals(ORDERS_PKG + ".LineItem.Modifier",
      named.getMessageType().findFieldByName("value").getMessageType().getFullName());
  }

  @Test
  public void resolveDoesNotConfuseTopLevelWithNestedLookupInRealBundle() throws Exception {
    byte[] bundle = loadAcmeWidgetsBundle();
    Descriptors.Descriptor catalogQuery =
      resolver.resolve(bundle, ENDPOINT_TYPES_PKG + ".CatalogQuery");
    Descriptors.Descriptor nestedQueryOp =
      resolver.resolve(bundle, ENDPOINT_TYPES_PKG + ".CatalogQuery.QueryOp");
    assertNotNull(catalogQuery);
    assertNotNull(nestedQueryOp);
    assertEquals("CatalogQuery", catalogQuery.getName());
    assertEquals("QueryOp", nestedQueryOp.getName());
    Assert.assertNotSame(catalogQuery, nestedQueryOp);

    assertEquals("CatalogQuery", nestedQueryOp.getContainingType().getName());

    assertNull(catalogQuery.getContainingType());
  }

  private static final int PROTO_MAP_ENTRY_KEY_TAG = 1;
  private static final int PROTO_MAP_ENTRY_VALUE_TAG = 2;

  private void assertResolvesToFullName(byte[] bundle, String fqName) throws IOException {
    Descriptors.Descriptor resolved = resolver.resolve(bundle, fqName);
    assertNotNull("Real-bundle resolution for '" + fqName + "' must succeed", resolved);
    assertEquals("Resolved descriptor's full name must equal the requested FQ name", fqName,
      resolved.getFullName());
  }

  private static FileDescriptorSet singleFileSet(String fileName, String pkg, String messageName) {
    return FileDescriptorSet.newBuilder()
      .addFile(FileDescriptorProto.newBuilder().setName(fileName).setPackage(pkg)
        .setSyntax("proto3").addMessageType(DescriptorProto.newBuilder().setName(messageName)))
      .build();
  }
}
