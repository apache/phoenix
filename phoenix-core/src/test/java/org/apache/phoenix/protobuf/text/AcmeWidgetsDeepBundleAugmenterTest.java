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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.phoenix.protobuf.descriptor.ProtoDescriptorResolver;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors;

public class AcmeWidgetsDeepBundleAugmenterTest {
  private static final String BUNDLE_RESOURCE = "protobuf/acmewidgets/acmewidgets_v1.desc";
  private static final String DEEP_PKG = AcmeWidgetsDeepBundleAugmenter.DEEP_PKG;

  @Test
  public void augmenterProducesWellFormedDescriptorSetContainingExactlyOneDeepFileEntry()
    throws Exception {
    byte[] existing = loadCheckedInBundle();
    FileDescriptorSet augmented = AcmeWidgetsDeepBundleAugmenter.augment(existing);

    FileDescriptorSet roundTripped = FileDescriptorSet.parseFrom(augmented.toByteArray());
    assertEquals(augmented.getFileCount(), roundTripped.getFileCount());

    int deepCount = 0;
    for (FileDescriptorProto file : augmented.getFileList()) {
      if ("widget_order_graph.proto".equals(file.getName())) {
        deepCount++;
      }
    }
    assertEquals("augmented set must have exactly one widget_order_graph.proto entry", 1,
      deepCount);
  }

  @Test
  public void augmenterIsIdempotentWhenRunTwice() throws Exception {
    byte[] existing = loadCheckedInBundle();
    FileDescriptorSet firstPass = AcmeWidgetsDeepBundleAugmenter.augment(existing);
    FileDescriptorSet secondPass = AcmeWidgetsDeepBundleAugmenter.augment(firstPass.toByteArray());

    assertEquals("second-pass augment must not add another widget_order_graph.proto entry",
      firstPass.getFileCount(), secondPass.getFileCount());

    assertEquals(firstPass, secondPass);
  }

  @Test
  public void augmenterPreservesAllExistingFileEntries() throws Exception {
    byte[] existing = loadCheckedInBundle();
    FileDescriptorSet before = FileDescriptorSet.parseFrom(existing);
    FileDescriptorSet after = AcmeWidgetsDeepBundleAugmenter.augment(existing);

    for (FileDescriptorProto beforeFile : before.getFileList()) {
      if ("widget_order_graph.proto".equals(beforeFile.getName())) {
        continue;
      }
      boolean found = false;
      for (FileDescriptorProto afterFile : after.getFileList()) {
        if (beforeFile.getName().equals(afterFile.getName())) {
          found = true;
          assertEquals(
            "existing file '" + beforeFile.getName() + "' bytes must survive augment untouched",
            beforeFile, afterFile);
          break;
        }
      }
      assertTrue("augmenter dropped existing file: " + beforeFile.getName(), found);
    }
  }

  @Test
  public void augmenterAllTopLevelMessagesResolve() throws Exception {
    byte[] augmentedBytes = augmentedBundleBytes();
    ProtoDescriptorResolver resolver = new ProtoDescriptorResolver();
    String[] topLevel =
      { "Money", "Category", "ContactInfo", "LineItem", "PaymentMethod", "Customer", "Order" };
    for (String simpleName : topLevel) {
      String fq = DEEP_PKG + "." + simpleName;
      Descriptors.Descriptor resolved = resolver.resolve(augmentedBytes, fq);
      assertNotNull("Top-level message '" + fq + "' must resolve in augmented bundle", resolved);
      assertEquals(fq, resolved.getFullName());
    }
  }

  @Test
  public void augmenterAllNestedMessagesResolve() throws Exception {
    byte[] augmentedBytes = augmentedBundleBytes();
    ProtoDescriptorResolver resolver = new ProtoDescriptorResolver();

    String[] nested =
      { "Category.AttributesEntry", "ContactInfo.Address", "ContactInfo.Address.GeoPoint",
        "LineItem.RegionalPricesEntry", "LineItem.Modifier", "LineItem.NamedModifiersEntry",
        "PaymentMethod.CreditCard", "PaymentMethod.BankAccount", "PaymentMethod.DigitalWallet",
        "Customer.PreferencesEntry", "Order.Event", "Order.Event.PayloadEntry" };
    for (String path : nested) {
      String fq = DEEP_PKG + "." + path;
      Descriptors.Descriptor resolved = resolver.resolve(augmentedBytes, fq);
      assertNotNull("Nested message '" + fq + "' must resolve in augmented bundle", resolved);
      assertEquals(fq, resolved.getFullName());
    }
  }

  @Test
  public void augmenterCategoryRecursiveSelfReferenceWiresUp() throws Exception {
    byte[] augmentedBytes = augmentedBundleBytes();
    Descriptors.Descriptor category =
      new ProtoDescriptorResolver().resolve(augmentedBytes, DEEP_PKG + ".Category");
    assertNotNull(category);

    Descriptors.FieldDescriptor parent = category.findFieldByName("parent");
    Descriptors.FieldDescriptor children = category.findFieldByName("children");
    assertEquals(DEEP_PKG + ".Category", parent.getMessageType().getFullName());
    assertEquals(DEEP_PKG + ".Category", children.getMessageType().getFullName());
    assertTrue("children must be repeated", children.isRepeated());
  }

  @Test
  public void augmenterModifierRecursiveViaRepeatedNestedWiresUp() throws Exception {
    byte[] augmentedBytes = augmentedBundleBytes();
    Descriptors.Descriptor modifier =
      new ProtoDescriptorResolver().resolve(augmentedBytes, DEEP_PKG + ".LineItem.Modifier");
    assertNotNull(modifier);
    Descriptors.FieldDescriptor subMods = modifier.findFieldByName("sub_modifiers");
    assertNotNull(subMods);
    assertTrue(subMods.isRepeated());
    assertEquals(DEEP_PKG + ".LineItem.Modifier", subMods.getMessageType().getFullName());

    assertNotNull("LineItem.Modifier.Kind nested enum must exist",
      modifier.findEnumTypeByName("Kind"));
  }

  @Test
  public void augmenterFourLevelDeepGeoPointResolves() throws Exception {
    byte[] augmentedBytes = augmentedBundleBytes();
    ProtoDescriptorResolver resolver = new ProtoDescriptorResolver();

    Descriptors.Descriptor geo =
      resolver.resolve(augmentedBytes, DEEP_PKG + ".ContactInfo.Address.GeoPoint");
    assertNotNull(geo);
    assertNotNull(geo.findFieldByName("latitude"));

    Descriptors.Descriptor customer = resolver.resolve(augmentedBytes, DEEP_PKG + ".Customer");
    Descriptors.FieldDescriptor contactField = customer.findFieldByName("contact");
    assertEquals(DEEP_PKG + ".ContactInfo", contactField.getMessageType().getFullName());
    Descriptors.FieldDescriptor addressesField =
      contactField.getMessageType().findFieldByName("addresses");
    assertTrue(addressesField.isRepeated());
    assertEquals(DEEP_PKG + ".ContactInfo.Address", addressesField.getMessageType().getFullName());
    Descriptors.FieldDescriptor geoField = addressesField.getMessageType().findFieldByName("geo");
    assertEquals(DEEP_PKG + ".ContactInfo.Address.GeoPoint",
      geoField.getMessageType().getFullName());
  }

  @Test
  public void augmenterCrossFileBackReferenceToWidgetCommonResolves() throws Exception {
    byte[] augmentedBytes = augmentedBundleBytes();
    Descriptors.Descriptor customer =
      new ProtoDescriptorResolver().resolve(augmentedBytes, DEEP_PKG + ".Customer");
    Descriptors.FieldDescriptor homeWarehouse = customer.findFieldByName("home_warehouse");
    assertNotNull(homeWarehouse);
    assertEquals("com.example.acmewidgets.common.v1.WarehouseEndpoint",
      homeWarehouse.getMessageType().getFullName());
  }

  @Test
  public void augmenterWellKnownTypeUsageResolves() throws Exception {
    byte[] augmentedBytes = augmentedBundleBytes();
    ProtoDescriptorResolver resolver = new ProtoDescriptorResolver();
    Descriptors.Descriptor order = resolver.resolve(augmentedBytes, DEEP_PKG + ".Order");
    Descriptors.FieldDescriptor created = order.findFieldByName("created_time");
    assertEquals("google.protobuf.Timestamp", created.getMessageType().getFullName());

    Descriptors.Descriptor event = resolver.resolve(augmentedBytes, DEEP_PKG + ".Order.Event");
    Descriptors.FieldDescriptor when = event.findFieldByName("when");
    assertEquals("google.protobuf.Timestamp", when.getMessageType().getFullName());
  }

  @Test
  public void augmenterPaymentMethodOneofArmsAllResolve() throws Exception {
    byte[] augmentedBytes = augmentedBundleBytes();
    Descriptors.Descriptor payment =
      new ProtoDescriptorResolver().resolve(augmentedBytes, DEEP_PKG + ".PaymentMethod");
    Descriptors.Descriptor desc = payment;
    String[] arms = { "credit_card", "bank_account", "digital_wallet" };
    Descriptors.OneofDescriptor kind = null;
    for (String arm : arms) {
      Descriptors.FieldDescriptor f = desc.findFieldByName(arm);
      assertNotNull("PaymentMethod oneof arm '" + arm + "' must exist", f);
      assertNotNull(arm + " must belong to a oneof", f.getContainingOneof());
      if (kind == null) {
        kind = f.getContainingOneof();
      } else {
        assertEquals("all 3 arms must share the same oneof", kind, f.getContainingOneof());
      }
    }
    assertEquals("method_kind", kind.getName());
    assertEquals(3, kind.getFieldCount());
  }

  @Test
  public void augmenterMapFieldsHaveCorrectValueTypes() throws Exception {
    byte[] augmentedBytes = augmentedBundleBytes();
    ProtoDescriptorResolver resolver = new ProtoDescriptorResolver();

    Descriptors.Descriptor category = resolver.resolve(augmentedBytes, DEEP_PKG + ".Category");
    Descriptors.FieldDescriptor attrs = category.findFieldByName("attributes");
    assertTrue("Category.attributes must be a map field", attrs.isMapField());
    assertEquals(Descriptors.FieldDescriptor.Type.STRING,
      attrs.getMessageType().findFieldByName("value").getType());

    Descriptors.Descriptor lineItem = resolver.resolve(augmentedBytes, DEEP_PKG + ".LineItem");
    Descriptors.FieldDescriptor regionalPrices = lineItem.findFieldByName("regional_prices");
    assertTrue(regionalPrices.isMapField());
    assertEquals("map<string, Money> value type", DEEP_PKG + ".Money",
      regionalPrices.getMessageType().findFieldByName("value").getMessageType().getFullName());

    Descriptors.FieldDescriptor namedMods = lineItem.findFieldByName("named_modifiers");
    assertTrue(namedMods.isMapField());
    assertEquals("map<string, LineItem.Modifier> value type", DEEP_PKG + ".LineItem.Modifier",
      namedMods.getMessageType().findFieldByName("value").getMessageType().getFullName());
  }

  private static byte[] augmentedBundleBytes() throws IOException {
    return AcmeWidgetsDeepBundleAugmenter.augment(loadCheckedInBundle()).toByteArray();
  }

  private static byte[] loadCheckedInBundle() throws IOException {
    ClassLoader cl = AcmeWidgetsDeepBundleAugmenterTest.class.getClassLoader();
    try (InputStream in = cl.getResourceAsStream(BUNDLE_RESOURCE)) {
      assertNotNull("acmewidgets_v1.desc not on test classpath", in);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[8192];
      int n;
      while ((n = in.read(buf)) > 0) {
        baos.write(buf, 0, n);
      }
      return baos.toByteArray();
    }
  }
}
