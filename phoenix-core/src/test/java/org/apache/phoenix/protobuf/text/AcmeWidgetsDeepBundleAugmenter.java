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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.DescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileOptions;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.MessageOptions;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.OneofDescriptorProto;

/**
 * Rebuilds {@code widget_order_graph.proto} into {@code acmewidgets_v1.desc} using
 * phoenix-thirdparty-protobuf (no {@code protoc}). Idempotent. See the acmewidgets README for how
 * to re-run.
 */
public final class AcmeWidgetsDeepBundleAugmenter {

  private static final String BUNDLE_PATH_FROM_MODULE_ROOT =
    "src/test/resources/protobuf/acmewidgets/acmewidgets_v1.desc";

  private static final String DEEP_FILE_NAME = "widget_order_graph.proto";
  static final String DEEP_PKG = "com.example.acmewidgets.orders.v1";
  private static final String COMMON_PKG = "com.example.acmewidgets.common.v1";

  private AcmeWidgetsDeepBundleAugmenter() {
    // Utility class
  }

  public static void main(String[] args) throws IOException {
    Path bundlePath = resolveBundlePath(args);
    System.out.println("Reading bundle: " + bundlePath.toAbsolutePath());
    byte[] existing = Files.readAllBytes(bundlePath);
    FileDescriptorSet updated = augment(existing);
    Files.write(bundlePath, updated.toByteArray());
    System.out.println("Wrote " + updated.getFileCount() + " file entries ("
      + updated.toByteArray().length + " bytes) to " + bundlePath.toAbsolutePath());
  }

  /**
   * Strips any existing {@code widget_order_graph.proto} entry and appends a rebuilt one.
   */
  public static FileDescriptorSet augment(byte[] existingBundleBytes) throws IOException {
    FileDescriptorSet existing = FileDescriptorSet.parseFrom(existingBundleBytes);
    List<FileDescriptorProto> keep = new ArrayList<>();
    for (FileDescriptorProto file : existing.getFileList()) {
      if (!DEEP_FILE_NAME.equals(file.getName())) {
        keep.add(file);
      }
    }
    FileDescriptorSet.Builder augmented = FileDescriptorSet.newBuilder().addAllFile(keep);
    augmented.addFile(buildWidgetOrderGraph());
    return augmented.build();
  }

  private static Path resolveBundlePath(String[] args) {
    if (args != null && args.length > 0) {
      return Paths.get(args[0]);
    }
    return Paths.get(BUNDLE_PATH_FROM_MODULE_ROOT);
  }

  static FileDescriptorProto buildWidgetOrderGraph() {
    return FileDescriptorProto.newBuilder().setName(DEEP_FILE_NAME).setPackage(DEEP_PKG)
      .setSyntax("proto3").addDependency("google/protobuf/timestamp.proto")
      .addDependency("widget_common.proto")
      .setOptions(
        FileOptions.newBuilder().setJavaPackage(DEEP_PKG).setJavaMultipleFiles(true).build())
      .addMessageType(buildMoney()).addMessageType(buildCategory())
      .addMessageType(buildContactInfo()).addMessageType(buildLineItem())
      .addMessageType(buildPaymentMethod()).addMessageType(buildCustomer())
      .addMessageType(buildOrder()).build();
  }

  private static DescriptorProto buildMoney() {
    return DescriptorProto.newBuilder().setName("Money")
      .addField(scalarField("currency_code", 1, Type.TYPE_STRING))
      .addField(scalarField("units", 2, Type.TYPE_INT64))
      .addField(scalarField("nanos", 3, Type.TYPE_INT32)).build();
  }

  private static DescriptorProto buildCategory() {
    String fqCategory = "." + DEEP_PKG + ".Category";
    DescriptorProto attributesEntry =
      mapEntry("AttributesEntry", Type.TYPE_STRING, null, Type.TYPE_STRING, null);
    return DescriptorProto.newBuilder().setName("Category")
      .addField(scalarField("id", 1, Type.TYPE_STRING))
      .addField(scalarField("name", 2, Type.TYPE_STRING))
      .addField(messageField("parent", 3, fqCategory, Label.LABEL_OPTIONAL))
      .addField(messageField("children", 4, fqCategory, Label.LABEL_REPEATED))
      .addField(mapField("attributes", 5, "." + DEEP_PKG + ".Category.AttributesEntry"))
      .addNestedType(attributesEntry).build();
  }

  private static DescriptorProto buildContactInfo() {
    DescriptorProto geoPoint = DescriptorProto.newBuilder().setName("GeoPoint")
      .addField(scalarField("latitude", 1, Type.TYPE_DOUBLE))
      .addField(scalarField("longitude", 2, Type.TYPE_DOUBLE)).build();
    DescriptorProto address = DescriptorProto.newBuilder().setName("Address")
      .addField(scalarField("street", 1, Type.TYPE_STRING))
      .addField(scalarField("city", 2, Type.TYPE_STRING))
      .addField(scalarField("region", 3, Type.TYPE_STRING))
      .addField(scalarField("postal_code", 4, Type.TYPE_STRING))
      .addField(scalarField("country_code", 5, Type.TYPE_STRING)).addField(messageField("geo", 6,
        "." + DEEP_PKG + ".ContactInfo.Address.GeoPoint", Label.LABEL_OPTIONAL))
      .addNestedType(geoPoint).build();
    return DescriptorProto.newBuilder().setName("ContactInfo")
      .addField(scalarField("email", 1, Type.TYPE_STRING))
      .addField(scalarField("phone", 2, Type.TYPE_STRING))
      .addField(
        messageField("addresses", 3, "." + DEEP_PKG + ".ContactInfo.Address", Label.LABEL_REPEATED))
      .addNestedType(address).build();
  }

  private static DescriptorProto buildLineItem() {
    EnumDescriptorProto modifierKind = EnumDescriptorProto.newBuilder().setName("Kind")
      .addValue(enumValue("KIND_UNSPECIFIED", 0)).addValue(enumValue("DISCOUNT", 1))
      .addValue(enumValue("SURCHARGE", 2)).addValue(enumValue("GIFT_WRAP", 3)).build();
    DescriptorProto namedModifiersEntry = mapEntry("NamedModifiersEntry", Type.TYPE_STRING, null,
      Type.TYPE_MESSAGE, "." + DEEP_PKG + ".LineItem.Modifier");
    DescriptorProto modifier = DescriptorProto.newBuilder().setName("Modifier")
      .addField(scalarField("modifier_id", 1, Type.TYPE_STRING))
      .addField(scalarField("display_name", 2, Type.TYPE_STRING))
      .addField(enumField("kind", 3, "." + DEEP_PKG + ".LineItem.Modifier.Kind"))
      .addField(messageField("amount", 4, "." + DEEP_PKG + ".Money", Label.LABEL_OPTIONAL))
      .addField(messageField("sub_modifiers", 5, "." + DEEP_PKG + ".LineItem.Modifier",
        Label.LABEL_REPEATED))
      .addEnumType(modifierKind).build();
    DescriptorProto regionalPricesEntry = mapEntry("RegionalPricesEntry", Type.TYPE_STRING, null,
      Type.TYPE_MESSAGE, "." + DEEP_PKG + ".Money");
    return DescriptorProto.newBuilder().setName("LineItem")
      .addField(scalarField("line_id", 1, Type.TYPE_STRING))
      .addField(scalarField("sku", 2, Type.TYPE_STRING))
      .addField(messageField("category", 3, "." + DEEP_PKG + ".Category", Label.LABEL_OPTIONAL))
      .addField(scalarField("quantity", 4, Type.TYPE_INT32))
      .addField(messageField("unit_price", 5, "." + DEEP_PKG + ".Money", Label.LABEL_OPTIONAL))
      .addField(messageField("line_total", 6, "." + DEEP_PKG + ".Money", Label.LABEL_OPTIONAL))
      .addField(mapField("regional_prices", 7, "." + DEEP_PKG + ".LineItem.RegionalPricesEntry"))
      .addField(
        messageField("modifiers", 8, "." + DEEP_PKG + ".LineItem.Modifier", Label.LABEL_REPEATED))
      .addField(mapField("named_modifiers", 9, "." + DEEP_PKG + ".LineItem.NamedModifiersEntry"))
      .addNestedType(regionalPricesEntry).addNestedType(modifier).addNestedType(namedModifiersEntry)
      .build();
  }

  private static DescriptorProto buildPaymentMethod() {
    DescriptorProto creditCard = DescriptorProto.newBuilder().setName("CreditCard")
      .addField(scalarField("masked_pan", 1, Type.TYPE_STRING))
      .addField(scalarField("card_network", 2, Type.TYPE_STRING))
      .addField(scalarField("exp_year", 3, Type.TYPE_INT32))
      .addField(scalarField("exp_month", 4, Type.TYPE_INT32)).build();
    DescriptorProto bankAccount = DescriptorProto.newBuilder().setName("BankAccount")
      .addField(scalarField("masked_iban", 1, Type.TYPE_STRING))
      .addField(scalarField("bank_code", 2, Type.TYPE_STRING)).build();
    DescriptorProto digitalWallet = DescriptorProto.newBuilder().setName("DigitalWallet")
      .addField(scalarField("wallet_type", 1, Type.TYPE_STRING))
      .addField(scalarField("account_id", 2, Type.TYPE_STRING)).build();
    // OneofIndex is the position in oneof_decl, not the field index (payment_id=0, amount=1).
    return DescriptorProto.newBuilder().setName("PaymentMethod")
      .addField(scalarField("payment_id", 1, Type.TYPE_STRING))
      .addField(messageField("amount", 2, "." + DEEP_PKG + ".Money", Label.LABEL_OPTIONAL))
      .addField(
        oneofMessageField("credit_card", 3, "." + DEEP_PKG + ".PaymentMethod.CreditCard", 0))
      .addField(
        oneofMessageField("bank_account", 4, "." + DEEP_PKG + ".PaymentMethod.BankAccount", 0))
      .addField(
        oneofMessageField("digital_wallet", 5, "." + DEEP_PKG + ".PaymentMethod.DigitalWallet", 0))
      .addOneofDecl(OneofDescriptorProto.newBuilder().setName("method_kind"))
      .addNestedType(creditCard).addNestedType(bankAccount).addNestedType(digitalWallet).build();
  }

  private static DescriptorProto buildCustomer() {
    DescriptorProto preferencesEntry =
      mapEntry("PreferencesEntry", Type.TYPE_STRING, null, Type.TYPE_STRING, null);
    return DescriptorProto.newBuilder().setName("Customer")
      .addField(scalarField("customer_id", 1, Type.TYPE_STRING))
      .addField(scalarField("display_name", 2, Type.TYPE_STRING))
      .addField(messageField("contact", 3, "." + DEEP_PKG + ".ContactInfo", Label.LABEL_OPTIONAL))
      .addField(messageField("home_warehouse", 4, "." + COMMON_PKG + ".WarehouseEndpoint",
        Label.LABEL_OPTIONAL))
      .addField(mapField("preferences", 5, "." + DEEP_PKG + ".Customer.PreferencesEntry"))
      .addNestedType(preferencesEntry).build();
  }

  private static DescriptorProto buildOrder() {
    DescriptorProto eventPayloadEntry =
      mapEntry("PayloadEntry", Type.TYPE_STRING, null, Type.TYPE_STRING, null);
    DescriptorProto event = DescriptorProto.newBuilder().setName("Event")
      .addField(messageField("when", 1, ".google.protobuf.Timestamp", Label.LABEL_OPTIONAL))
      .addField(scalarField("kind", 2, Type.TYPE_STRING))
      .addField(mapField("payload", 3, "." + DEEP_PKG + ".Order.Event.PayloadEntry"))
      .addNestedType(eventPayloadEntry).build();
    return DescriptorProto.newBuilder().setName("Order")
      .addField(scalarField("order_id", 1, Type.TYPE_STRING))
      .addField(messageField("customer", 2, "." + DEEP_PKG + ".Customer", Label.LABEL_OPTIONAL))
      .addField(messageField("line_items", 3, "." + DEEP_PKG + ".LineItem", Label.LABEL_REPEATED))
      .addField(
        messageField("payments", 4, "." + DEEP_PKG + ".PaymentMethod", Label.LABEL_REPEATED))
      .addField(messageField("subtotal", 5, "." + DEEP_PKG + ".Money", Label.LABEL_OPTIONAL))
      .addField(messageField("tax_total", 6, "." + DEEP_PKG + ".Money", Label.LABEL_OPTIONAL))
      .addField(messageField("grand_total", 7, "." + DEEP_PKG + ".Money", Label.LABEL_OPTIONAL))
      .addField(messageField("created_time", 8, ".google.protobuf.Timestamp", Label.LABEL_OPTIONAL))
      .addField(messageField("history", 9, "." + DEEP_PKG + ".Order.Event", Label.LABEL_REPEATED))
      .addNestedType(event).build();
  }

  private static FieldDescriptorProto scalarField(String name, int number, Type type) {
    return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type)
      .setLabel(Label.LABEL_OPTIONAL).build();
  }

  private static FieldDescriptorProto messageField(String name, int number, String fqTypeName,
    Label label) {
    return FieldDescriptorProto.newBuilder().setName(name).setNumber(number)
      .setType(Type.TYPE_MESSAGE).setTypeName(fqTypeName).setLabel(label).build();
  }

  private static FieldDescriptorProto enumField(String name, int number, String fqEnumName) {
    return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(Type.TYPE_ENUM)
      .setTypeName(fqEnumName).setLabel(Label.LABEL_OPTIONAL).build();
  }

  /** Wire form of a map field: {@code repeated MapEntry}. */
  private static FieldDescriptorProto mapField(String name, int number, String fqEntryTypeName) {
    return FieldDescriptorProto.newBuilder().setName(name).setNumber(number)
      .setType(Type.TYPE_MESSAGE).setTypeName(fqEntryTypeName).setLabel(Label.LABEL_REPEATED)
      .build();
  }

  private static FieldDescriptorProto oneofMessageField(String name, int number, String fqTypeName,
    int oneofIndex) {
    return FieldDescriptorProto.newBuilder().setName(name).setNumber(number)
      .setType(Type.TYPE_MESSAGE).setTypeName(fqTypeName).setLabel(Label.LABEL_OPTIONAL)
      .setOneofIndex(oneofIndex).build();
  }

  private static EnumValueDescriptorProto enumValue(String name, int number) {
    return EnumValueDescriptorProto.newBuilder().setName(name).setNumber(number).build();
  }

  /** Synthetic map-entry nested type: key=1, value=2, {@code map_entry = true}. */
  private static DescriptorProto mapEntry(String entryName, Type keyType, String keyMessageName,
    Type valueType, String valueMessageName) {
    FieldDescriptorProto.Builder keyBuilder = FieldDescriptorProto.newBuilder().setName("key")
      .setNumber(1).setType(keyType).setLabel(Label.LABEL_OPTIONAL);
    if (keyMessageName != null) {
      keyBuilder.setTypeName(keyMessageName);
    }
    FieldDescriptorProto.Builder valueBuilder = FieldDescriptorProto.newBuilder().setName("value")
      .setNumber(2).setType(valueType).setLabel(Label.LABEL_OPTIONAL);
    if (valueMessageName != null) {
      valueBuilder.setTypeName(valueMessageName);
    }
    return DescriptorProto.newBuilder().setName(entryName).addField(keyBuilder.build())
      .addField(valueBuilder.build())
      .setOptions(MessageOptions.newBuilder().setMapEntry(true).build()).build();
  }
}
