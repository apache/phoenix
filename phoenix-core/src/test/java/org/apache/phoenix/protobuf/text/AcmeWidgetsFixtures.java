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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.Descriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.DescriptorValidationException;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.phoenix.thirdparty.com.google.protobuf.DynamicMessage;

public final class AcmeWidgetsFixtures {

  public static final String BUNDLE_RESOURCE = "protobuf/acmewidgets/acmewidgets_v1.desc";

  private AcmeWidgetsFixtures() {
  }

  public static String bundleFileUrl() {
    URL url = AcmeWidgetsFixtures.class.getClassLoader().getResource(BUNDLE_RESOURCE);
    if (url == null) {
      throw new IllegalStateException(BUNDLE_RESOURCE + " not on the test classpath");
    }
    return url.toString();
  }

  public static FileDescriptorSet loadBundle() throws IOException {
    ClassLoader cl = AcmeWidgetsFixtures.class.getClassLoader();
    try (InputStream in = cl.getResourceAsStream(BUNDLE_RESOURCE)) {
      if (in == null) {
        throw new IOException(BUNDLE_RESOURCE + " not on the test classpath");
      }
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[8192];
      int n;
      while ((n = in.read(buf)) > 0) {
        baos.write(buf, 0, n);
      }
      return FileDescriptorSet.parseFrom(baos.toByteArray());
    }
  }

  public static Map<String, FileDescriptor> buildAllFileDescriptors(FileDescriptorSet set)
    throws DescriptorValidationException {
    Map<String, FileDescriptor> byName = new HashMap<>();
    for (FileDescriptorProto fileProto : set.getFileList()) {
      FileDescriptor[] deps = new FileDescriptor[fileProto.getDependencyCount()];
      for (int i = 0; i < fileProto.getDependencyCount(); i++) {
        FileDescriptor dep = byName.get(fileProto.getDependency(i));
        if (dep == null) {
          throw new IllegalStateException(fileProto.getName() + " imports "
            + fileProto.getDependency(i) + " but dependency wasn't in the bundle set");
        }
        deps[i] = dep;
      }
      byName.put(fileProto.getName(), FileDescriptor.buildFrom(fileProto, deps));
    }
    return byName;
  }

  public static Map<String, FileDescriptor> loadAndBuildDescriptorGraph()
    throws IOException, DescriptorValidationException {
    return buildAllFileDescriptors(loadBundle());
  }

  public static Descriptor accountDescriptor(Map<String, FileDescriptor> graph) {
    return requireMessage(graph, "account_sync.proto", "Account");
  }

  public static Descriptor widgetEndpointDescriptor(Map<String, FileDescriptor> graph) {
    return requireMessage(graph, "widget_governance.proto", "WidgetEndpoint");
  }

  public static Descriptor catalogQueryDescriptor(Map<String, FileDescriptor> graph) {
    return requireMessage(graph, "widget_endpoint_types.proto", "CatalogQuery");
  }

  public static Descriptor entitlementDescriptor(Map<String, FileDescriptor> graph) {
    return requireMessage(graph, "entitlement_sync.proto", "Entitlement");
  }

  public static Descriptor entitlementKeyDescriptor(Map<String, FileDescriptor> graph) {
    return requireMessage(graph, "entitlement_sync.proto", "EntitlementKey");
  }

  public static Descriptor entitlementVersionDescriptor(Map<String, FileDescriptor> graph) {
    return requireMessage(graph, "entitlement_sync.proto", "EntitlementVersion");
  }

  public static Descriptor policyToggleDescriptor(Map<String, FileDescriptor> graph) {
    return requireMessage(graph, "entitlement_sync.proto", "PolicyToggle");
  }

  public static Descriptor tokenRevocationDescriptor(Map<String, FileDescriptor> graph) {
    return requireMessage(graph, "voucher_revocation_sync.proto", "TokenRevocation");
  }

  public static Descriptor orderDescriptor(Map<String, FileDescriptor> graph) {
    return requireMessage(graph, "widget_order_graph.proto", "Order");
  }

  public static Descriptor lineItemDescriptor(Map<String, FileDescriptor> graph) {
    return requireMessage(graph, "widget_order_graph.proto", "LineItem");
  }

  public static Descriptor moneyDescriptor(Map<String, FileDescriptor> graph) {
    return requireMessage(graph, "widget_order_graph.proto", "Money");
  }

  public static Descriptor customerDescriptor(Map<String, FileDescriptor> graph) {
    return requireMessage(graph, "widget_order_graph.proto", "Customer");
  }

  private static Descriptor requireMessage(Map<String, FileDescriptor> graph, String file,
    String name) {
    FileDescriptor fd = graph.get(file);
    if (fd == null) {
      throw new IllegalStateException(file + " not in descriptor graph");
    }
    Descriptor d = fd.findMessageTypeByName(name);
    if (d == null) {
      throw new IllegalStateException(name + " not in " + file);
    }
    return d;
  }

  public static DynamicMessage newAccount(Descriptor account, String tenantKey, String displayName,
    boolean active, String homeWarehouse) {
    return DynamicMessage.newBuilder(account)
      .setField(account.findFieldByName("tenant_key"), tenantKey)
      .setField(account.findFieldByName("display_name"), displayName)
      .setField(account.findFieldByName("active"), active)
      .setField(account.findFieldByName("home_warehouse"), homeWarehouse).build();
  }

  public static DynamicMessage newWidgetEndpointWithCatalogQueryArm(Descriptor widgetEndpoint,
    Descriptor catalogQuery, String tenantKey, String name) {
    return DynamicMessage.newBuilder(widgetEndpoint)
      .setField(widgetEndpoint.findFieldByName("tenant_key"), tenantKey)
      .setField(widgetEndpoint.findFieldByName("name"), name)
      .setField(widgetEndpoint.findFieldByName("catalog_query"),
        DynamicMessage.newBuilder(catalogQuery).build())
      .build();
  }

  public static DynamicMessage newEntitlement(Descriptor entitlement, Descriptor entitlementKey,
    Descriptor entitlementVersion, Descriptor policyToggle, String tenantKey,
    String entitlementName, String entitlementId, long version,
    Map<String, Boolean> policyToggles) {
    DynamicMessage key = DynamicMessage.newBuilder(entitlementKey)
      .setField(entitlementKey.findFieldByName("entitlement_name"), entitlementName)
      .setField(entitlementKey.findFieldByName("tenant_key"), tenantKey)
      .setField(entitlementKey.findFieldByName("entitlement_id"), entitlementId).build();
    DynamicMessage ver = DynamicMessage.newBuilder(entitlementVersion)
      .setField(entitlementVersion.findFieldByName("version"), version).build();
    FieldDescriptor policyTogglesField = entitlement.findFieldByName("policy_toggles");
    Descriptor entryType = policyTogglesField.getMessageType();
    DynamicMessage.Builder builder =
      DynamicMessage.newBuilder(entitlement).setField(entitlement.findFieldByName("key"), key)
        .setField(entitlement.findFieldByName("version"), ver);
    for (Map.Entry<String, Boolean> e : policyToggles.entrySet()) {
      DynamicMessage toggle = DynamicMessage.newBuilder(policyToggle)
        .setField(policyToggle.findFieldByName("toggle_name"), e.getKey())
        .setField(policyToggle.findFieldByName("enabled"), e.getValue()).build();
      DynamicMessage mapEntry =
        DynamicMessage.newBuilder(entryType).setField(entryType.findFieldByName("key"), e.getKey())
          .setField(entryType.findFieldByName("value"), toggle).build();
      builder.addRepeatedField(policyTogglesField, mapEntry);
    }
    return builder.build();
  }

  public static DynamicMessage newTokenRevocation(Descriptor tokenRevocation, String tenantId,
    String tokenId, String reason) {
    return DynamicMessage.newBuilder(tokenRevocation)
      .setField(tokenRevocation.findFieldByName("tenant_id"), tenantId)
      .setField(tokenRevocation.findFieldByName("token_id"), tokenId)
      .setField(tokenRevocation.findFieldByName("reason"), reason).build();
  }

  public static DynamicMessage newTokenRevocationWithReasonUnset(Descriptor tokenRevocation,
    String tenantId, String tokenId) {
    return DynamicMessage.newBuilder(tokenRevocation)
      .setField(tokenRevocation.findFieldByName("tenant_id"), tenantId)
      .setField(tokenRevocation.findFieldByName("token_id"), tokenId).build();
  }

  /**
   * Richly populated {@code Order} for path-walker tests. See
   * {@link #newOrderRichlyPopulatedSeeded} for the leaf-to-parameter map.
   */
  public static DynamicMessage newOrderRichlyPopulated(Map<String, FileDescriptor> graph,
    String orderId, String customerId) {
    return newOrderRichlyPopulatedSeeded(graph, orderId, customerId, 42L, 5L, 37.7749d, 2,
      "Sub Modifier Alpha", "Root Category");
  }

  /**
   * Seeded Order for multi-row ITs. Parameters map to line_total.units,
   * sub_modifiers[0].amount.units, geo.latitude, quantity, holiday display_name, and
   * category.parent.parent.name.
   */
  public static DynamicMessage newOrderRichlyPopulatedSeeded(Map<String, FileDescriptor> graph,
    String orderId, String customerId, long lineTotalUnits, long subModifierAmountUnits,
    double latitude, int quantity, String subModifierDisplayName, String categoryRootName) {
    Descriptor order = orderDescriptor(graph);
    Descriptor customer = customerDescriptor(graph);
    Descriptor contactInfo = requireMessage(graph, "widget_order_graph.proto", "ContactInfo");
    Descriptor address = contactInfo.findNestedTypeByName("Address");
    Descriptor geoPoint = address.findNestedTypeByName("GeoPoint");
    Descriptor lineItem = lineItemDescriptor(graph);
    Descriptor modifier = lineItem.findNestedTypeByName("Modifier");
    Descriptor money = moneyDescriptor(graph);
    Descriptor category = requireMessage(graph, "widget_order_graph.proto", "Category");

    DynamicMessage geo =
      DynamicMessage.newBuilder(geoPoint).setField(geoPoint.findFieldByName("latitude"), latitude)
        .setField(geoPoint.findFieldByName("longitude"), -122.4194d).build();
    DynamicMessage addr =
      DynamicMessage.newBuilder(address).setField(address.findFieldByName("street"), "1 Market St")
        .setField(address.findFieldByName("city"), "San Francisco")
        .setField(address.findFieldByName("region"), "CA")
        .setField(address.findFieldByName("postal_code"), "94103")
        .setField(address.findFieldByName("country_code"), "US")
        .setField(address.findFieldByName("geo"), geo).build();
    DynamicMessage contact = DynamicMessage.newBuilder(contactInfo)
      .setField(contactInfo.findFieldByName("email"), "buyer@example.com")
      .setField(contactInfo.findFieldByName("phone"), "+1-555-0100")
      .addRepeatedField(contactInfo.findFieldByName("addresses"), addr).build();
    DynamicMessage cust = DynamicMessage.newBuilder(customer)
      .setField(customer.findFieldByName("customer_id"), customerId)
      .setField(customer.findFieldByName("display_name"), "Deep Customer")
      .setField(customer.findFieldByName("contact"), contact).build();

    DynamicMessage lineTotal =
      DynamicMessage.newBuilder(money).setField(money.findFieldByName("currency_code"), "USD")
        .setField(money.findFieldByName("units"), lineTotalUnits)
        .setField(money.findFieldByName("nanos"), 500000000).build();
    DynamicMessage regionalPriceUS =
      DynamicMessage.newBuilder(money).setField(money.findFieldByName("currency_code"), "USD")
        .setField(money.findFieldByName("units"), 40L).setField(money.findFieldByName("nanos"), 0)
        .build();

    // sub_modifiers[0] used by both modifiers[0] and named_modifiers["holiday"]
    DynamicMessage subAmount =
      DynamicMessage.newBuilder(money).setField(money.findFieldByName("currency_code"), "USD")
        .setField(money.findFieldByName("units"), subModifierAmountUnits)
        .setField(money.findFieldByName("nanos"), 0).build();
    DynamicMessage subModifier =
      DynamicMessage.newBuilder(modifier).setField(modifier.findFieldByName("modifier_id"), "sub-1")
        .setField(modifier.findFieldByName("display_name"), subModifierDisplayName)
        .setField(modifier.findFieldByName("amount"), subAmount).build();
    DynamicMessage topModifier =
      DynamicMessage.newBuilder(modifier).setField(modifier.findFieldByName("modifier_id"), "mod-1")
        .setField(modifier.findFieldByName("display_name"), "Top Modifier")
        .addRepeatedField(modifier.findFieldByName("sub_modifiers"), subModifier).build();

    // Category self-reference: leaf -> parent -> parent
    DynamicMessage grandparentCat =
      DynamicMessage.newBuilder(category).setField(category.findFieldByName("id"), "cat-root")
        .setField(category.findFieldByName("name"), categoryRootName).build();
    DynamicMessage parentCat =
      DynamicMessage.newBuilder(category).setField(category.findFieldByName("id"), "cat-mid")
        .setField(category.findFieldByName("name"), "Middle Category")
        .setField(category.findFieldByName("parent"), grandparentCat).build();
    DynamicMessage leafCat =
      DynamicMessage.newBuilder(category).setField(category.findFieldByName("id"), "cat-leaf")
        .setField(category.findFieldByName("name"), "Leaf Category")
        .setField(category.findFieldByName("parent"), parentCat).build();

    FieldDescriptor regionalPricesField = lineItem.findFieldByName("regional_prices");
    Descriptor regionalEntry = regionalPricesField.getMessageType();
    DynamicMessage regionalEntryUs =
      DynamicMessage.newBuilder(regionalEntry).setField(regionalEntry.findFieldByName("key"), "US")
        .setField(regionalEntry.findFieldByName("value"), regionalPriceUS).build();

    FieldDescriptor namedModifiersField = lineItem.findFieldByName("named_modifiers");
    Descriptor namedEntry = namedModifiersField.getMessageType();
    DynamicMessage namedEntryHoliday =
      DynamicMessage.newBuilder(namedEntry).setField(namedEntry.findFieldByName("key"), "holiday")
        .setField(namedEntry.findFieldByName("value"), topModifier).build();

    DynamicMessage li =
      DynamicMessage.newBuilder(lineItem).setField(lineItem.findFieldByName("line_id"), "line-1")
        .setField(lineItem.findFieldByName("sku"), "SKU-001")
        .setField(lineItem.findFieldByName("category"), leafCat)
        .setField(lineItem.findFieldByName("quantity"), quantity)
        .setField(lineItem.findFieldByName("line_total"), lineTotal)
        .addRepeatedField(regionalPricesField, regionalEntryUs)
        .addRepeatedField(lineItem.findFieldByName("modifiers"), topModifier)
        .addRepeatedField(namedModifiersField, namedEntryHoliday).build();

    return DynamicMessage.newBuilder(order).setField(order.findFieldByName("order_id"), orderId)
      .setField(order.findFieldByName("customer"), cust)
      .addRepeatedField(order.findFieldByName("line_items"), li).build();
  }

  public static DynamicMessage newTokenRevocationWithPartnerSourceExtension(
    Descriptor tokenRevocation, FileDescriptor voucherFile, String tenantId, String tokenId,
    String partnerSource) {
    FieldDescriptor ext = null;
    List<FieldDescriptor> extensions = voucherFile.getExtensions();
    for (FieldDescriptor candidate : extensions) {
      if ("partner_revocation_source".equals(candidate.getName())) {
        ext = candidate;
        break;
      }
    }
    if (ext == null) {
      throw new IllegalStateException(
        "partner_revocation_source extension not declared in voucher_revocation_sync.proto");
    }
    return DynamicMessage.newBuilder(tokenRevocation)
      .setField(tokenRevocation.findFieldByName("tenant_id"), tenantId)
      .setField(tokenRevocation.findFieldByName("token_id"), tokenId).setField(ext, partnerSource)
      .build();
  }
}
