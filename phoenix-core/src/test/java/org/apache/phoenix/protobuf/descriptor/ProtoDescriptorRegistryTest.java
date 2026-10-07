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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.expression.function.ProtoFunctionsUtil;
import org.apache.phoenix.expression.util.proto.PathPlan;
import org.apache.phoenix.expression.util.proto.ProtoPathParser;
import org.apache.phoenix.expression.util.proto.ProtoPathResolver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.base.Ticker;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.DescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors;

public class ProtoDescriptorRegistryTest {
  private static final String NAME = "test.Account";
  private static final byte[] FIXTURE_BYTES = makeSingleMessageFileSet("test", "Account");

  private CountingLoader loader;
  private FakeTicker ticker;
  private Configuration conf;

  @Before
  public void setUp() {
    loader = new CountingLoader();
    ticker = new FakeTicker();
    conf = new Configuration(false);
  }

  @Test
  public void firstLookupCallsLoaderAndCachesResult() throws Exception {
    loader.setResponse(NAME, FIXTURE_BYTES);
    ProtoDescriptorRegistry registry = newRegistry();

    ProtoDescriptorRegistry.CachedDescriptor cached = registry.lookup(NAME);

    assertNotNull(cached);
    assertEquals(NAME, cached.getDescriptor().getFullName());
    assertEquals("Loader should be hit exactly once on first lookup", 1, loader.callCount(NAME));
  }

  @Test
  public void secondLookupReturnsCachedResultWithoutCallingLoader() throws Exception {
    loader.setResponse(NAME, FIXTURE_BYTES);
    ProtoDescriptorRegistry registry = newRegistry();

    ProtoDescriptorRegistry.CachedDescriptor first = registry.lookup(NAME);
    ProtoDescriptorRegistry.CachedDescriptor second = registry.lookup(NAME);

    assertSame("Cached lookups must return the same instance", first, second);
    assertEquals("Loader should NOT be re-invoked on cache hit", 1, loader.callCount(NAME));
  }

  @Test
  public void unregisteredNameReturnsNull() throws Exception {
    ProtoDescriptorRegistry registry = newRegistry();

    assertNull(registry.lookup("test.NotRegistered"));
    assertEquals(1, loader.callCount("test.NotRegistered"));

    assertNull(registry.lookup("test.NotRegistered"));
    assertEquals(2, loader.callCount("test.NotRegistered"));
  }

  @Test
  public void nullOrEmptyNameShortCircuitsToNullWithoutHittingLoader() throws Exception {
    ProtoDescriptorRegistry registry = newRegistry();

    assertNull(registry.lookup(null));
    assertNull(registry.lookup(""));
    assertEquals("Loader should never be invoked for null/empty names", 0, loader.totalCalls());
  }

  @Test
  public void invalidateClearsCacheEntryAndNextLookupReloads() throws Exception {
    loader.setResponse(NAME, FIXTURE_BYTES);
    ProtoDescriptorRegistry registry = newRegistry();

    registry.lookup(NAME);
    assertEquals(1, loader.callCount(NAME));

    registry.invalidate(NAME);

    registry.lookup(NAME);
    assertEquals("Loader should be re-invoked after invalidate", 2, loader.callCount(NAME));
  }

  @Test
  public void invalidateNullOrEmptyNameIsNoOp() throws Exception {
    loader.setResponse(NAME, FIXTURE_BYTES);
    ProtoDescriptorRegistry registry = newRegistry();
    registry.lookup(NAME);
    registry.invalidate(null);
    registry.invalidate("");
    registry.lookup(NAME);
    assertEquals("Existing entry must survive null/empty invalidate", 1, loader.callCount(NAME));
  }

  @Test
  public void invalidateMissingNameIsNoOp() throws Exception {
    loader.setResponse(NAME, FIXTURE_BYTES);
    ProtoDescriptorRegistry registry = newRegistry();
    registry.lookup(NAME);
    registry.invalidate("test.NeverCached");
    registry.lookup(NAME);
    assertEquals("Unrelated entry must survive invalidate of a missing name", 1,
      loader.callCount(NAME));
  }

  @Test
  public void tryForceRefreshEvictsEntryAndNextLookupReloads() throws Exception {
    loader.setResponse(NAME, FIXTURE_BYTES);
    ProtoDescriptorRegistry registry = newRegistry();

    ProtoDescriptorRegistry.CachedDescriptor first = registry.lookup(NAME);
    assertTrue(registry.tryForceRefresh(NAME));
    ProtoDescriptorRegistry.CachedDescriptor second = registry.lookup(NAME);

    assertEquals("Loader should be re-invoked after a forced refresh", 2, loader.callCount(NAME));
    assertTrue("Forced refresh must yield a fresh entry", first != second);
  }

  @Test
  public void tryForceRefreshIsRateLimitedPerName() throws Exception {
    loader.setResponse(NAME, FIXTURE_BYTES);
    conf.setLong(ProtoDescriptorRegistry.MIN_FORCED_REFRESH_INTERVAL_MS_ATTRIB, 5_000L);
    ProtoDescriptorRegistry registry = newRegistry();
    registry.lookup(NAME);

    assertTrue(registry.tryForceRefresh(NAME));
    registry.lookup(NAME);
    assertEquals(2, loader.callCount(NAME));

    assertTrue("Second refresh within the interval must be refused",
      !registry.tryForceRefresh(NAME));
    registry.lookup(NAME);
    assertEquals("Refused refresh must not evict the entry", 2, loader.callCount(NAME));

    assertTrue("Other names are not rate-limited by NAME", registry.tryForceRefresh("x.Y"));

    ticker.advanceMillis(5_001L);
    assertTrue("Refresh allowed again after the interval", registry.tryForceRefresh(NAME));
    registry.lookup(NAME);
    assertEquals(3, loader.callCount(NAME));
  }

  @Test
  public void descriptorRegisteredAfterMissIsVisibleOnNextLookup() throws Exception {
    ProtoDescriptorRegistry registry = newRegistry();
    assertNull(registry.lookup(NAME));

    loader.setResponse(NAME, FIXTURE_BYTES);
    assertNotNull(registry.lookup(NAME));
    assertEquals(2, loader.callCount(NAME));
  }

  @Test
  public void tryForceRefreshNullOrEmptyNameReturnsFalse() {
    ProtoDescriptorRegistry registry = newRegistry();
    assertTrue(!registry.tryForceRefresh(null));
    assertTrue(!registry.tryForceRefresh(""));
  }

  @Test
  public void refreshOnMissResolvesFieldAddedByAlterElsewhere() throws Exception {
    loader.setResponse(NAME, accountFileSet(false));
    ProtoDescriptorRegistry registry = newRegistry();
    // Warm the cache with a descriptor that has no 'email' field.
    assertNotNull(registry.lookup(NAME));

    // Another client ALTERs the descriptor to add 'email'. This JVM's entry is still within TTL.
    loader.setResponse(NAME, accountFileSet(true));

    PathPlan plan = ProtoFunctionsUtil.withDescriptorRefreshOnMiss(registry, NAME, "PROTO_VALUE",
      cached -> ProtoPathResolver.resolveForExistence(ProtoPathParser.parse("email"),
        cached.getDescriptor(), "email"));
    assertNotNull(plan);
    assertEquals(2, loader.callCount(NAME));
  }

  @Test
  public void refreshOnMissRethrowsWhenFieldGenuinelyMissing() throws Exception {
    loader.setResponse(NAME, accountFileSet(true));
    ProtoDescriptorRegistry registry = newRegistry();
    for (int attempt = 0; attempt < 3; attempt++) {
      try {
        ProtoFunctionsUtil.withDescriptorRefreshOnMiss(registry, NAME, "PROTO_VALUE",
          cached -> ProtoPathResolver.resolveForExistence(ProtoPathParser.parse("nope"),
            cached.getDescriptor(), "nope"));
        fail("Expected PROTOBUF_PATH_FIELD_NOT_FOUND");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.PROTOBUF_PATH_FIELD_NOT_FOUND.getErrorCode(),
          e.getErrorCode());
      }
    }
    assertEquals("Only the first failure may force a reload within the interval", 2,
      loader.callCount(NAME));
  }

  @Test
  public void refreshOnMissDoesNotReloadForNonStalenessErrors() throws Exception {
    loader.setResponse(NAME, accountFileSet(true));
    ProtoDescriptorRegistry registry = newRegistry();
    try {
      ProtoFunctionsUtil.withDescriptorRefreshOnMiss(registry, NAME, "PROTO_VALUE",
        cached -> ProtoPathResolver.resolveForExistence(ProtoPathParser.parse("email[0]"),
          cached.getDescriptor(), "email[0]"));
      fail("Expected cardinality error");
    } catch (SQLException e) {
      assertTrue(e.getErrorCode() != SQLExceptionCode.PROTOBUF_PATH_FIELD_NOT_FOUND.getErrorCode());
    }
    assertEquals(1, loader.callCount(NAME));
  }

  private static byte[] accountFileSet(boolean withEmail) {
    DescriptorProto.Builder msg =
      DescriptorProto.newBuilder().setName("Account").addField(FieldDescriptorProto.newBuilder()
        .setName("id").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_INT64));
    if (withEmail) {
      msg.addField(FieldDescriptorProto.newBuilder().setName("email").setNumber(2)
        .setType(FieldDescriptorProto.Type.TYPE_STRING));
    }
    return FileDescriptorSet.newBuilder().addFile(FileDescriptorProto.newBuilder()
      .setName("test.proto").setPackage("test").setSyntax("proto3").addMessageType(msg)).build()
      .toByteArray();
  }

  @Test
  public void cacheEntryExpiresAfterTtl() throws Exception {
    loader.setResponse(NAME, FIXTURE_BYTES);
    conf.setLong(ProtoDescriptorRegistry.CACHE_TTL_SECONDS_ATTRIB, 60L);
    ProtoDescriptorRegistry registry = newRegistry();

    registry.lookup(NAME);
    assertEquals(1, loader.callCount(NAME));

    ticker.advanceMillis(30_000L);
    registry.lookup(NAME);
    assertEquals("Lookup within TTL should not re-load", 1, loader.callCount(NAME));

    ticker.advanceMillis(31_000L);
    registry.lookup(NAME);
    assertEquals("Lookup past TTL should reload", 2, loader.callCount(NAME));
  }

  @Test
  public void ioExceptionFromLoaderIsWrappedAsPhoenixIOException() {
    loader.setThrowOn(NAME, new IOException("catalog unreachable"));
    ProtoDescriptorRegistry registry = newRegistry();

    try {
      registry.lookup(NAME);
      fail("Expected PhoenixIOException");
    } catch (PhoenixIOException e) {
      assertTrue("Cause should be the original IOException: " + e.getCause(),
        e.getCause() instanceof IOException);
      assertTrue(e.getCause().getMessage().contains("catalog unreachable"));
    }
  }

  @Test
  public void corruptBytesFromLoaderSurfaceAsPhoenixIOException() {
    loader.setResponse(NAME, new byte[] { (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef });
    ProtoDescriptorRegistry registry = newRegistry();

    try {
      registry.lookup(NAME);
      fail("Expected PhoenixIOException for corrupt bytes");
    } catch (PhoenixIOException e) {
      assertTrue("Cause should be IOException: " + e.getCause(),
        e.getCause() instanceof IOException);
    }
  }

  @Test
  public void lookupResolvesNestedMessageName() throws Exception {
    byte[] withNested =
      FileDescriptorSet.newBuilder().addFile(FileDescriptorProto.newBuilder().setName("o.proto")
        .setPackage("o").setSyntax("proto3").addMessageType(DescriptorProto.newBuilder()
          .setName("Order").addNestedType(DescriptorProto.newBuilder().setName("LineItem"))))
        .build().toByteArray();
    loader.setResponse("o.Order.LineItem", withNested);
    ProtoDescriptorRegistry registry = newRegistry();

    ProtoDescriptorRegistry.CachedDescriptor cached = registry.lookup("o.Order.LineItem");

    assertNotNull("Nested message must be looked up via the registry", cached);
    assertEquals("o.Order.LineItem", cached.getDescriptor().getFullName());
    assertEquals("LineItem", cached.getDescriptor().getName());
  }

  @Test
  public void bytesWithoutMatchingTopLevelMessageReturnsNull() throws Exception {
    loader.setResponse("test.MismatchedName", makeSingleMessageFileSet("test", "OtherName"));
    ProtoDescriptorRegistry registry = newRegistry();

    assertNull(registry.lookup("test.MismatchedName"));
    assertEquals(1, loader.callCount("test.MismatchedName"));
  }

  @Test
  public void negativeMaxSizeConfigFallsBackToDefault() throws Exception {
    loader.setResponse(NAME, FIXTURE_BYTES);
    conf.setLong(ProtoDescriptorRegistry.CACHE_MAX_SIZE_ATTRIB, -1L);
    ProtoDescriptorRegistry registry = newRegistry();

    assertNotNull(registry.lookup(NAME));
  }

  @Test
  public void zeroTtlConfigFallsBackToDefault() throws Exception {
    loader.setResponse(NAME, FIXTURE_BYTES);
    conf.setLong(ProtoDescriptorRegistry.CACHE_TTL_SECONDS_ATTRIB, 0L);
    ProtoDescriptorRegistry registry = newRegistry();

    registry.lookup(NAME);
    registry.lookup(NAME);
    assertEquals("Zero TTL config falls back to default — entry stays cached", 1,
      loader.callCount(NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullLoaderRejected() {
    new ProtoDescriptorRegistry(conf, null);
  }

  private static final String ACMEWIDGETS_BUNDLE_RESOURCE =
    "protobuf/acmewidgets/acmewidgets_v1.desc";

  @Test
  public void lookupTopLevelInRealBundle() throws Exception {
    BundleBackedLoader bundleLoader = new BundleBackedLoader(loadAcmeWidgetsBundle());
    ProtoDescriptorRegistry registry =
      new ProtoDescriptorRegistry(conf, bundleLoader, new ProtoDescriptorResolver(), ticker);

    ProtoDescriptorRegistry.CachedDescriptor account =
      registry.lookup("com.example.acmewidgets.account.v1.Account");
    assertNotNull(account);
    assertEquals("com.example.acmewidgets.account.v1.Account",
      account.getDescriptor().getFullName());
    assertEquals("Account", account.getDescriptor().getName());
  }

  @Test
  public void lookupNestedMessagesInRealBundle() throws Exception {
    BundleBackedLoader bundleLoader = new BundleBackedLoader(loadAcmeWidgetsBundle());
    ProtoDescriptorRegistry registry =
      new ProtoDescriptorRegistry(conf, bundleLoader, new ProtoDescriptorResolver(), ticker);

    List<String> nestedNames = Arrays.asList(
      "com.example.acmewidgets.governance.v1.endpointtypes.CatalogQuery.NamedCatalogConnection",
      "com.example.acmewidgets.governance.v1.endpointtypes.CatalogQuery.QueryOp",
      "com.example.acmewidgets.governance.v1.endpointtypes.BulkOrder.NamedCatalogConnectionRef",
      "com.example.acmewidgets.governance.v1.endpointtypes.BulkOrder.Key",
      "com.example.acmewidgets.governance.v1.endpointtypes.FlashSale.PromotionId",
      "com.example.acmewidgets.governance.v1.endpointtypes.FlashSale.Key");
    for (String name : nestedNames) {
      ProtoDescriptorRegistry.CachedDescriptor cached = registry.lookup(name);
      assertNotNull("Registry must look up nested type '" + name + "'", cached);
      assertEquals(name, cached.getDescriptor().getFullName());
    }

    for (String name : nestedNames) {
      assertEquals("Loader should be hit exactly once for " + name, 1,
        bundleLoader.callCount(name));
    }
  }

  @Test
  public void lookupRealBundleCachesEachNameIndependently() throws Exception {
    BundleBackedLoader bundleLoader = new BundleBackedLoader(loadAcmeWidgetsBundle());
    ProtoDescriptorRegistry registry =
      new ProtoDescriptorRegistry(conf, bundleLoader, new ProtoDescriptorResolver(), ticker);

    String a = "com.example.acmewidgets.account.v1.Account";
    String b = "com.example.acmewidgets.billing.v1.Entitlement";
    registry.lookup(a);
    registry.lookup(b);
    registry.lookup(a);
    registry.lookup(b);
    registry.lookup(a);

    assertEquals("First name loaded exactly once, subsequent hits cached", 1,
      bundleLoader.callCount(a));
    assertEquals("Second name loaded exactly once, subsequent hits cached", 1,
      bundleLoader.callCount(b));
  }

  @Test
  public void lookupRealBundleCrossFileFieldReferencesAreWiredUp() throws Exception {
    BundleBackedLoader bundleLoader = new BundleBackedLoader(loadAcmeWidgetsBundle());
    ProtoDescriptorRegistry registry =
      new ProtoDescriptorRegistry(conf, bundleLoader, new ProtoDescriptorResolver(), ticker);

    ProtoDescriptorRegistry.CachedDescriptor entitlement =
      registry.lookup("com.example.acmewidgets.billing.v1.Entitlement");
    assertNotNull(entitlement);
    Descriptors.FieldDescriptor deletedTime =
      entitlement.getDescriptor().findFieldByName("deleted_time");
    assertNotNull(deletedTime);
    assertEquals("google.protobuf.Timestamp", deletedTime.getMessageType().getFullName());

    Descriptors.FieldDescriptor policyToggles =
      entitlement.getDescriptor().findFieldByName("policy_toggles");
    assertNotNull(policyToggles);
    assertTrue(policyToggles.isMapField());
    Descriptors.FieldDescriptor mapValue = policyToggles.getMessageType().findFieldByName("value");
    assertNotNull(mapValue);
    assertEquals("com.example.acmewidgets.billing.v1.PolicyToggle",
      mapValue.getMessageType().getFullName());
  }

  @Test
  public void lookupRealBundleProto2AndOneofCoexist() throws Exception {
    BundleBackedLoader bundleLoader = new BundleBackedLoader(loadAcmeWidgetsBundle());
    ProtoDescriptorRegistry registry =
      new ProtoDescriptorRegistry(conf, bundleLoader, new ProtoDescriptorResolver(), ticker);

    ProtoDescriptorRegistry.CachedDescriptor proto2Token =
      registry.lookup("com.example.acmewidgets.auth.v1.proto2.TokenRevocation");
    assertNotNull(proto2Token);

    String syntax = proto2Token.getDescriptor().getFile().toProto().hasSyntax()
      ? proto2Token.getDescriptor().getFile().toProto().getSyntax()
      : "proto2";
    assertEquals("proto2", syntax);

    assertNotNull(proto2Token.getDescriptor().findFieldByName("tenant_id"));
    assertNotNull(proto2Token.getDescriptor().findFieldByName("token_id"));

    ProtoDescriptorRegistry.CachedDescriptor widgetEndpoint =
      registry.lookup("com.example.acmewidgets.governance.v1.WidgetEndpoint");
    assertNotNull(widgetEndpoint);
    Descriptors.OneofDescriptor endpointKind =
      widgetEndpoint.getDescriptor().findFieldByName("catalog_query").getContainingOneof();
    assertEquals("endpoint_kind", endpointKind.getName());
    assertEquals("Oneof must resolve all 4 cross-file arms", 4, endpointKind.getFieldCount());
    for (Descriptors.FieldDescriptor arm : endpointKind.getFields()) {
      assertTrue(
        "Every arm must resolve to widget_endpoint_types.proto — got "
          + arm.getMessageType().getFullName(),
        arm.getMessageType().getFullName()
          .startsWith("com.example.acmewidgets.governance.v1.endpointtypes."));
    }
  }

  private static final String ORDERS_PKG = "com.example.acmewidgets.orders.v1";

  @Test
  public void lookupDeepFixtureFullFiveLevelWalk() throws Exception {
    BundleBackedLoader bundleLoader = new BundleBackedLoader(loadAcmeWidgetsBundle());
    ProtoDescriptorRegistry registry =
      new ProtoDescriptorRegistry(conf, bundleLoader, new ProtoDescriptorResolver(), ticker);

    ProtoDescriptorRegistry.CachedDescriptor order = registry.lookup(ORDERS_PKG + ".Order");
    assertNotNull(order);

    Descriptors.FieldDescriptor customer = order.getDescriptor().findFieldByName("customer");
    Descriptors.FieldDescriptor contact = customer.getMessageType().findFieldByName("contact");
    Descriptors.FieldDescriptor addresses = contact.getMessageType().findFieldByName("addresses");
    Descriptors.FieldDescriptor geo = addresses.getMessageType().findFieldByName("geo");
    Descriptors.FieldDescriptor latitude = geo.getMessageType().findFieldByName("latitude");
    assertEquals(Descriptors.FieldDescriptor.Type.DOUBLE, latitude.getType());

    assertEquals(
      "Deep field walk must not trigger extra loader calls beyond the one top-level " + "lookup", 1,
      bundleLoader.callCount(ORDERS_PKG + ".Order"));
  }

  @Test
  public void lookupDeepFixtureRecursiveWalkThroughSubModifiers() throws Exception {
    BundleBackedLoader bundleLoader = new BundleBackedLoader(loadAcmeWidgetsBundle());
    ProtoDescriptorRegistry registry =
      new ProtoDescriptorRegistry(conf, bundleLoader, new ProtoDescriptorResolver(), ticker);

    ProtoDescriptorRegistry.CachedDescriptor lineItem = registry.lookup(ORDERS_PKG + ".LineItem");
    assertNotNull(lineItem);
    Descriptors.FieldDescriptor modifiers = lineItem.getDescriptor().findFieldByName("modifiers");
    Descriptors.Descriptor level1Type = modifiers.getMessageType();
    Descriptors.Descriptor level2Type =
      level1Type.findFieldByName("sub_modifiers").getMessageType();
    Descriptors.Descriptor level3Type =
      level2Type.findFieldByName("sub_modifiers").getMessageType();

    assertSame("Recursive descent must resolve to the same Modifier descriptor", level1Type,
      level2Type);
    assertSame(level2Type, level3Type);

    Descriptors.FieldDescriptor amount = level3Type.findFieldByName("amount");
    assertEquals(ORDERS_PKG + ".Money", amount.getMessageType().getFullName());
    assertEquals(Descriptors.FieldDescriptor.Type.STRING,
      amount.getMessageType().findFieldByName("currency_code").getType());
  }

  @Test
  public void lookupDeepFixtureOneofPathWalkAllArms() throws Exception {
    BundleBackedLoader bundleLoader = new BundleBackedLoader(loadAcmeWidgetsBundle());
    ProtoDescriptorRegistry registry =
      new ProtoDescriptorRegistry(conf, bundleLoader, new ProtoDescriptorResolver(), ticker);

    ProtoDescriptorRegistry.CachedDescriptor payment =
      registry.lookup(ORDERS_PKG + ".PaymentMethod");
    assertNotNull(payment);
    String[][] armExpectations = { { "credit_card", ORDERS_PKG + ".PaymentMethod.CreditCard" },
      { "bank_account", ORDERS_PKG + ".PaymentMethod.BankAccount" },
      { "digital_wallet", ORDERS_PKG + ".PaymentMethod.DigitalWallet" } };
    for (String[] pair : armExpectations) {
      Descriptors.FieldDescriptor arm = payment.getDescriptor().findFieldByName(pair[0]);
      assertNotNull("Oneof arm " + pair[0] + " must exist", arm);
      assertEquals(pair[1], arm.getMessageType().getFullName());
    }
  }

  @Test
  public void lookupDeepFixtureMapOfMessageWalkThroughRegistry() throws Exception {
    BundleBackedLoader bundleLoader = new BundleBackedLoader(loadAcmeWidgetsBundle());
    ProtoDescriptorRegistry registry =
      new ProtoDescriptorRegistry(conf, bundleLoader, new ProtoDescriptorResolver(), ticker);

    ProtoDescriptorRegistry.CachedDescriptor lineItem = registry.lookup(ORDERS_PKG + ".LineItem");
    assertNotNull(lineItem);
    Descriptors.FieldDescriptor regional =
      lineItem.getDescriptor().findFieldByName("regional_prices");
    assertTrue(regional.isMapField());
    Descriptors.FieldDescriptor value = regional.getMessageType().findFieldByName("value");
    assertEquals(ORDERS_PKG + ".Money", value.getMessageType().getFullName());

    assertEquals(Descriptors.FieldDescriptor.Type.INT64,
      value.getMessageType().findFieldByName("units").getType());
  }

  @Test
  public void lookupNestedArraysThreeLevelChainSingleLoaderCall() throws Exception {
    BundleBackedLoader bundleLoader = new BundleBackedLoader(loadAcmeWidgetsBundle());
    ProtoDescriptorRegistry registry =
      new ProtoDescriptorRegistry(conf, bundleLoader, new ProtoDescriptorResolver(), ticker);

    ProtoDescriptorRegistry.CachedDescriptor order = registry.lookup(ORDERS_PKG + ".Order");
    assertNotNull(order);
    Descriptors.FieldDescriptor level1 = order.getDescriptor().findFieldByName("line_items");
    Descriptors.FieldDescriptor level2 = level1.getMessageType().findFieldByName("modifiers");
    Descriptors.FieldDescriptor level3 = level2.getMessageType().findFieldByName("sub_modifiers");

    assertTrue("Every level of the nested-array chain must be repeated",
      level1.isRepeated() && level2.isRepeated() && level3.isRepeated());
    assertEquals(ORDERS_PKG + ".LineItem", level1.getMessageType().getFullName());
    assertEquals(ORDERS_PKG + ".LineItem.Modifier", level2.getMessageType().getFullName());
    assertEquals(ORDERS_PKG + ".LineItem.Modifier", level3.getMessageType().getFullName());
    assertEquals("Deep field walk across 3 nested arrays must not trigger extra loader calls "
      + "beyond the one top-level lookup", 1, bundleLoader.callCount(ORDERS_PKG + ".Order"));
  }

  @Test
  public void lookupNestedArraysThroughMapValueEndToEnd() throws Exception {
    BundleBackedLoader bundleLoader = new BundleBackedLoader(loadAcmeWidgetsBundle());
    ProtoDescriptorRegistry registry =
      new ProtoDescriptorRegistry(conf, bundleLoader, new ProtoDescriptorResolver(), ticker);

    ProtoDescriptorRegistry.CachedDescriptor order = registry.lookup(ORDERS_PKG + ".Order");
    Descriptors.FieldDescriptor lineItems = order.getDescriptor().findFieldByName("line_items");
    assertTrue(lineItems.isRepeated());
    Descriptors.FieldDescriptor namedMods =
      lineItems.getMessageType().findFieldByName("named_modifiers");
    assertTrue(namedMods.isMapField());
    Descriptors.Descriptor mapValueType =
      namedMods.getMessageType().findFieldByName("value").getMessageType();
    assertEquals(ORDERS_PKG + ".LineItem.Modifier", mapValueType.getFullName());

    Descriptors.FieldDescriptor subMods = mapValueType.findFieldByName("sub_modifiers");
    assertTrue("Array following map-value chain must remain repeated", subMods.isRepeated());
    assertEquals(1, bundleLoader.callCount(ORDERS_PKG + ".Order"));
  }

  @Test
  public void lookupNestedArraysAcrossSingularBridgeEndToEnd() throws Exception {
    BundleBackedLoader bundleLoader = new BundleBackedLoader(loadAcmeWidgetsBundle());
    ProtoDescriptorRegistry registry =
      new ProtoDescriptorRegistry(conf, bundleLoader, new ProtoDescriptorResolver(), ticker);

    ProtoDescriptorRegistry.CachedDescriptor order = registry.lookup(ORDERS_PKG + ".Order");
    Descriptors.FieldDescriptor lineItems = order.getDescriptor().findFieldByName("line_items");
    Descriptors.FieldDescriptor category = lineItems.getMessageType().findFieldByName("category");
    Descriptors.FieldDescriptor children = category.getMessageType().findFieldByName("children");

    assertTrue("line_items is repeated", lineItems.isRepeated());
    assertTrue("category is the singular bridge (NOT repeated)", !category.isRepeated());
    assertTrue("children (recursive-tree bottom) is repeated", children.isRepeated());

    assertEquals(ORDERS_PKG + ".Category", children.getMessageType().getFullName());
    assertSame(
      "Descriptor identity check: category.getMessageType() and "
        + "children.getMessageType() must be the same Category Descriptor instance",
      category.getMessageType(), children.getMessageType());
    assertEquals(1, bundleLoader.callCount(ORDERS_PKG + ".Order"));
  }

  @Test
  public void lookupDeepFixtureNestedTypeIsCachedIndependentlyFromOuter() throws Exception {
    BundleBackedLoader bundleLoader = new BundleBackedLoader(loadAcmeWidgetsBundle());
    ProtoDescriptorRegistry registry =
      new ProtoDescriptorRegistry(conf, bundleLoader, new ProtoDescriptorResolver(), ticker);

    registry.lookup(ORDERS_PKG + ".Order");
    registry.lookup(ORDERS_PKG + ".Order.Event");

    ProtoDescriptorRegistry.CachedDescriptor outer = registry.lookup(ORDERS_PKG + ".Order");
    ProtoDescriptorRegistry.CachedDescriptor nested = registry.lookup(ORDERS_PKG + ".Order.Event");
    assertEquals("Outer cached independently", 1, bundleLoader.callCount(ORDERS_PKG + ".Order"));
    assertEquals("Nested cached independently", 1,
      bundleLoader.callCount(ORDERS_PKG + ".Order.Event"));
    Assert.assertNotSame(outer.getDescriptor(), nested.getDescriptor());
    assertEquals("Order", outer.getDescriptor().getName());
    assertEquals("Event", nested.getDescriptor().getName());
  }

  @Test
  public void lookupMissingNameInRealBundleReturnsNullWithoutCaching() throws Exception {
    BundleBackedLoader bundleLoader = new BundleBackedLoader(loadAcmeWidgetsBundle());
    ProtoDescriptorRegistry registry =
      new ProtoDescriptorRegistry(conf, bundleLoader, new ProtoDescriptorResolver(), ticker);

    String bogus = "com.example.acmewidgets.account.v1.NoSuchMessage";
    assertNull(registry.lookup(bogus));
    assertNull(registry.lookup(bogus));
    assertNull(registry.lookup(bogus));
    assertEquals(3, bundleLoader.callCount(bogus));
  }

  private ProtoDescriptorRegistry newRegistry() {
    return new ProtoDescriptorRegistry(conf, loader, new ProtoDescriptorResolver(), ticker);
  }

  private static byte[] makeSingleMessageFileSet(String pkg, String message) {
    return FileDescriptorSet.newBuilder()
      .addFile(FileDescriptorProto.newBuilder().setName(pkg + ".proto").setPackage(pkg)
        .setSyntax("proto3").addMessageType(DescriptorProto.newBuilder().setName(message)))
      .build().toByteArray();
  }

  private static byte[] loadAcmeWidgetsBundle() throws IOException {
    ClassLoader cl = ProtoDescriptorRegistryTest.class.getClassLoader();
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

  private static final class BundleBackedLoader implements ProtoDescriptorRegistry.Loader {
    private final byte[] bundleBytes;
    private final Map<String, AtomicInteger> callCounts = new HashMap<>();

    BundleBackedLoader(byte[] bundleBytes) {
      this.bundleBytes = bundleBytes;
    }

    int callCount(String name) {
      AtomicInteger c = callCounts.get(name);
      return c == null ? 0 : c.get();
    }

    @Override
    public byte[] load(String fullyQualifiedMessageName) {
      callCounts.computeIfAbsent(fullyQualifiedMessageName, k -> new AtomicInteger())
        .incrementAndGet();
      return bundleBytes;
    }
  }

  private static final class CountingLoader implements ProtoDescriptorRegistry.Loader {
    private final Map<String, byte[]> responses = new HashMap<>();
    private final Map<String, IOException> throwables = new HashMap<>();
    private final Map<String, AtomicInteger> callCounts = new HashMap<>();

    void setResponse(String name, byte[] bytes) {
      responses.put(name, bytes);
    }

    void setThrowOn(String name, IOException ex) {
      throwables.put(name, ex);
    }

    int callCount(String name) {
      AtomicInteger c = callCounts.get(name);
      return c == null ? 0 : c.get();
    }

    int totalCalls() {
      int total = 0;
      for (AtomicInteger c : callCounts.values()) {
        total += c.get();
      }
      return total;
    }

    @Override
    public byte[] load(String fullyQualifiedMessageName) throws IOException {
      callCounts.computeIfAbsent(fullyQualifiedMessageName, k -> new AtomicInteger())
        .incrementAndGet();
      IOException toThrow = throwables.get(fullyQualifiedMessageName);
      if (toThrow != null) {
        throw toThrow;
      }
      return responses.get(fullyQualifiedMessageName);
    }
  }

  private static final class FakeTicker extends Ticker {
    private long nanos = 0L;

    void advanceMillis(long millis) {
      nanos += TimeUnit.MILLISECONDS.toNanos(millis);
    }

    @Override
    public long read() {
      return nanos;
    }
  }
}
