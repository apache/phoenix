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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.expression.util.proto.PathPlan;
import org.apache.phoenix.schema.types.PDataType;

import org.apache.phoenix.thirdparty.com.google.common.base.Ticker;
import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors;

/**
 * Per-JVM cache mapping fully-qualified PROTOBUF message names to resolved protobuf
 * {@code Descriptor} instances. {@link #withLoader} gives a caller its own loader over the same
 * cache.
 */
public final class ProtoDescriptorRegistry {

  @FunctionalInterface
  public interface Loader {
    /**
     * Returns the raw {@code FileDescriptorSet} bytes registered under
     * {@code fullyQualifiedMessageName} in {@code SYSTEM.PROTO_DESCRIPTORS}, or {@code null} if the
     * name is not registered.
     */
    byte[] load(String fullyQualifiedMessageName) throws IOException;
  }

  /**
   * Cache entry: the resolved message Descriptor. Also owns a per-descriptor PathPlan cache keyed
   * by {@code (pathLiteral, PlanKind, requestedType)}.
   */
  public static final class CachedDescriptor {
    private final Descriptors.Descriptor descriptor;
    private final int sizeBytes;
    private final Cache<String, PathPlan> pathPlans;

    CachedDescriptor(Descriptors.Descriptor descriptor, int sizeBytes, long planCacheMaxSize,
      long planCacheTtlSeconds, Ticker ticker) {
      this.descriptor = descriptor;
      this.sizeBytes = sizeBytes;
      this.pathPlans = CacheBuilder.newBuilder().maximumSize(planCacheMaxSize)
        .expireAfterWrite(planCacheTtlSeconds, TimeUnit.SECONDS).ticker(ticker).build();
    }

    public Descriptors.Descriptor getDescriptor() {
      return descriptor;
    }

    public PathPlan getOrCompilePathPlan(String pathLiteral, PlanKind kind,
      PDataType<?> requestedType, PathPlanCompiler compiler) throws SQLException {
      // PlanKind partitions the cache so PROTO_VALUE / PROTO_EXISTS /
      // PROTO_ARRAY_LENGTH plans for the same path never share a cache entry.
      String key = pathLiteral + '|' + kind.name() + '|'
        + (requestedType == null ? "" : requestedType.getSqlTypeName());
      try {
        return pathPlans.get(key, () -> compiler.compile(descriptor));
      } catch (ExecutionException | UncheckedExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof SQLException) {
          throw (SQLException) cause;
        }
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        throw new SQLException(cause != null ? cause : e);
      }
    }
  }

  @FunctionalInterface
  public interface PathPlanCompiler {
    PathPlan compile(Descriptors.Descriptor descriptor) throws SQLException;
  }

  public enum PlanKind {
    VALUE,
    PRESENCE,
    ARRAY_LENGTH
  }

  /**
   * Bounds the per-descriptor plan cache and the forced-refresh table, not the descriptor cache.
   */
  public static final String CACHE_MAX_SIZE_ATTRIB = "phoenix.protobuf.descriptor.cache.max.size";
  public static final long DEFAULT_CACHE_MAX_SIZE = 1024L;

  public static final String CACHE_MAX_HEAP_PERCENT_ATTRIB =
    "phoenix.protobuf.descriptor.cache.max.heap.percent";
  public static final int DEFAULT_CACHE_MAX_HEAP_PERCENT = 5;

  public static final String CACHE_TTL_SECONDS_ATTRIB =
    "phoenix.protobuf.descriptor.cache.ttl.seconds";
  public static final long DEFAULT_CACHE_TTL_SECONDS = 3600L;

  /**
   * Minimum interval between two forced refreshes of the same descriptor name (see
   * {@link #tryForceRefresh(String)}). Bounds the reload rate when a path is genuinely wrong rather
   * than stale.
   */
  public static final String MIN_FORCED_REFRESH_INTERVAL_MS_ATTRIB =
    "phoenix.protobuf.descriptor.cache.min.forced.refresh.interval.ms";
  public static final long DEFAULT_MIN_FORCED_REFRESH_INTERVAL_MS = 10000L;

  private final Cache<String, Optional<CachedDescriptor>> byName;
  private final Cache<String, Boolean> recentForcedRefreshes;
  private final Loader loader;
  private final ProtoDescriptorResolver resolver;
  private final Ticker ticker;
  private final long planCacheMaxSize;
  private final long planCacheTtlSeconds;

  public ProtoDescriptorRegistry(Configuration conf, Loader loader) {
    this(conf, loader, new ProtoDescriptorResolver(), Ticker.systemTicker());
  }

  ProtoDescriptorRegistry(Configuration conf, Loader loader, ProtoDescriptorResolver resolver,
    Ticker ticker) {
    if (loader == null) {
      throw new IllegalArgumentException("Loader must not be null");
    }
    if (resolver == null) {
      throw new IllegalArgumentException("Resolver must not be null");
    }
    if (ticker == null) {
      throw new IllegalArgumentException("Ticker must not be null");
    }
    this.loader = loader;
    this.resolver = resolver;
    this.ticker = ticker;
    long maxSize = readPositiveLong(conf, CACHE_MAX_SIZE_ATTRIB, DEFAULT_CACHE_MAX_SIZE);
    long ttlSeconds = readPositiveLong(conf, CACHE_TTL_SECONDS_ATTRIB, DEFAULT_CACHE_TTL_SECONDS);
    this.planCacheMaxSize = maxSize;
    this.planCacheTtlSeconds = ttlSeconds;
    // A load on first use or expiry runs on the calling thread, bounded only by the query timeout.
    // Stale entries are not served during a reload, to keep the cache simple.
    // Up to 10% shorter per JVM so RegionServers don't all reload against the one region together.
    long ttlMs = TimeUnit.SECONDS.toMillis(ttlSeconds);
    ttlMs -= ThreadLocalRandom.current().nextLong(ttlMs / 10 + 1);
    this.byName = CacheBuilder.newBuilder().maximumWeight(cacheMaxBytes(conf))
      .weigher((String k, Optional<CachedDescriptor> v) -> v
        .map(cachedDescriptor -> cachedDescriptor.sizeBytes).orElse(0))
      .concurrencyLevel(1).expireAfterWrite(ttlMs, TimeUnit.MILLISECONDS).ticker(ticker).build();
    long minRefreshMs = readPositiveLong(conf, MIN_FORCED_REFRESH_INTERVAL_MS_ATTRIB,
      DEFAULT_MIN_FORCED_REFRESH_INTERVAL_MS);
    this.recentForcedRefreshes = CacheBuilder.newBuilder().maximumSize(maxSize)
      .expireAfterWrite(minRefreshMs, TimeUnit.MILLISECONDS).ticker(ticker).build();
  }

  private ProtoDescriptorRegistry(ProtoDescriptorRegistry shared, Loader loader) {
    this.byName = shared.byName;
    this.recentForcedRefreshes = shared.recentForcedRefreshes;
    this.loader = loader;
    this.resolver = shared.resolver;
    this.ticker = shared.ticker;
    this.planCacheMaxSize = shared.planCacheMaxSize;
    this.planCacheTtlSeconds = shared.planCacheTtlSeconds;
  }

  /**
   * Returns a view that shares this registry's caches but loads misses with {@code loader}, so
   * every caller in the JVM sees one cache entry per name however it reaches the catalog.
   */
  public ProtoDescriptorRegistry withLoader(Loader loader) {
    if (loader == null) {
      throw new IllegalArgumentException("Loader must not be null");
    }
    return new ProtoDescriptorRegistry(this, loader);
  }

  /**
   * Evicts the cached entry for {@code fullyQualifiedMessageName} so the next {@link #lookup}
   * reloads it from {@code SYSTEM.PROTO_DESCRIPTORS}, unless the same name was already
   * force-refreshed within the configured minimum interval.
   * <p>
   * Used when path resolution fails against a possibly stale descriptor (another client ran
   * {@code CREATE}/{@code ALTER PROTOBUF DESCRIPTOR} and this JVM's entry has not reached its TTL
   * yet). Evolution is add-only, so one reload is enough to pick up any field the caller could
   * legitimately reference.
   * @return {@code true} if the caller won the refresh and should retry once; {@code false} if a
   *         refresh happened recently (the failure is then not caused by staleness of more than the
   *         interval) or the name is empty
   */
  public boolean tryForceRefresh(String fullyQualifiedMessageName) {
    if (fullyQualifiedMessageName == null || fullyQualifiedMessageName.isEmpty()) {
      return false;
    }
    if (
      recentForcedRefreshes.asMap().putIfAbsent(fullyQualifiedMessageName, Boolean.TRUE) != null
    ) {
      return false;
    }
    byName.invalidate(fullyQualifiedMessageName);
    return true;
  }

  /**
   * Resolves {@code fullyQualifiedMessageName} to a {@link CachedDescriptor}, hitting the cache
   * first and falling back to the {@link Loader} on miss. Returns {@code null} if the descriptor is
   * not registered (Loader returned null) or if the registered bytes do not contain a top-level
   * message matching the requested name (resolver filtered). Misses are not cached, so a descriptor
   * registered after a failed lookup is visible on the next one.
   * <p>
   * Single-flight per key — Guava's {@code Cache.get(K, Callable)} guarantees only one concurrent
   * loader call for the same key; other callers block until the load completes and then share the
   * cached result.
   * @throws PhoenixIOException if the Loader throws {@link IOException} (e.g. catalog unreachable)
   *                            or if the resolver fails to parse the bytes (e.g. corrupt descriptor
   *                            cell)
   */
  public CachedDescriptor lookup(String fullyQualifiedMessageName) throws PhoenixIOException {
    if (fullyQualifiedMessageName == null || fullyQualifiedMessageName.isEmpty()) {
      return null;
    }
    Optional<CachedDescriptor> result;
    try {
      result = byName.get(fullyQualifiedMessageName, () -> {
        byte[] bytes = loader.load(fullyQualifiedMessageName);
        if (bytes == null) {
          return Optional.empty();
        }
        Descriptors.Descriptor descriptor = resolver.resolve(bytes, fullyQualifiedMessageName);
        if (descriptor == null) {
          return Optional.empty();
        }
        return Optional.of(new CachedDescriptor(descriptor, bytes.length, planCacheMaxSize,
          planCacheTtlSeconds, ticker));
      });
    } catch (ExecutionException | UncheckedExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw new PhoenixIOException(cause);
      }
      throw new PhoenixIOException(cause != null ? new IOException(cause) : new IOException(e));
    }
    if (!result.isPresent()) {
      byName.asMap().remove(fullyQualifiedMessageName, result);
      return null;
    }
    return result.get();
  }

  public void invalidate(String fullyQualifiedMessageName) {
    if (fullyQualifiedMessageName == null || fullyQualifiedMessageName.isEmpty()) {
      return;
    }
    byName.invalidate(fullyQualifiedMessageName);
  }

  public static long cacheMaxBytes(Configuration conf) {
    long percent =
      readPositiveLong(conf, CACHE_MAX_HEAP_PERCENT_ATTRIB, DEFAULT_CACHE_MAX_HEAP_PERCENT);
    return Runtime.getRuntime().maxMemory() / 100 * Math.min(percent, 100);
  }

  private static long readPositiveLong(Configuration conf, String key, long defaultValue) {
    if (conf == null) {
      return defaultValue;
    }
    long value = conf.getLong(key, defaultValue);
    return value > 0 ? value : defaultValue;
  }
}
