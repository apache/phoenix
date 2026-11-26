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

/**
 * Per-JVM static holder for a shared {@link ProtoDescriptorRegistry} instance.
 */
public final class ProtoDescriptorRegistryHolder {

  /**
   * Lazy-construction hook used by server-side. Registered via {@link #setFactory}, and invoked at
   * most once by the first {@link #get()} that observes {@code registryInstance == null}.
   */
  @FunctionalInterface
  public interface RegistryFactory {
    ProtoDescriptorRegistry create();
  }

  public static final String ENDPOINT_NOT_REGISTERED =
    "PROTOBUF functions need PhoenixRegionServerEndpoint installed on every region server";

  private static volatile ProtoDescriptorRegistry registryInstance;
  private static volatile RegistryFactory registryFactory;

  private ProtoDescriptorRegistryHolder() {
  }

  /**
   * Installs {@code registry} as the per-JVM shared instance if none is installed yet. Used by the
   * client-side eager path; server-side callers should prefer {@link #setFactory} so nothing is
   * built until the first function evaluate needs it.
   * @param registry the registry to install
   * @return {@code true} if this call installed the registry, {@code false} if another caller had
   *         already installed one
   */
  public static boolean install(ProtoDescriptorRegistry registry) {
    if (registry == null) {
      throw new IllegalArgumentException("registry must not be null");
    }
    if (registryInstance != null) {
      return false;
    }
    synchronized (ProtoDescriptorRegistryHolder.class) {
      if (registryInstance != null) {
        return false;
      }
      registryInstance = registry;
      return true;
    }
  }

  /**
   * Registers a lazy {@link RegistryFactory}. Used by server-side.
   * @param factory factory to register
   * @return {@code true} if this call registered the factory, {@code false} if another caller had
   *         already registered one
   */
  public static boolean setFactory(RegistryFactory factory) {
    if (factory == null) {
      throw new IllegalArgumentException("factory must not be null");
    }
    if (registryFactory != null) {
      return false;
    }
    synchronized (ProtoDescriptorRegistryHolder.class) {
      if (registryFactory != null) {
        return false;
      }
      registryFactory = factory;
      return true;
    }
  }

  /**
   * @return the installed per-JVM registry; if none is installed but a {@link RegistryFactory} has
   *         been registered via {@link #setFactory}, the factory is invoked exactly once (under the
   *         holder lock) to build and install one on demand
   * @throws IllegalStateException if neither a registry nor a factory has been registered in this
   *                               JVM
   */
  public static ProtoDescriptorRegistry get() {
    ProtoDescriptorRegistry snapshot = registryInstance;
    if (snapshot != null) {
      return snapshot;
    }
    RegistryFactory factory = registryFactory;
    if (factory != null) {
      synchronized (ProtoDescriptorRegistryHolder.class) {
        snapshot = registryInstance;
        if (snapshot != null) {
          return snapshot;
        }
        snapshot = factory.create();
        if (snapshot == null) {
          throw new IllegalStateException("ProtoDescriptorRegistryHolder factory returned null");
        }
        registryInstance = snapshot;
        return snapshot;
      }
    }
    throw new IllegalStateException(ENDPOINT_NOT_REGISTERED);
  }

  /**
   * Like {@link #get()}, but installs {@code fallback}'s registry when neither a registry nor a
   * factory is registered.
   */
  public static ProtoDescriptorRegistry getOrInstall(RegistryFactory fallback) {
    if (registryInstance == null && registryFactory == null) {
      install(fallback.create());
    }
    return get();
  }

  public static ProtoDescriptorRegistry getIfInstalled() {
    return registryInstance;
  }

  public static void uninstall() {
    synchronized (ProtoDescriptorRegistryHolder.class) {
      registryInstance = null;
      registryFactory = null;
    }
  }
}
