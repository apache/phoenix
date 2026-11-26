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
package org.apache.phoenix.coprocessor;

import static org.apache.phoenix.coprocessor.MetaDataEndpointImpl.mutateRowsWithLocks;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Region.RowLock;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.AlterProtobufDescriptorRequest;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.CreateProtobufBundleRequest;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.CreateProtobufDescriptorRequest;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.DropProtobufDescriptorRequest;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.ProtobufDescriptorMutationResponse;
import org.apache.phoenix.coprocessor.generated.ProtobufDescriptorProtos.ProtobufDescriptorService;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.protobuf.descriptor.ProtoDescriptorRegistry;
import org.apache.phoenix.protobuf.descriptor.ProtoDescriptorRegistryHolder;
import org.apache.phoenix.protobuf.descriptor.ProtoDescriptorResolver;
import org.apache.phoenix.protobuf.descriptor.ProtoEvolutionValidator;
import org.apache.phoenix.protobuf.descriptor.ProtobufDescriptorFetcher;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import org.apache.phoenix.thirdparty.com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors;

/**
 * Region-side coprocessor for {@link ProtobufDescriptorService} on
 * {@code SYSTEM.PROTO_DESCRIPTORS}. CREATE / ALTER / DROP DESCRIPTOR and CREATE BUNDLE all write
 * through {@link MetaDataEndpointImpl#mutateRowsWithLocks}.
 */
public class ProtobufDescriptorEndpointImpl extends ProtobufDescriptorService
  implements RegionCoprocessor {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(ProtobufDescriptorEndpointImpl.class);

  private RegionCoprocessorEnvironment env;
  private boolean accessCheckEnabled;
  private PhoenixMetaDataCoprocessorHost phoenixAccessCoprocessorHost;
  private final ProtobufDescriptorFetcher fetcher = new ProtobufDescriptorFetcher();

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment) env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
    Configuration conf = env.getConfiguration();
    this.accessCheckEnabled = conf.getBoolean(QueryServices.PHOENIX_ACLS_ENABLED,
      QueryServicesOptions.DEFAULT_PHOENIX_ACLS_ENABLED);
    this.phoenixAccessCoprocessorHost = new PhoenixMetaDataCoprocessorHost(this.env);
    PhoenixRegionServerEndpoint.registerProtoDescriptorRegistryFactory(conf);
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(this);
  }

  @Override
  public void createProtobufDescriptor(RpcController controller,
    CreateProtobufDescriptorRequest request, RpcCallback<ProtobufDescriptorMutationResponse> done) {

    String protobufName = request.getProtobufName();
    String sourceUrl = request.getSourceUrl();
    boolean ifNotExists = request.getIfNotExists();
    try {
      phoenixAccessCoprocessorHost
        .preCreateProtobufDescriptor(Collections.singletonList(protobufName));
      byte[] descriptorBytes =
        inlineBytesOrNull(request.hasInlineDescriptorBytes(), request.getInlineDescriptorBytes());
      FileDescriptorSet descriptorSet = materializeDescriptor(descriptorBytes, sourceUrl);

      if (!isTopLevelMessageInSet(protobufName, descriptorSet)) {
        done.run(typeNotFoundResponse(protobufName));
        return;
      }
      resolveOrThrow(descriptorBytes, protobufName);

      Region region = env.getRegion();
      List<RowLock> locks = lockRows(region, Collections.singletonList(protobufName));
      try {
        if (descriptorRowExists(region, protobufName)) {
          if (ifNotExists) {
            done.run(okResponse(Collections.emptyList()));
          } else {
            done.run(alreadyExistsResponse(protobufName));
          }
          return;
        }
        long now = EnvironmentEdgeManager.currentTimeMillis();
        List<Mutation> mutations =
          Collections.singletonList(buildDescriptorPut(protobufName, descriptorBytes, now));
        mutateRowsWithLocks(this.accessCheckEnabled, region, mutations, Collections.emptySet(),
          HConstants.NO_NONCE, HConstants.NO_NONCE);
      } finally {
        ServerUtil.releaseRowLocks(locks);
      }
      done.run(okResponse(Collections.singletonList(protobufName)));
    } catch (AccessDeniedException e) {
      accessDenied(controller, done, "createProtobufDescriptor", protobufName, e);
    } catch (SQLException e) {
      done.run(fromSqlException(e, protobufName));
    } catch (Throwable t) {
      LOGGER.error("Failed to handle createProtobufDescriptor for {}", protobufName, t);
      ProtobufUtil.setControllerException(controller,
        ClientUtil.createIOException(protobufName, t));
    }
  }

  @Override
  public void alterProtobufDescriptor(RpcController controller,
    AlterProtobufDescriptorRequest request, RpcCallback<ProtobufDescriptorMutationResponse> done) {

    String protobufName = request.getProtobufName();
    String sourceUrl = request.getSourceUrl();
    boolean ifExists = request.getIfExists();
    try {
      phoenixAccessCoprocessorHost.preAlterProtobufDescriptor(protobufName);
      byte[] descriptorBytes =
        inlineBytesOrNull(request.hasInlineDescriptorBytes(), request.getInlineDescriptorBytes());
      FileDescriptorSet descriptorSet = materializeDescriptor(descriptorBytes, sourceUrl);

      if (!isTopLevelMessageInSet(protobufName, descriptorSet)) {
        done.run(typeNotFoundResponse(protobufName));
        return;
      }

      Descriptors.Descriptor newDesc = resolveOrThrow(descriptorBytes, protobufName);
      Region region = env.getRegion();
      List<RowLock> locks = lockRows(region, Collections.singletonList(protobufName));
      try {
        byte[] oldBytes = getDescriptorBytes(region, protobufName);
        if (oldBytes == null) {
          if (ifExists) {
            done.run(okResponse(Collections.emptyList()));
          } else {
            done.run(notFoundResponse(protobufName));
          }
          return;
        }
        ProtoEvolutionValidator.validate(resolveOrThrow(oldBytes, protobufName), newDesc);
        long now = EnvironmentEdgeManager.currentTimeMillis();
        List<Mutation> mutations =
          Collections.singletonList(buildDescriptorPut(protobufName, descriptorBytes, now));
        mutateRowsWithLocks(this.accessCheckEnabled, region, mutations, Collections.emptySet(),
          HConstants.NO_NONCE, HConstants.NO_NONCE);
      } finally {
        ServerUtil.releaseRowLocks(locks);
      }
      invalidateRegistryEntry(protobufName);
      done.run(okResponse(Collections.singletonList(protobufName)));
    } catch (AccessDeniedException e) {
      accessDenied(controller, done, "alterProtobufDescriptor", protobufName, e);
    } catch (SQLException e) {
      done.run(fromSqlException(e, protobufName));
    } catch (Throwable t) {
      LOGGER.error("Failed to handle alterProtobufDescriptor for {}", protobufName, t);
      ProtobufUtil.setControllerException(controller,
        ClientUtil.createIOException(protobufName, t));
    }
  }

  @Override
  public void dropProtobufDescriptor(RpcController controller,
    DropProtobufDescriptorRequest request, RpcCallback<ProtobufDescriptorMutationResponse> done) {

    String protobufName = request.getProtobufName();
    boolean ifExists = request.getIfExists();
    try {
      phoenixAccessCoprocessorHost.preDropProtobufDescriptor(protobufName);
      Region region = env.getRegion();
      List<RowLock> locks = lockRows(region, Collections.singletonList(protobufName));
      try {
        if (!descriptorRowExists(region, protobufName)) {
          if (ifExists) {
            done.run(okResponse(Collections.emptyList()));
          } else {
            done.run(notFoundResponse(protobufName));
          }
          return;
        }
        // Not locked against a concurrent CREATE / ALTER ADD of a column using this descriptor.
        // Shared lock between two coproc hooks that do not share same region requires complexity
        // that is not worth adding here.
        if (isDescriptorInUse(protobufName)) {
          done.run(inUseResponse(protobufName));
          return;
        }
        long now = EnvironmentEdgeManager.currentTimeMillis();
        List<Mutation> mutations =
          Collections.singletonList(new Delete(Bytes.toBytes(protobufName), now));
        mutateRowsWithLocks(this.accessCheckEnabled, region, mutations, Collections.emptySet(),
          HConstants.NO_NONCE, HConstants.NO_NONCE);
      } finally {
        ServerUtil.releaseRowLocks(locks);
      }
      invalidateRegistryEntry(protobufName);
      done.run(okResponse(Collections.singletonList(protobufName)));
    } catch (AccessDeniedException e) {
      accessDenied(controller, done, "dropProtobufDescriptor", protobufName, e);
    } catch (Throwable t) {
      LOGGER.error("Failed to handle dropProtobufDescriptor for {}", protobufName, t);
      ProtobufUtil.setControllerException(controller,
        ClientUtil.createIOException(protobufName, t));
    }
  }

  private static void accessDenied(RpcController controller,
    RpcCallback<ProtobufDescriptorMutationResponse> done, String op, String name,
    AccessDeniedException e) {
    LOGGER.warn("{} denied for {}: {}", op, name, e);
    ProtobufUtil.setControllerException(controller, e);
    done.run(null);
  }

  private static void invalidateRegistryEntry(String protobufName) {
    ProtoDescriptorRegistry registry = ProtoDescriptorRegistryHolder.getIfInstalled();
    if (registry != null) {
      registry.invalidate(protobufName);
    }
  }

  @Override
  public void createProtobufBundle(RpcController controller, CreateProtobufBundleRequest request,
    RpcCallback<ProtobufDescriptorMutationResponse> done) {

    List<String> names = request.getProtobufNamesList();
    String sourceUrl = request.getSourceUrl();
    boolean ifNotExists = request.getIfNotExists();
    try {
      phoenixAccessCoprocessorHost.preCreateProtobufDescriptor(names);
      byte[] descriptorBytes =
        inlineBytesOrNull(request.hasInlineDescriptorBytes(), request.getInlineDescriptorBytes());
      FileDescriptorSet descriptorSet = materializeDescriptor(descriptorBytes, sourceUrl);
      // Every name stores, and caches, its own copy of the set: cap the total at the cache budget.
      // The cap grows with the heap and there is no fixed limit. Real sets are small, and this is
      // an admin DDL, so a fixed limit is not needed.
      long maxBytes = ProtoDescriptorRegistry.cacheMaxBytes(env.getConfiguration());
      if ((long) names.size() * descriptorBytes.length > maxBytes) {
        done.run(errorResponse(SQLExceptionCode.PROTOBUF_BUNDLE_TOO_LARGE, names.get(0),
          names.size() + " names x " + descriptorBytes.length + " bytes exceeds " + maxBytes));
        return;
      }

      Set<String> topLevelMessages = collectTopLevelMessageNames(descriptorSet);
      for (String name : names) {
        if (!topLevelMessages.contains(name)) {
          done.run(typeNotFoundResponse(name));
          return;
        }
      }
      try {
        resolveOrThrow(descriptorBytes, names.get(0));
      } catch (SQLException e) {
        done.run(fromSqlException(e, names.get(0)));
        return;
      }

      Region region = env.getRegion();
      List<String> registeredNames = new ArrayList<>(names.size());
      List<RowLock> locks = lockRows(region, names);
      try {
        long now = EnvironmentEdgeManager.currentTimeMillis();
        List<Mutation> mutations = new ArrayList<>(names.size());
        for (String name : names) {
          if (descriptorRowExists(region, name)) {
            if (ifNotExists) {
              continue;
            }
            done.run(alreadyExistsResponse(name));
            return;
          }
          mutations.add(buildDescriptorPut(name, descriptorBytes, now));
          registeredNames.add(name);
        }
        if (!mutations.isEmpty()) {
          mutateRowsWithLocks(this.accessCheckEnabled, region, mutations, Collections.emptySet(),
            HConstants.NO_NONCE, HConstants.NO_NONCE);
        }
      } finally {
        ServerUtil.releaseRowLocks(locks);
      }
      done.run(okResponse(registeredNames));
    } catch (AccessDeniedException e) {
      accessDenied(controller, done, "createProtobufBundle", String.valueOf(names), e);
    } catch (SQLException e) {
      done.run(fromSqlException(e, names.isEmpty() ? "" : names.get(0)));
    } catch (Throwable t) {
      LOGGER.error("Failed to handle createProtobufBundle for {}", names, t);
      ProtobufUtil.setControllerException(controller,
        ClientUtil.createIOException(String.valueOf(names), t));
    }
  }

  /**
   * Parses the inline bytes the client read from its local file:/// URL. The server never fetches
   * {@code sourceUrl} itself: doing so would read with the region server's own filesystem
   * credentials (or its local disk) on behalf of the caller.
   */
  private FileDescriptorSet materializeDescriptor(byte[] inlineBytes, String sourceUrl)
    throws SQLException {
    if (inlineBytes == null) {
      throw ProtobufDescriptorFetcher.urlNotAllowed(sourceUrl);
    }
    return fetcher.parseBytes(sourceUrl, inlineBytes);
  }

  private static byte[] inlineBytesOrNull(boolean hasInline, ByteString inline) {
    return hasInline ? inline.toByteArray() : null;
  }

  /** Top-level messages only; nested types are not CREATE/ALTER targets. */
  private static boolean isTopLevelMessageInSet(String protobufName, FileDescriptorSet set) {
    return collectTopLevelMessageNames(set).contains(protobufName);
  }

  private static Set<String> collectTopLevelMessageNames(FileDescriptorSet set) {
    Set<String> names = new LinkedHashSet<>();
    for (FileDescriptorProto file : set.getFileList()) {
      String pkg = file.hasPackage() ? file.getPackage() : "";
      for (int i = 0; i < file.getMessageTypeCount(); i++) {
        String msg = file.getMessageType(i).getName();
        names.add(pkg.isEmpty() ? msg : pkg + "." + msg);
      }
    }
    return names;
  }

  /** Write-locks the descriptor rows in sorted order, so overlapping bundles cannot deadlock. */
  private static List<RowLock> lockRows(Region region, Collection<String> names)
    throws IOException {
    Set<byte[]> rows = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (String name : names) {
      rows.add(Bytes.toBytes(name));
    }
    List<RowLock> locks = new ArrayList<>(rows.size());
    try {
      for (byte[] row : rows) {
        locks.add(region.getRowLock(row, false));
      }
    } catch (IOException e) {
      ServerUtil.releaseRowLocks(locks);
      throw e;
    }
    return locks;
  }

  private static boolean descriptorRowExists(Region region, String protobufName)
    throws IOException {
    byte[] rowKey = Bytes.toBytes(protobufName);
    Get get = new Get(rowKey);
    get.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
      PhoenixDatabaseMetaData.FILE_DESCRIPTOR_SET_BYTES);
    Result result = region.get(get);
    return result != null && !result.isEmpty();
  }

  /** Currently registered FILE_DESCRIPTOR_SET bytes, or null if the row is missing. */
  private static byte[] getDescriptorBytes(Region region, String protobufName) throws IOException {
    byte[] rowKey = Bytes.toBytes(protobufName);
    Get get = new Get(rowKey);
    get.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
      PhoenixDatabaseMetaData.FILE_DESCRIPTOR_SET_BYTES);
    Result result = region.get(get);
    if (result == null || result.isEmpty()) {
      return null;
    }
    return result.getValue(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
      PhoenixDatabaseMetaData.FILE_DESCRIPTOR_SET_BYTES);
  }

  /** Resolves {@code protobufName} from FileDescriptorSet bytes, or throws. */
  private static Descriptors.Descriptor resolveOrThrow(byte[] descriptorBytes, String protobufName)
    throws SQLException {
    try {
      Descriptors.Descriptor resolved =
        new ProtoDescriptorResolver().resolve(descriptorBytes, protobufName);
      if (resolved == null) {
        throw new SQLExceptionInfo.Builder(
          SQLExceptionCode.PROTOBUF_DESCRIPTOR_TYPE_NOT_FOUND_IN_FILE)
            .setMessage("descriptor '" + protobufName + "' not present in resolved set").build()
            .buildException();
      }
      return resolved;
    } catch (IOException e) {
      throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_DESCRIPTOR_PARSE_FAILED)
        .setMessage("failed to resolve descriptor '" + protobufName + "': " + e.getMessage())
        .build().buildException();
    }
  }

  private static Put buildDescriptorPut(String protobufName, byte[] descriptorBytes,
    long timestamp) {
    byte[] rowKey = Bytes.toBytes(protobufName);
    Put put = new Put(rowKey);
    put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
      PhoenixDatabaseMetaData.FILE_DESCRIPTOR_SET_BYTES, timestamp, descriptorBytes);
    put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES,
      timestamp, QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
    return put;
  }

  /**
   * Returns {@code true} if a {@code SYSTEM.PROTO_DESC_LINK} row for {@code protobufName} points at
   * a live {@code SYSTEM.CATALOG} column still bound to it. Links whose column is gone are left in
   * place; the next link write for the same column overwrites them.
   * <p>
   * A DROP can still race a concurrent CREATE TABLE or ADD COLUMN on the same descriptor: if the
   * DROP scans before the new column is written, both succeed and the column points to a missing
   * descriptor. PROTO_* queries on it fail until the descriptor is registered again.
   */
  private boolean isDescriptorInUse(String protobufName) throws IOException {
    byte[] name = Bytes.toBytes(protobufName);
    Scan scan = new Scan();
    scan.setStartStopRowForPrefixScan(Bytes.add(name, QueryConstants.SEPARATOR_BYTE_ARRAY));
    scan.setFilter(new FirstKeyOnlyFilter());
    Configuration conf = env.getConfiguration();
    try (
      Table links = ServerUtil.getHTableForCoprocessorScan(env,
        SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_PROTO_DESC_LINK_NAME_BYTES,
          conf));
      Table catalog = ServerUtil.getHTableForCoprocessorScan(env,
        SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, conf));
      ResultScanner scanner = links.getScanner(scan)) {
      for (Result link : scanner) {
        // protobuf name, tenant, schema, table, family, column
        byte[][] key = new byte[6][];
        SchemaUtil.getVarChars(link.getRow(), key);
        byte[] columnKey = SchemaUtil.getColumnKey(Bytes.toString(key[1]), Bytes.toString(key[2]),
          Bytes.toString(key[3]), Bytes.toString(key[5]),
          key[4].length == 0 ? null : Bytes.toString(key[4]));
        Result column = catalog.get(new Get(columnKey).addColumn(TABLE_FAMILY_BYTES,
          PhoenixDatabaseMetaData.PROTOBUF_DESCRIPTOR_NAME_BYTES));
        if (
          Bytes.equals(name,
            column.getValue(TABLE_FAMILY_BYTES,
              PhoenixDatabaseMetaData.PROTOBUF_DESCRIPTOR_NAME_BYTES))
        ) {
          return true;
        }
      }
    }
    return false;
  }

  private static ProtobufDescriptorMutationResponse okResponse(Collection<String> registered) {
    return ProtobufDescriptorMutationResponse.newBuilder().addAllRegisteredNames(registered)
      .build();
  }

  private static ProtobufDescriptorMutationResponse errorResponse(SQLExceptionCode code,
    String name, String message) {
    ProtobufDescriptorMutationResponse.Builder b =
      ProtobufDescriptorMutationResponse.newBuilder().setErrorCode(code.getErrorCode());
    if (name != null) {
      b.setOffendingName(name);
    }
    if (message != null) {
      b.setErrorMessage(message);
    }
    return b.build();
  }

  private static ProtobufDescriptorMutationResponse alreadyExistsResponse(String name) {
    return errorResponse(SQLExceptionCode.PROTOBUF_DESCRIPTOR_ALREADY_EXISTS, name, null);
  }

  private static ProtobufDescriptorMutationResponse notFoundResponse(String name) {
    return errorResponse(SQLExceptionCode.PROTOBUF_DESCRIPTOR_NOT_FOUND, name, null);
  }

  private static ProtobufDescriptorMutationResponse typeNotFoundResponse(String name) {
    return errorResponse(SQLExceptionCode.PROTOBUF_DESCRIPTOR_TYPE_NOT_FOUND_IN_FILE, name,
      "No top-level protobuf message named '" + name
        + "' was found in the loaded FileDescriptorSet");
  }

  private static ProtobufDescriptorMutationResponse inUseResponse(String name) {
    return errorResponse(SQLExceptionCode.PROTOBUF_DESCRIPTOR_IN_USE, name, null);
  }

  private static ProtobufDescriptorMutationResponse fromSqlException(SQLException e, String name) {
    SQLExceptionCode code = SQLExceptionCode.fromErrorCode(e.getErrorCode());
    if (code == SQLExceptionCode.UNKNOWN_ERROR_CODE) {
      code = SQLExceptionCode.PROTOBUF_DESCRIPTOR_INTERNAL_ERROR;
      LOGGER.error("Unexpected failure for PROTOBUF descriptor '{}'", name, e);
    } else {
      LOGGER.info("PROTOBUF descriptor '{}' request rejected: {}", name, e.getMessage());
    }
    return errorResponse(code, name, String.valueOf(e.getMessage()));
  }
}
