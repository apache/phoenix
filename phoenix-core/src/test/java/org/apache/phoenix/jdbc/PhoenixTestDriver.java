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
package org.apache.phoenix.jdbc;

import static org.apache.phoenix.thirdparty.com.google.common.base.Preconditions.checkNotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.phoenix.end2end.ConnectionQueryServicesTestImpl;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionlessQueryServicesImpl;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesTestImpl;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;

/**
 * JDBC Driver implementation of Phoenix for testing. To use this driver, specify test=true in url.
 * @since 0.1
 */
@ThreadSafe
public class PhoenixTestDriver extends PhoenixEmbeddedDriver {

  private final ReadOnlyProps overrideProps;

  private final QueryServices queryServices;

  private final Map<ConnectionInfo, ConnectionQueryServices> connectionQueryServicesMap =
    new HashMap<>();

  @GuardedBy("closeLock")
  private volatile boolean closed = false;
  private final ReadWriteLock closeLock = new ReentrantReadWriteLock();

  public PhoenixTestDriver() {
    this(ReadOnlyProps.EMPTY_PROPS);
  }

  // For tests to override the default configuration
  public PhoenixTestDriver(ReadOnlyProps props) {
    overrideProps = props;
    queryServices = new QueryServicesTestImpl(getDefaultProps(), overrideProps);
  }

  @Override
  public QueryServices getQueryServices() throws SQLException {
    lockInterruptibly(PhoenixTestDriver.LockMode.READ);
    try {
      checkClosed();
      return queryServices;
    } finally {
      unlock(PhoenixTestDriver.LockMode.READ);
    }
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    // Accept the url only if test=true attribute set
    return super.acceptsURL(url) && isTestUrl(url);
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    lockInterruptibly(PhoenixTestDriver.LockMode.READ);
    try {
      checkClosed();
      return super.connect(url, info);
    } finally {
      unlock(PhoenixTestDriver.LockMode.READ);
    }
  }

  @Override // public for testing
  public ConnectionQueryServices getConnectionQueryServices(String url, Properties infoIn)
    throws SQLException {
    lockInterruptibly(PhoenixTestDriver.LockMode.READ);
    try {
      checkClosed();
      final Properties info = PropertiesUtil.deepCopy(infoIn);
      ConnectionInfo connInfo = ConnectionInfo.create(url, null, info);
      ConnectionQueryServices connectionQueryServices = connectionQueryServicesMap.get(connInfo);
      if (connectionQueryServices != null) {
        return connectionQueryServices;
      }
      info.putAll(connInfo.asProps().asMap());
      if (connInfo.isConnectionless()) {
        connectionQueryServices =
          new ConnectionlessQueryServicesImpl(queryServices, connInfo, info);
      } else {
        connectionQueryServices =
          new ConnectionQueryServicesTestImpl(queryServices, connInfo, info);
      }
      connectionQueryServices.init(url, info);
      connectionQueryServicesMap.put(connInfo, connectionQueryServices);
      return connectionQueryServices;
    } finally {
      unlock(PhoenixTestDriver.LockMode.READ);
    }
  }

  public void cleanUpCQSICache() throws SQLException {
    lockInterruptibly(LockMode.WRITE);
    try {
      for (ConnectionQueryServices service : connectionQueryServicesMap.values()) {
        service.close();
      }
      connectionQueryServicesMap.clear();
    } finally {
      unlock(LockMode.WRITE);
    }
  }

  @GuardedBy("closeLock")
  private void checkClosed() {
    if (closed) {
      throw new IllegalStateException("The Phoenix jdbc test driver has been closed.");
    }
  }

  @Override
  public void close() throws SQLException {
    lockInterruptibly(PhoenixTestDriver.LockMode.WRITE);

    try {
      if (closed) {
        return;
      }
      closed = true;
    } finally {
      unlock(PhoenixTestDriver.LockMode.WRITE);
    }
    try {
      for (ConnectionQueryServices cqs : connectionQueryServicesMap.values()) {
        cqs.close();
      }
    } finally {
      ThreadPoolExecutor executor = queryServices.getExecutor();
      try {
        queryServices.close();
      } finally {
        if (executor != null) executor.shutdownNow();
        connectionQueryServicesMap.clear();
        ;
      }
    }
  }

  private enum LockMode {
    READ,
    WRITE
  };

  private void lockInterruptibly(PhoenixTestDriver.LockMode mode) throws SQLException {
    checkNotNull(mode);
    switch (mode) {
      case READ:
        try {
          closeLock.readLock().lockInterruptibly();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION).setRootCause(e)
            .build().buildException();
        }
        break;
      case WRITE:
        try {
          closeLock.writeLock().lockInterruptibly();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION).setRootCause(e)
            .build().buildException();
        }
    }
  }

  private void unlock(PhoenixTestDriver.LockMode mode) {
    checkNotNull(mode);
    switch (mode) {
      case READ:
        closeLock.readLock().unlock();
        break;
      case WRITE:
        closeLock.writeLock().unlock();
    }
  }
}
