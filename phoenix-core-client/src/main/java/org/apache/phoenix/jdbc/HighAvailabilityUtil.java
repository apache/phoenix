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

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_STALE_CRR_DETECTED_COUNT;

import java.sql.SQLException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.phoenix.exception.MutationBlockedIOException;
import org.apache.phoenix.exception.ReplicationFailoverInProgressException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.exception.StaleClusterRoleRecordException;
import org.apache.phoenix.monitoring.HAGroupMetricsManager;
import org.apache.phoenix.monitoring.MetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High Availability Utility class.
 */
public class HighAvailabilityUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityUtil.class);

  public static boolean isStaleClusterRoleRecordExceptionExistsInThrowable(Throwable e) {
    if (e == null) {
      return false;
    }
    if (isGivenThrowableStaleException(e)) {
      return true;
    }

    if (e instanceof RetriesExhaustedWithDetailsException) {
      for (Throwable t : ((RetriesExhaustedWithDetailsException) e).getCauses()) {
        if (isGivenThrowableStaleException(t)) {
          return true;
        }
      }
    }

    if (e.getCause() != null) {
      return isStaleClusterRoleRecordExceptionExistsInThrowable(e.getCause());
    }

    return false;
  }

  /**
   * Walks the throwable chain (including {@link RetriesExhaustedWithDetailsException} causes) to
   * detect a {@link MutationBlockedIOException} surface. Mirrors the structure of
   * {@link #isStaleClusterRoleRecordExceptionExistsInThrowable(Throwable)} so that batched mutation
   * rejections wrapped at varying depth are still attributable to the mutation-block gate.
   */
  public static boolean isMutationBlockedIOExceptionExistsInThrowable(Throwable e) {
    if (e == null) {
      return false;
    }
    if (isGivenThrowableMutationBlockedException(e)) {
      return true;
    }

    if (e instanceof RetriesExhaustedWithDetailsException) {
      for (Throwable t : ((RetriesExhaustedWithDetailsException) e).getCauses()) {
        if (isGivenThrowableMutationBlockedException(t)) {
          return true;
        }
      }
    }

    if (e.getCause() != null) {
      return isMutationBlockedIOExceptionExistsInThrowable(e.getCause());
    }

    return false;
  }

  /**
   * Walks the throwable chain (including {@link RetriesExhaustedWithDetailsException} causes) to
   * detect a {@link ReplicationFailoverInProgressException}: a write committed on the old active
   * whose replication failed during a consistent failover, so its outcome on the peer is unknown.
   */
  public static boolean isReplicationFailoverInProgressExceptionExistsInThrowable(Throwable e) {
    if (e == null) {
      return false;
    }
    if (e instanceof ReplicationFailoverInProgressException) {
      return true;
    }
    if (e instanceof RetriesExhaustedWithDetailsException) {
      for (Throwable t : ((RetriesExhaustedWithDetailsException) e).getCauses()) {
        if (t instanceof ReplicationFailoverInProgressException) {
          return true;
        }
      }
    }
    return isReplicationFailoverInProgressExceptionExistsInThrowable(e.getCause());
  }

  /**
   * If {@code e} on an HA connection carries a {@link StaleClusterRoleRecordException}, force a CRR
   * refresh (which closes the connection when its cluster is no longer active) and return
   * FAILOVER_IN_PROGRESS with {@code e} as the cause; otherwise return {@code e} unchanged.
   */
  public static SQLException toFailoverInProgressIfStale(PhoenixConnection conn, SQLException e) {
    HighAvailabilityGroup haGroup = conn == null ? null : conn.getHAGroup();
    if (
      haGroup == null || e.getErrorCode() == SQLExceptionCode.FAILOVER_IN_PROGRESS.getErrorCode()
        || !isStaleClusterRoleRecordExceptionExistsInThrowable(e)
    ) {
      return e;
    }
    GLOBAL_HA_STALE_CRR_DETECTED_COUNT.increment();
    HAGroupMetricsManager.increment(haGroup.getName(), MetricType.HA_STALE_CRR_DETECTED_COUNT);
    try {
      if (!haGroup.refreshClusterRoleRecord(true)) {
        LOG.error("Failed to refresh cluster role record for HAGroup {}", haGroup.getName());
      }
    } catch (SQLException refreshError) {
      LOG.error("Failed to refresh cluster role record for HAGroup {}", haGroup.getName(),
        refreshError);
      e.setNextException(refreshError);
    }
    return new SQLExceptionInfo.Builder(SQLExceptionCode.FAILOVER_IN_PROGRESS)
      .setMessage(String.format("Operation failed because failover is in progress for HAGroup %s",
        haGroup.getName()))
      .setRootCause(e).setHaGroupInfo(haGroup.getGroupInfo().toString()).build().buildException();
  }

  private static boolean isGivenThrowableStaleException(Throwable t) {
    return t instanceof StaleClusterRoleRecordException;
  }

  private static boolean isGivenThrowableMutationBlockedException(Throwable t) {
    return t instanceof MutationBlockedIOException;
  }
}
