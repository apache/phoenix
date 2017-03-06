/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.iterate;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.CLOSED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.LOCK_NOT_ACQUIRED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.RENEWED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.THRESHOLD_NOT_REACHED;

import java.sql.SQLException;

public class RenewLeaseOnlyTableIterator extends TableResultIterator {

    private final int numberOfLeaseRenewals;
    private final int thresholdNotReachedAt;
    private final int failToAcquireLockAt;
    private final int failLeaseRenewalAt;
    private int counter = 0;
    private RenewLeaseStatus lastRenewLeaseStatus;

    public RenewLeaseOnlyTableIterator(int renewLeaseCount, int skipRenewLeaseAt, int failToAcquireLockAt, int doNotRenewLeaseAt) throws SQLException {
        super();
        checkArgument(renewLeaseCount >= skipRenewLeaseAt);
        this.numberOfLeaseRenewals = renewLeaseCount;
        this.thresholdNotReachedAt = skipRenewLeaseAt;
        this.failToAcquireLockAt = failToAcquireLockAt;
        this.failLeaseRenewalAt = doNotRenewLeaseAt;
    }

    @Override
    public RenewLeaseStatus renewLease() {
        counter++;
        if (counter == thresholdNotReachedAt) {
            lastRenewLeaseStatus = THRESHOLD_NOT_REACHED;
        } else if (counter == failLeaseRenewalAt) {
            lastRenewLeaseStatus = null;
            throw new RuntimeException("Failing lease renewal");
        } else if (counter == failToAcquireLockAt) {
            lastRenewLeaseStatus = LOCK_NOT_ACQUIRED;
        } else if (counter <= numberOfLeaseRenewals) {
            lastRenewLeaseStatus = RENEWED;
        } else {
            lastRenewLeaseStatus = CLOSED;
        }
        return lastRenewLeaseStatus;
    }

    public RenewLeaseStatus getLastRenewLeaseStatus() {
        return lastRenewLeaseStatus;
    }

}
