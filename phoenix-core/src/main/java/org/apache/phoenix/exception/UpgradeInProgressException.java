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
package org.apache.phoenix.exception;


public class UpgradeInProgressException extends RetriableUpgradeException {
    public UpgradeInProgressException(String upgradeFrom, String upgradeTo) {
        super("Cluster is being concurrently upgraded from " + upgradeFrom + " to " + upgradeTo
                + ". Please retry establishing connection.", SQLExceptionCode.CONCURRENT_UPGRADE_IN_PROGRESS
                .getSQLState(), SQLExceptionCode.CONCURRENT_UPGRADE_IN_PROGRESS.getErrorCode());
    }
}