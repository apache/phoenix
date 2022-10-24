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
package org.apache.phoenix.coprocessor;

import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tephra support has been removed, see PHOENIX-6627. However we preserve a class
 * of this name for now with a no-op implementation, in case the user has not
 * followed proper upgrade or migration procedure for former Tephra managed transactional
 * tables. Although expected but unavailable functionality will be missing, regionservers
 * will not crash due to a failure to load a coprocessor of this name.
 */
public class TephraTransactionalProcessor implements RegionObserver, RegionCoprocessor {

  private static final Logger LOG = LoggerFactory.getLogger(TephraTransactionalProcessor.class);
  static {
    LOG.error("Tephra support has been removed, see https://issues.apache.org/jira/browse/PHOENIX-6627.");
  }

}
