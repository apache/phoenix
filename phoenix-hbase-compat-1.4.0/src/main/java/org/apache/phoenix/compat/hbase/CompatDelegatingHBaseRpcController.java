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
package org.apache.phoenix.compat.hbase;

import org.apache.hadoop.hbase.ipc.DelegatingHBaseRpcController;

// We need to copy the HBase implementation, because we need to have CompatHBaseRpcController
// as ancestor, so we cannot simply subclass the HBase Delegating* class
public abstract class CompatDelegatingHBaseRpcController extends DelegatingHBaseRpcController
        implements CompatHBaseRpcController {

    public CompatDelegatingHBaseRpcController(CompatHBaseRpcController delegate) {
        super(delegate);
    }
}
