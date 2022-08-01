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
 
package org.apache.hadoop.hbase.ipc.controller;

import com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;

public interface ServerToServerRpcController extends RpcController {

    /**
     * @param priority Priority for this request; should fall roughly in the range
     *          {@link HConstants#NORMAL_QOS} to {@link HConstants#HIGH_QOS}
     */
    void setPriority(int priority);

    /**
     * @param tn Set priority based off the table we are going against.
     */
    void setPriority(final TableName tn);

    /**
     * @return The priority of this request
     */
    int getPriority();
}
