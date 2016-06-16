/**
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
package org.apache.phoenix.hbase.index.balancer;

import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer;

/**
 * <p>This class is an extension of the load balancer class. 
 * It allows to co-locate the regions of the user table and the regions of corresponding
 * index table if any.</p> 
 * 
 * </>roundRobinAssignment, retainAssignment -> index regions will follow the actual table regions. 
 * randomAssignment, balancerCluster -> either index table or actual table region(s) will follow
 * each other based on which ever comes first.</p> 
 * 
 * <p>In case of master failover there is a chance that the znodes of the index
 * table and actual table are left behind. Then in that scenario we may get randomAssignment for
 * either the actual table region first or the index table region first.</p>
 * 
 * <p>In case of balancing by table any table can balance first.</p>
 * 
 */

public class IndexLoadBalancer extends StochasticLoadBalancer {
    
}
