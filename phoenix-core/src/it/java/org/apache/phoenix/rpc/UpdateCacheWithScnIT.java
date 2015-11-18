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
package org.apache.phoenix.rpc;

import static org.apache.phoenix.util.TestUtil.MUTABLE_INDEX_DATA_TABLE;

import org.apache.phoenix.end2end.BaseClientManagedTimeIT;
import org.junit.Before;
import org.junit.Test;

public class UpdateCacheWithScnIT extends BaseClientManagedTimeIT {
	
	protected long ts;

	@Before
	public void initTable() throws Exception {
		ts = nextTimestamp();
		ensureTableCreated(getUrl(), MUTABLE_INDEX_DATA_TABLE, ts);
	}
	
	@Test
	public void testUpdateCacheWithScn() throws Exception {
		UpdateCacheIT.helpTestUpdateCache(false, ts+2);
	}

}
