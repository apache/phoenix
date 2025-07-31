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
package org.apache.phoenix.end2end.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class ServerSideImmutableIndexIT extends BaseImmutableIndexIT {

  public ServerSideImmutableIndexIT(boolean localIndex, boolean transactional,
    String transactionProvider, boolean columnEncoded) {
    super(localIndex, transactional, transactionProvider, columnEncoded, true);
  }

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> serverProps = createServerProps();
    Map<String, String> clientProps = createClientProps();
    clientProps.put(QueryServices.SERVER_SIDE_IMMUTABLE_INDEXES_ENABLED_ATTRIB, "true");
    driver = null;
    setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
      new ReadOnlyProps(clientProps.entrySet().iterator()));
  }

  // name is used by failsafe as file name in reports
  @Parameters(
      name = "ServerSideImmutableIndexIT_localIndex={0},transactional={1},transactionProvider={2},columnEncoded={3}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { false, false, null, false },
      { false, false, null, true },
      // OMID does not support local indexes or column encoding
      { false, true, "OMID", false }, { true, false, null, false }, { true, false, null, true }, });
  }

}
