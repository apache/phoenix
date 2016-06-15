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
package org.apache.phoenix;

import org.apache.phoenix.queryserver.client.ThinClientUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Ensure the "thick" Phoenix driver and it's "thin" counterpart can coexist on
 * the same classpath.
 */
public class DriverCohabitationTest {

  @Test
  public void testDriverCohabitation() throws SQLException {
    Driver thickDriver = null;
    Driver thinDriver = null;

    for (Driver d : Collections.list(DriverManager.getDrivers())) {
      if (d instanceof org.apache.phoenix.jdbc.PhoenixDriver) {
        thickDriver = d;
      } else if (d instanceof org.apache.phoenix.queryserver.client.Driver) {
        thinDriver = d;
      }
    }
    assertNotNull("Thick driver not registered with DriverManager.", thickDriver);
    assertNotNull("Thin driver not registered with DriverManager.", thinDriver);

    final String thickUrl = QueryUtil.getUrl("localhost");
    final String thinUrl = ThinClientUtil.getConnectionUrl("localhost", 1234);
    assertTrue("Thick driver should accept connections like " + thickUrl,
        thickDriver.acceptsURL(thickUrl));
    assertFalse("Thick driver should reject connections like " + thinUrl,
        thickDriver.acceptsURL(thinUrl));
    assertTrue("Thin driver should accept connections like " + thinUrl,
        thinDriver.acceptsURL(thinUrl));
    assertFalse("Thin driver should reject connections like " + thickUrl,
        thinDriver.acceptsURL(thickUrl));
  }
}
