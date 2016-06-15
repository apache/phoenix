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
package org.apache.phoenix.queryserver.client;

import org.apache.calcite.avatica.DriverVersion;

public class Driver extends org.apache.calcite.avatica.remote.Driver {

  public static final String CONNECT_STRING_PREFIX = "jdbc:phoenix:thin:";

  static {
    new Driver().register();
  }

  public Driver() {
    super();
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(
        Driver.class,
        "org-apache-phoenix-remote-jdbc.properties",
        "Phoenix Remote JDBC Driver",
        "unknown version",
        "Apache Phoenix",
        "unknown version");
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }
}
