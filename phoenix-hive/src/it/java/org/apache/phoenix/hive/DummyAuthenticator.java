/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;

public class DummyAuthenticator implements HiveAuthenticationProvider {

  private final List<String> groupNames;
  private final String userName;
  private Configuration conf;

  public DummyAuthenticator() {
    this.groupNames = new ArrayList<String>();
    groupNames.add("hive_test_group1");
    groupNames.add("hive_test_group2");
    userName = "hive_test_user";
  }

  @Override
  public void destroy() throws HiveException{
    return;
  }

  @Override
  public List<String> getGroupNames() {
    return groupNames;
  }

  @Override
  public String getUserName() {
    return userName;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setSessionState(SessionState ss) {
    //no op
  }

}
