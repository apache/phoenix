/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.FileDescriptor;
import java.net.InetAddress;
import java.security.Permission;

/**
 * Custom SecurityManager impl used by SystemExitRule
 */
class TestSecurityManager extends SecurityManager {

    @Override
    public void checkExit(int status) {
        throw new SystemExitRule.SystemExitInTestException();
    }

    @Override
    public void checkPermission(Permission permission) {
        // no-op
    }

    @Override
    public void checkPermission(Permission var1, Object var2) {
        // no-op
    }

    @Override
    public void checkSecurityAccess(String var1) {
        // no-op
    }

    @Override
    public void checkConnect(String var1, int var2, Object var3) {
        // no-op
    }

    @Override
    public void checkWrite(String var1) {
        // no-op
    }

    @Override
    public void checkDelete(String var1) {
        // no-op
    }

    @Override
    public void checkConnect(String var1, int var2) {
        // no-op
    }

    @Override
    public void checkLink(String var1) {
        // no-op
    }

    @Override
    public void checkRead(FileDescriptor var1) {
        // no-op
    }

    @Override
    public void checkAccess(Thread var1) {
        // no-op
    }

    @Override
    public void checkAccess(ThreadGroup var1) {
        // no-op
    }

    @Override
    public void checkCreateClassLoader() {
        // no-op
    }

    @Override
    public void checkListen(int var1) {
        // no-op
    }

    @Override
    public void checkAccept(String var1, int var2) {
        // no-op
    }

    @Override
    public void checkMulticast(InetAddress var1) {
        // no-op
    }

    @Override
    public void checkMulticast(InetAddress var1, byte var2) {
        // no-op
    }

    @Override
    public void checkPropertiesAccess() {
        // no-op
    }

    @Override
    public void checkPropertyAccess(String var1) {
        // no-op
    }

    @Override
    public void checkPackageAccess(String var1) {
        // no-op
    }

    @Override
    public void checkPackageDefinition(String var1) {
        // no-op
    }

    @Override
    public void checkSetFactory() {
        // no-op
    }

}
