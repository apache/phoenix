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
package org.apache.phoenix.hbase.index.util;


public class VersionUtil {
    private VersionUtil() {
    }

    // Encode a version string in the format of "major.minor.patch" into an integer.
    public static int encodeVersion(String version) {
        String[] versionParts = VersionUtil.splitHBaseVersionString(version);
        return VersionUtil.encodeVersion(versionParts[0], versionParts.length > 1 ? versionParts[1] : null, versionParts.length > 2 ? versionParts[2] : null);
    }

    public static String[] splitHBaseVersionString(String version) {
        return version.split("[-\\.]");
    }

    // Encode the major as 2nd byte in the int, minor as the first byte and patch as the last byte.
    public static int encodeVersion(String major, String minor, String patch) {
        return encodeVersion(major == null ? 0 : Integer.parseInt(major), minor == null ? 0 : Integer.parseInt(minor), 
                        patch == null ? 0 : Integer.parseInt(patch));
    }

    public static int encodeVersion(int major, int minor, int patch) {
        int version = 0;
        version |= (major << Byte.SIZE * 2);
        version |= (minor << Byte.SIZE);
        version |= patch;
        return version;
    }

    public static int encodeMaxPatchVersion(int major, int minor) {
        int version = 0;
        version |= (major << Byte.SIZE * 2);
        version |= (minor << Byte.SIZE);
        version |= 0xFF;
        return version;
    }

    public static int encodeMinPatchVersion(int major, int minor) {
        int version = 0;
        version |= (major << Byte.SIZE * 2);
        version |= (minor << Byte.SIZE);
        return version;
    }

    public static int encodeMaxMinorVersion(int major) {
        int version = 0;
        version |= (major << Byte.SIZE * 2);
        version |= 0xFFFF;
        return version;
    }

    public static int encodeMinMinorVersion(int major) {
        int version = 0;
        version |= (major << Byte.SIZE * 2);
        return version;
    }
}
