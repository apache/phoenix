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
package org.apache.phoenix.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests ConnectionQueryServices caching when Kerberos authentication is enabled. It's not
 * trivial to directly test this, so we exploit the knowledge that the caching is driven by
 * a ConcurrentHashMap. We can use a HashSet to determine when instances of ConnectionInfo
 * collide and when they do not.
 */
public class SecureUserConnectionsTest {
    private static final Log LOG = LogFactory.getLog(SecureUserConnectionsTest.class); 
    private static final int KDC_START_ATTEMPTS = 10;

    private static final File TEMP_DIR = new File(getClassTempDir());
    private static final File KEYTAB_DIR = new File(TEMP_DIR, "keytabs");
    private static final File KDC_DIR = new File(TEMP_DIR, "kdc");
    private static final List<File> USER_KEYTAB_FILES = new ArrayList<>();
    private static final List<File> SERVICE_KEYTAB_FILES = new ArrayList<>();
    private static final int NUM_USERS = 3;
    private static final Properties EMPTY_PROPERTIES = new Properties();
    private static final String BASE_URL = PhoenixRuntime.JDBC_PROTOCOL + ":localhost:2181";

    private static MiniKdc KDC;

    @BeforeClass
    public static void setupKdc() throws Exception {
        ensureIsEmptyDirectory(KDC_DIR);
        ensureIsEmptyDirectory(KEYTAB_DIR);
        // Create and start the KDC. MiniKDC appears to have a race condition in how it does
        // port allocation (with apache-ds). See PHOENIX-3287.
        boolean started = false;
        for (int i = 0; !started && i < KDC_START_ATTEMPTS; i++) {
            Properties kdcConf = MiniKdc.createConf();
            kdcConf.put(MiniKdc.DEBUG, true);
            KDC = new MiniKdc(kdcConf, KDC_DIR);
            try {
                KDC.start();
                started = true;
            } catch (Exception e) {
                LOG.warn("PHOENIX-3287: Failed to start KDC, retrying..", e);
            }
        }
        assertTrue("The embedded KDC failed to start successfully after " + KDC_START_ATTEMPTS
                + " attempts.", started);

        createUsers(NUM_USERS);
        createServiceUsers(NUM_USERS);

        final Configuration conf = new Configuration(false);
        conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        conf.set(User.HBASE_SECURITY_CONF_KEY, "kerberos");
        conf.setBoolean(User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY, true);
        UserGroupInformation.setConfiguration(conf);

        // Clear the cached singletons so we can inject our own.
        InstanceResolver.clearSingletons();
        // Make sure the ConnectionInfo doesn't try to pull a default Configuration
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
            @Override
            public Configuration getConfiguration() {
                return conf;
            }
            @Override
            public Configuration getConfiguration(Configuration confToClone) {
                Configuration copy = new Configuration(conf);
                copy.addResource(confToClone);
                return copy;
            }
        });
        updateDefaultRealm();
    }

    private static void updateDefaultRealm() throws Exception {
        // (at least) one other phoenix test triggers the caching of this field before the KDC is up
        // which causes principal parsing to fail.
        Field f = KerberosName.class.getDeclaredField("defaultRealm");
        f.setAccessible(true);
        // Default realm for MiniKDC
        f.set(null, "EXAMPLE.COM");
    }

    @AfterClass
    public static void stopKdc() throws Exception {
        // Remove our custom ConfigurationFactory for future tests
        InstanceResolver.clearSingletons();
        if (null != KDC) {
            KDC.stop();
            KDC = null;
        }
    }

    private static String getClassTempDir() {
        StringBuilder sb = new StringBuilder(32);
        sb.append(System.getProperty("user.dir")).append(File.separator);
        sb.append("target").append(File.separator);
        sb.append(SecureUserConnectionsTest.class.getSimpleName());
        return sb.toString();
    }

    private static void ensureIsEmptyDirectory(File f) throws IOException {
        if (f.exists()) {
            if (f.isDirectory()) {
                FileUtils.deleteDirectory(f);
            } else {
                assertTrue("Failed to delete keytab directory", f.delete());
            }
        }
        assertTrue("Failed to create keytab directory", f.mkdirs());
    }

    private static void createUsers(int numUsers) throws Exception {
        assertNotNull("KDC is null, was setup method called?", KDC);
        for (int i = 1; i <= numUsers; i++) {
            String principal = "user" + i;
            File keytabFile = new File(KEYTAB_DIR, principal + ".keytab");
            KDC.createPrincipal(keytabFile, principal);
            USER_KEYTAB_FILES.add(keytabFile);
        }
    }

    private static void createServiceUsers(int numUsers) throws Exception {
        assertNotNull("KDC is null, was setup method called?", KDC);
        for (int i = 1; i <= numUsers; i++) {
            String principal = "user" + i + "/localhost";
            File keytabFile = new File(KEYTAB_DIR, "user" + i + ".service.keytab");
            KDC.createPrincipal(keytabFile, principal);
            SERVICE_KEYTAB_FILES.add(keytabFile);
        }
    }

    /**
     * Returns the principal for a user.
     *
     * @param offset The "number" user to return, based on one, not zero.
     */
    private static String getUserPrincipal(int offset) {
        return "user" + offset + "@" + KDC.getRealm();
    }

    private static String getServicePrincipal(int offset) {
        return "user" + offset + "/localhost@" + KDC.getRealm();
    }

    /**
     * Returns the keytab file for the corresponding principal with the same {@code offset}.
     * Requires {@link #createUsers(int)} to have been called with a value greater than {@code offset}.
     *
     * @param offset The "number" for the principal whose keytab should be returned. One-based, not zero-based.
     */
    public static File getUserKeytabFile(int offset) {
        return getKeytabFile(offset, USER_KEYTAB_FILES);
    }

    public static File getServiceKeytabFile(int offset) {
        return getKeytabFile(offset, SERVICE_KEYTAB_FILES);
    }

    private static File getKeytabFile(int offset, List<File> keytabs) {
        assertTrue("Invalid offset: " + offset, (offset - 1) >= 0 && (offset - 1) < keytabs.size());
        return keytabs.get(offset - 1);
    }

    private String joinUserAuthentication(String origUrl, String principal, File keytab) {
        StringBuilder sb = new StringBuilder(64);
        // Knock off the trailing terminator if one exists
        if (origUrl.charAt(origUrl.length() - 1) == PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR) {
            sb.append(origUrl, 0, origUrl.length() - 1);
        } else {
            sb.append(origUrl);
        }

        sb.append(PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR).append(principal);
        sb.append(PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR).append(keytab.getPath());
        return sb.append(PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR).toString();
    }

    @Test
    public void testMultipleInvocationsBySameUserAreEquivalent() throws Exception {
        final HashSet<ConnectionInfo> connections = new HashSet<>();
        final String princ1 = getUserPrincipal(1);
        final File keytab1 = getUserKeytabFile(1);

        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(princ1, keytab1.getPath());

        PrivilegedExceptionAction<Void> callable = new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                String url = joinUserAuthentication(BASE_URL, princ1, keytab1);
                connections.add(ConnectionInfo.create(url).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
                return null;
            }
        };

        // Using the same UGI should result in two equivalent ConnectionInfo objects
        ugi.doAs(callable);
        assertEquals(1, connections.size());
        verifyAllConnectionsAreKerberosBased(connections);

        ugi.doAs(callable);
        assertEquals(1, connections.size());
        verifyAllConnectionsAreKerberosBased(connections);
    }

    @Test
    public void testMultipleUniqueUGIInstancesAreDisjoint() throws Exception {
        final HashSet<ConnectionInfo> connections = new HashSet<>();
        final String princ1 = getUserPrincipal(1);
        final File keytab1 = getUserKeytabFile(1);

        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(princ1, keytab1.getPath());

        PrivilegedExceptionAction<Void> callable = new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                String url = joinUserAuthentication(BASE_URL, princ1, keytab1);
                connections.add(ConnectionInfo.create(url).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
                return null;
            }
        };

        ugi.doAs(callable);
        assertEquals(1, connections.size());
        verifyAllConnectionsAreKerberosBased(connections);

        // A second, but equivalent, call from the same "real" user but a different UGI instance
        // is expected functionality (programmer error).
        UserGroupInformation ugiCopy = UserGroupInformation.loginUserFromKeytabAndReturnUGI(princ1, keytab1.getPath());
        ugiCopy.doAs(callable);
        assertEquals(2, connections.size());
        verifyAllConnectionsAreKerberosBased(connections);
    }

    @Test
    public void testAlternatingLogins() throws Exception {
        final HashSet<ConnectionInfo> connections = new HashSet<>();
        final String princ1 = getUserPrincipal(1);
        final File keytab1 = getUserKeytabFile(1);
        final String princ2 = getUserPrincipal(2);
        final File keytab2 = getUserKeytabFile(2);

        UserGroupInformation ugi1 = UserGroupInformation.loginUserFromKeytabAndReturnUGI(princ1, keytab1.getPath());
        UserGroupInformation ugi2 = UserGroupInformation.loginUserFromKeytabAndReturnUGI(princ2, keytab2.getPath());

        // Using the same UGI should result in two equivalent ConnectionInfo objects
        ugi1.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                String url = joinUserAuthentication(BASE_URL, princ1, keytab1);
                connections.add(ConnectionInfo.create(url).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
                return null;
            }
        });
        assertEquals(1, connections.size());
        // Sanity check
        verifyAllConnectionsAreKerberosBased(connections);

        ugi2.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                String url = joinUserAuthentication(BASE_URL, princ2, keytab2);
                connections.add(ConnectionInfo.create(url).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
                return null;
            }
        });
        assertEquals(2, connections.size());
        verifyAllConnectionsAreKerberosBased(connections);

        ugi1.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                String url = joinUserAuthentication(BASE_URL, princ1, keytab1);
                connections.add(ConnectionInfo.create(url).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
                return null;
            }
        });
        assertEquals(2, connections.size());
        verifyAllConnectionsAreKerberosBased(connections);
    }

    @Test
    public void testAlternatingDestructiveLogins() throws Exception {
        final HashSet<ConnectionInfo> connections = new HashSet<>();
        final String princ1 = getUserPrincipal(1);
        final File keytab1 = getUserKeytabFile(1);
        final String princ2 = getUserPrincipal(2);
        final File keytab2 = getUserKeytabFile(2);
        final String url1 = joinUserAuthentication(BASE_URL, princ1, keytab1);
        final String url2 = joinUserAuthentication(BASE_URL, princ2, keytab2);

        UserGroupInformation.loginUserFromKeytab(princ1, keytab1.getPath());
        // Using the same UGI should result in two equivalent ConnectionInfo objects
        connections.add(ConnectionInfo.create(url1).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
        assertEquals(1, connections.size());
        // Sanity check
        verifyAllConnectionsAreKerberosBased(connections);

        UserGroupInformation.loginUserFromKeytab(princ2, keytab2.getPath());
        connections.add(ConnectionInfo.create(url2).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
        assertEquals(2, connections.size());
        verifyAllConnectionsAreKerberosBased(connections);

        // Because the UGI instances are unique, so are the connections
        UserGroupInformation.loginUserFromKeytab(princ1, keytab1.getPath());
        connections.add(ConnectionInfo.create(url1).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
        assertEquals(3, connections.size());
        verifyAllConnectionsAreKerberosBased(connections);
    }

    @Test
    public void testMultipleConnectionsAsSameUser() throws Exception {
        final HashSet<ConnectionInfo> connections = new HashSet<>();
        final String princ1 = getUserPrincipal(1);
        final File keytab1 = getUserKeytabFile(1);
        final String url = joinUserAuthentication(BASE_URL, princ1, keytab1);

        UserGroupInformation.loginUserFromKeytab(princ1, keytab1.getPath());
        // Using the same UGI should result in two equivalent ConnectionInfo objects
        connections.add(ConnectionInfo.create(url).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
        assertEquals(1, connections.size());
        // Sanity check
        verifyAllConnectionsAreKerberosBased(connections);

        // Because the UGI instances are unique, so are the connections
        connections.add(ConnectionInfo.create(url).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
        assertEquals(1, connections.size());
    }

    @Test
    public void testMultipleConnectionsAsSameUserWithoutLogin() throws Exception {
        final HashSet<ConnectionInfo> connections = new HashSet<>();
        final String princ1 = getUserPrincipal(1);
        final File keytab1 = getUserKeytabFile(1);

        // Using the same UGI should result in two equivalent ConnectionInfo objects
        final String url = joinUserAuthentication(BASE_URL, princ1, keytab1);
        connections.add(ConnectionInfo.create(url).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
        assertEquals(1, connections.size());
        // Sanity check
        verifyAllConnectionsAreKerberosBased(connections);

        // Because the UGI instances are unique, so are the connections
        connections.add(ConnectionInfo.create(url).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
        assertEquals(1, connections.size());
    }

    @Test
    public void testAlternatingConnectionsWithoutLogin() throws Exception {
        final HashSet<ConnectionInfo> connections = new HashSet<>();
        final String princ1 = getUserPrincipal(1);
        final File keytab1 = getUserKeytabFile(1);
        final String princ2 = getUserPrincipal(2);
        final File keytab2 = getUserKeytabFile(2);
        final String url1 = joinUserAuthentication(BASE_URL, princ1, keytab1);
        final String url2 = joinUserAuthentication(BASE_URL, princ2, keytab2);

        // Using the same UGI should result in two equivalent ConnectionInfo objects
        connections.add(ConnectionInfo.create(url1).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
        assertEquals(1, connections.size());
        // Sanity check
        verifyAllConnectionsAreKerberosBased(connections);

        // Because the UGI instances are unique, so are the connections
        connections.add(ConnectionInfo.create(url2).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
        assertEquals(2, connections.size());
        verifyAllConnectionsAreKerberosBased(connections);

        // Using the same UGI should result in two equivalent ConnectionInfo objects
        connections.add(ConnectionInfo.create(url1).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
        assertEquals(3, connections.size());
        // Sanity check
        verifyAllConnectionsAreKerberosBased(connections);
    }

    @Test
    public void testHostSubstitutionInUrl() throws Exception {
        final HashSet<ConnectionInfo> connections = new HashSet<>();
        final String princ1 = getServicePrincipal(1);
        final File keytab1 = getServiceKeytabFile(1);
        final String princ2 = getServicePrincipal(2);
        final File keytab2 = getServiceKeytabFile(2);
        final String url1 = joinUserAuthentication(BASE_URL, princ1, keytab1);
        final String url2 = joinUserAuthentication(BASE_URL, princ2, keytab2);

        // Using the same UGI should result in two equivalent ConnectionInfo objects
        connections.add(ConnectionInfo.create(url1).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
        assertEquals(1, connections.size());
        // Sanity check
        verifyAllConnectionsAreKerberosBased(connections);

        // Logging in as the same user again should not duplicate connections
        connections.add(ConnectionInfo.create(url1).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
        assertEquals(1, connections.size());
        // Sanity check
        verifyAllConnectionsAreKerberosBased(connections);

        // Add a second one.
        connections.add(ConnectionInfo.create(url2).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
        assertEquals(2, connections.size());
        verifyAllConnectionsAreKerberosBased(connections);

        // Again, verify this user is not duplicated
        connections.add(ConnectionInfo.create(url2).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
        assertEquals(2, connections.size());
        verifyAllConnectionsAreKerberosBased(connections);

        // Because the UGI instances are unique, so are the connections
        connections.add(ConnectionInfo.create(url1).normalize(ReadOnlyProps.EMPTY_PROPS, EMPTY_PROPERTIES));
        assertEquals(3, connections.size());
        verifyAllConnectionsAreKerberosBased(connections);
    }

    private void verifyAllConnectionsAreKerberosBased(Collection<ConnectionInfo> connections) {
        for (ConnectionInfo cnxnInfo : connections) {
            assertTrue("ConnectionInfo does not have kerberos credentials: " + cnxnInfo, cnxnInfo.getUser().getUGI().hasKerberosCredentials());
        }
    }
}
