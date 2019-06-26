package org.apache.phoenix.end2end;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PermissionNSEnabledIT extends BasePermissionsIT {

    public PermissionNSEnabledIT() throws Exception {
        super(true);
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        BasePermissionsIT.initCluster(true);
    }

    @Test
    public void testSchemaPermissions() throws Throwable{
        try {
            grantSystemTableAccess();
            final String schemaName = "S_" + generateUniqueName();
            superUser1.runAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    try {
                        AccessControlClient.grant(getUtility().getConnection(), regularUser1.getShortName(),
                                Permission.Action.ADMIN);
                    } catch (Throwable e) {
                        if (e instanceof Exception) {
                            throw (Exception)e;
                        } else {
                            throw new Exception(e);
                        }
                    }
                    return null;
                }
            });
            verifyAllowed(createSchema(schemaName), regularUser1);
            // Unprivileged user cannot drop a schema
            verifyDenied(dropSchema(schemaName), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(createSchema(schemaName), AccessDeniedException.class, unprivilegedUser);

            verifyAllowed(dropSchema(schemaName), regularUser1);
        } finally {
            revokeAll();
        }
    }

    @Test
    public void testConnectionCreationFailsWhenNoExecPermsOnSystemCatalog() throws Throwable {
        try {
            grantSystemTableAccess();
            superUser1.runAs((PrivilegedExceptionAction<Object>) () -> {
                TableName systemCatalogTableName =
                        TableName.valueOf(SchemaUtil.getPhysicalHBaseTableName(
                                SYSTEM_SCHEMA_NAME, SYSTEM_CATALOG_TABLE, true).getString());
                try {
                    // Revoke Exec permissions for SYSTEM CATALOG for the unprivileged user
                    AccessControlClient.revoke(getUtility().getConnection(), systemCatalogTableName,
                            unprivilegedUser.getShortName(), null, null, Permission.Action.EXEC);
                } catch (Throwable t) {
                    if (t instanceof Exception) {
                        throw (Exception)t;
                    } else {
                        throw new Exception(t);
                    }
                }
                return null;
            });
            unprivilegedUser.runAs((PrivilegedExceptionAction<Void>) () -> {
                try (Connection ignored = getConnection()) {
                    // We expect this to throw a wrapped AccessDeniedException.
                    fail("Should have failed with a wrapped AccessDeniedException");
                } catch (Throwable ex) {
                    assertTrue("Should not get an incompatible jars exception",
                            ex instanceof SQLException && ((SQLException)ex).getErrorCode() !=
                                    SQLExceptionCode.INCOMPATIBLE_CLIENT_SERVER_JAR.getErrorCode());
                    assertTrue("Expected a wrapped AccessDeniedException",
                            ex.getCause() instanceof AccessDeniedException);
                }
                return null;
            });
        } finally {
            revokeAll();
        }
    }
}
