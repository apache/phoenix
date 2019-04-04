package org.apache.phoenix.end2end;

import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.junit.BeforeClass;
import org.junit.Test;

import java.security.PrivilegedExceptionAction;

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
}
