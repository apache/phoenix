package org.apache.phoenix.end2end;

import org.junit.BeforeClass;

public class PermissionNSDisabledIT extends BasePermissionsIT {

    public PermissionNSDisabledIT() throws Exception {
        super(false);
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        BasePermissionsIT.initCluster(false);
    }
}
