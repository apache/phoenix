package org.apache.phoenix.end2end;

import org.junit.Test;

public class TenantSpecificViewIndexSaltedIT extends BaseTenantSpecificViewIndexIT {
    private static final Integer SALT_BUCKETS = 3;
    
    @Test
    public void testUpdatableSaltedView() throws Exception {
        testUpdatableView(SALT_BUCKETS);
    }
    
    @Test
    public void testUpdatableViewsWithSameNameDifferentTenants() throws Exception {
        testUpdatableViewsWithSameNameDifferentTenants(SALT_BUCKETS);
    }
}
