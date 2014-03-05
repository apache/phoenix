package org.apache.phoenix.end2end;

import org.junit.Ignore;
import org.junit.Test;

public class TenantSpecificViewIndexSaltedTest extends BaseTenantSpecificViewIndexTest {
    private static final Integer SALT_BUCKETS = 3;
    
    @Test @Ignore("PHOENIX-110")
    public void testUpdatableSaltedView() throws Exception {
        testUpdatableView(SALT_BUCKETS);
    }
    
    @Test@Ignore("PHOENIX-110")
    public void testUpdatableViewsWithSameNameDifferentTenants() throws Exception {
        testUpdatableViewsWithSameNameDifferentTenants(SALT_BUCKETS);
    }
}
