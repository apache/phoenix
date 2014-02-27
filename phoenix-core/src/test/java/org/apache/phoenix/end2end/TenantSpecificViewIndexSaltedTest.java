package org.apache.phoenix.end2end;

import org.junit.Test;

public class TenantSpecificViewIndexSaltedTest extends BaseTenantSpecificViewIndexTest {
    private static final Integer SALT_BUCKETS = 3;
    
    @Test
    public void testUpdatableSaltedView() throws Exception {
        testUpdatableView(SALT_BUCKETS);
    }

}
