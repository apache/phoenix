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
package org.apache.phoenix.end2end;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class TenantSpecificViewIndexSaltedIT extends BaseTenantSpecificViewIndexIT {

    private final Integer saltBuckets;

    public TenantSpecificViewIndexSaltedIT(Integer saltBuckets) {
        this.saltBuckets = saltBuckets;
    }

    @Parameterized.Parameters(name = "TenantSpecificViewIndexSaltedIT_SaltBuckets={0}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {{3}, {13}, {39}});
    }

    @Test
    public void testUpdatableSaltedView() throws Exception {
        testUpdatableView(saltBuckets);
    }

    @Test
    public void testUpdatableViewsWithSameNameDifferentTenants() throws Exception {
        testUpdatableViewsWithSameNameDifferentTenants(saltBuckets);
    }

    @Test
    public void testUpdatableSaltedViewWithLocalIndex() throws Exception {
        testUpdatableView(saltBuckets, true);
    }

    @Test
    public void testUpdatableViewsWithSameNameDifferentTenantsWithLocalIndex() throws Exception {
        testUpdatableViewsWithSameNameDifferentTenants(saltBuckets, true);
    }
}
