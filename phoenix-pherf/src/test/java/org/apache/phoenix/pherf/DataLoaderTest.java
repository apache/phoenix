/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf;

import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.loaddata.DataLoader;
import org.apache.phoenix.pherf.util.RowCalculator;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

public class DataLoaderTest extends BaseTestWithCluster {
    private static XMLConfigParser parser = null;
    private static DataModel model = null;
    private DataLoader loader = null;

    @BeforeClass
    public static void init() {

        try {
            parser = new XMLConfigParser(matcherScenario);
            model = parser.getDataModels().get(0);
        } catch (Exception e) {
            fail("Failed to initialize test: " + e.getMessage());
        }
    }

    /**
     * Test rows divide evenly with large rows and small threadpool
     * @throws Exception
     */
    @Test
    public void testRowsEvenDivide() throws Exception {
        int threadPoolSize = 10;
        int tableRowCount = 100;
        assertRowsSum(threadPoolSize, tableRowCount);
    }

    /**
     * Test rows add up when not divided evenly with large rows and small threadpool
     *
     * @throws Exception
     */
    @Test
    public void testRowsNotEvenDivide() throws Exception {
        int threadPoolSize = 9;
        int tableRowCount = 100;
        assertRowsSum(threadPoolSize, tableRowCount);
    }

    /**
     * Test rows add up when not divided evenly with large threadpool and small rowcount
     *
     * @throws Exception
     */
    @Test
    public void testRowsNotEvenDivideSmallRC() throws Exception {
        int threadPoolSize = 50;
        int tableRowCount = 21;
        assertRowsSum(threadPoolSize, tableRowCount);
    }

    /**
     * Test rows count equal to thread pool
     *
     * @throws Exception
     */
    @Test
    public void testRowsEqualToPool() throws Exception {
        int threadPoolSize = 50;
        int tableRowCount = 50;
        assertRowsSum(threadPoolSize, tableRowCount);
    }

    private void assertRowsSum(int threadPoolSize, int tableRowCount) {
        int sum = 0;
        RowCalculator rc = new RowCalculator(threadPoolSize, tableRowCount);
        assertEquals("Rows generated did not match expected count! ", threadPoolSize, rc.size());

        // Sum of all rows should equal expected row count
        for (int i = 0; i < threadPoolSize; i++) {
            sum += rc.getNext();
        }
        assertEquals("Rows did not sum up correctly", tableRowCount, sum);

        // Ensure rows were removed from list
        assertEquals(rc.size(), 0);
    }
}
