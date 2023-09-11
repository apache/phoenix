/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.monitoring;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class TableHistogramsTest {

    @Test
    public void testTableHistograms() {
        String table = "TEST_TABLE";
        Configuration conf = new Configuration();
        TableHistograms tableHistograms = new TableHistograms(table, conf);
        Assert.assertEquals(table, tableHistograms.getTableName());
        Assert.assertNotNull(tableHistograms.getUpsertLatencyHisto());
        Assert.assertNotNull(tableHistograms.getUpsertSizeHisto());
        Assert.assertNotNull(tableHistograms.getDeleteLatencyHisto());
        Assert.assertNotNull(tableHistograms.getDeleteSizeHisto());
        Assert.assertNotNull(tableHistograms.getQueryLatencyHisto());
        Assert.assertNotNull(tableHistograms.getQuerySizeHisto());
        Assert.assertNotNull(tableHistograms.getPointLookupLatencyHisto());
        Assert.assertNotNull(tableHistograms.getPointLookupSizeHisto());
        Assert.assertNotNull(tableHistograms.getRangeScanLatencyHisto());
        Assert.assertNotNull(tableHistograms.getRangeScanSizeHisto());

        Assert.assertEquals(5, tableHistograms.getTableLatencyHistogramsDistribution().size());
        Assert.assertEquals(5, tableHistograms.getTableSizeHistogramsDistribution().size());
    }

}