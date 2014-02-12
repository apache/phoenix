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
package org.apache.phoenix.util;

import static org.junit.Assert.*;

import org.junit.Test;


public class MetaDataUtilTest {

    @Test
    public void testEncode() {
        assertEquals(MetaDataUtil.encodeVersion("0.94.5"),MetaDataUtil.encodeVersion("0.94.5-mapR"));
        assertTrue(MetaDataUtil.encodeVersion("0.94.6")>MetaDataUtil.encodeVersion("0.94.5-mapR"));
        assertTrue(MetaDataUtil.encodeVersion("0.94.6")>MetaDataUtil.encodeVersion("0.94.5"));
        assertTrue(MetaDataUtil.encodeVersion("0.94.1-mapR")>MetaDataUtil.encodeVersion("0.94"));
    }
    
    @Test
    public void testCompatibility() {
        assertTrue(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(1,2,1), 1, 2));
        assertTrue(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(1,2,10), 1, 2));
        assertTrue(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(1,2,0), 1, 2));
        assertTrue(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(1,2,255), 1, 2));
        assertFalse(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(2,2,0), 2, 1));
        assertFalse(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(3,1,10), 4, 2));
    }
}
