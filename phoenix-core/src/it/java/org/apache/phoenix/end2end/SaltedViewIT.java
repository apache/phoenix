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

import org.junit.Test;


public class SaltedViewIT extends BaseViewIT {
    
    /**
     * Salted tests must be in their own test file to ensure that the underlying
     * table is dropped. Otherwise, the splits may not be performed.
     * TODO: we should throw in that case
     * 
     * @throws Exception
     */
    @Test
    public void testSaltedUpdatableViewWithIndex() throws Exception {
        testUpdatableViewWithIndex(3, false);
    }

    @Test
    public void testSaltedUpdatableViewWithLocalIndex() throws Exception {
        testUpdatableViewWithIndex(3, true);
    }
}
