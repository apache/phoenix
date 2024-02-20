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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.util.Collection;

import org.apache.phoenix.pherf.util.ResourceList;
import org.junit.Test;

public class ResourceTest {
    @Test
    public void testSchemaResourceList() throws Exception{
        String extension = ".sql";
        assertResources(PherfConstants.SCHEMA_ROOT_PATTERN + extension, PherfConstants.RESOURCE_DATAMODEL, extension);
    }

    @Test
    public void testScenarioResourceList() throws Exception {
        String extension = ".xml";
        assertResources(PherfConstants.TEST_SCENARIO_ROOT_PATTERN + extension, PherfConstants.RESOURCE_SCENARIO, extension);
    }

    @Test
    public void testResourceListPropertyDirectory() throws Exception {
        PherfConstants constants = PherfConstants.create();
        assertNotNull(constants.getProperty("pherf.default.dataloader.threadpool"));
        assertNotNull(constants.getProperty("pherf.default.results.dir"));
        assertNotNull(constants.getProperty(PherfConstants.LOG_PER_NROWS_NAME));
    }

    private Collection<Path> assertResources(String pattern, String rootDir, String assertStr) throws Exception {
        ResourceList list = new ResourceList(rootDir);
        Collection<Path> paths =
                list.getResourceList(pattern);
        assertTrue("Resource file list was empty", paths.size() > 0);
        for (Path path : paths) {
            assertTrue(path.toString().contains(assertStr));
        }
        return paths;
    }
}