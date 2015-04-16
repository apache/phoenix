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

package org.apache.phoenix.pherf.schema;

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.exception.FileLoaderException;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.util.ResourceList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Collection;

public class SchemaReader {
    private static final Logger logger = LoggerFactory.getLogger(SchemaReader.class);
    private final PhoenixUtil pUtil = new PhoenixUtil();
    private Collection<Path> resourceList;
    private final String searchPattern;
    private final ResourceList resourceUtil;

    /**
     * Used for testing search Pattern
     * @param searchPattern {@link java.util.regex.Pattern} that matches a resource on the CP
     * @throws Exception
     */
    public SchemaReader(final String searchPattern) throws Exception {
        this.searchPattern = searchPattern;
        this.resourceUtil = new ResourceList(PherfConstants.RESOURCE_DATAMODEL);
        read();
    }

    public Collection<Path> getResourceList() {
        return resourceList;
    }

    public void applySchema() throws Exception {
        Connection connection = null;
        try {
            connection = pUtil.getConnection();
            for (Path file : resourceList) {
                logger.info("\nApplying schema to file: " + file);
                pUtil.executeStatement(resourceToString(file), connection);
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public String resourceToString(final Path file) throws Exception {
        String fName = PherfConstants.RESOURCE_DATAMODEL + "/" + file.getFileName().toString();
        BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(fName)));
        StringBuffer sb = new StringBuffer();

        String line;
        while ((line = br.readLine()) != null) {
            sb.append(line);
        }

        return sb.toString();
    }

    private void read() throws Exception {
        logger.debug("Trying to match resource pattern: " + searchPattern);
        System.out.println("Trying to match resource pattern: " + searchPattern);

        resourceList = null;
        resourceList = resourceUtil.getResourceList(searchPattern);
        logger.info("File resourceList Loaded: " + resourceList);
        System.out.println("File resourceList Loaded: " + resourceList);
        if (resourceList.isEmpty()) {
            throw new FileLoaderException("Could not load Schema Files");
        }
    }
}