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

package org.apache.phoenix.pherf.util;

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.exception.PherfException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

/**
 * list resources available from the classpath @ *
 */
public class ResourceList {
    private static final Logger logger = LoggerFactory.getLogger(ResourceList.class);
    private final String rootResourceDir;

    public ResourceList() {
        this("/");
    }

    public ResourceList(String rootResourceDir) {
        this.rootResourceDir = rootResourceDir;
    }

    public Collection<Path> getResourceList(final String pattern) throws Exception {
        Properties properties = getProperties();

        // Include files from config directory
        Collection<Path> paths = getResourcesPaths(Pattern.compile(pattern));


        return paths;
    }

    /**
     * for all elements of java.class.path get a Collection of resources Pattern
     * pattern = Pattern.compile(".*"); gets all resources
     *
     * @param pattern the pattern to match
     * @return the resources in the order they are found
     */
    private Collection<Path> getResourcesPaths(
            final Pattern pattern) throws Exception {

        final String classPath = System.getProperty("java.class.path", ".");
        final String[] classPathElements = classPath.split(":");
        List<String> strResources = new ArrayList<>();
        Collection<Path> paths = new ArrayList<>();

        // TODO Make getResourcesPaths() return the URLs directly instead of converting them
        // Get resources as strings.
        for (final String element : classPathElements) {
            strResources.addAll(getResources(element, pattern));
        }

        // Convert resources to URL
        for (String resource : strResources) {
            URL url = null;
            URI uri = null;
            Path path = null;

            String rName = rootResourceDir + resource;

            logger.debug("Trying with the root append.");
            url = ResourceList.class.getResource(rName);
            if (url == null) {
                logger.debug("Failed! Must be using a jar. Trying without the root append.");
                url = ResourceList.class.getResource(resource);

                if (url == null) {
                    throw new PherfException("Could not load resources: " + rName);
                }
                final String[] splits = url.toString().split("!");
                uri = URI.create(splits[0]);
                path = (splits.length < 2) ? Paths.get(uri) : Paths.get(splits[1]);
            } else {
                path = Paths.get(url.toURI());
            }
            logger.debug("Found the correct resource: " + path.toString());
            paths.add(path);
        }

        return paths;
    }

    public Properties getProperties() throws Exception {
        return getProperties(PherfConstants.PHERF_PROPERTIES);
    }

    public Properties getProperties(final String fileName) throws Exception {
        Properties pherfProps = new Properties();
        InputStream is = null;
        try {
            is = getClass().getClassLoader().getResourceAsStream(fileName);
            pherfProps.load(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }
        return pherfProps;
    }

    /**
     * Utility method to check if base result dir exists
     */
    public void ensureBaseDirExists(String directory) {
        File baseDir = new File(directory);
        if (!baseDir.exists()) {
            boolean made = baseDir.mkdir();
            if (!made) {
                logger.error("Could not make directory:" + directory);
            }
        }
    }

    private Collection<String> getResources(
            final String element,
            final Pattern pattern) {
        final List<String> retVal = new ArrayList<>();
        if (element.equals("")) {
            return retVal;
        }
        final File file = new File(element);
        if (file.isDirectory()) {
            retVal.addAll(getResourcesFromDirectory(file, pattern));
        } else {
            retVal.addAll(getResourcesFromJarFile(file, pattern));
        }
        return retVal;
    }

    private Collection<String> getResourcesFromJarFile(
            final File file,
            final Pattern pattern) {
        final List<String> retVal = new ArrayList<>();
        ZipFile zf;
        try {
            zf = new ZipFile(file);
        } catch (final ZipException e) {
            throw new Error(e);
        } catch (final IOException e) {
            throw new Error(e);
        }
        final Enumeration e = zf.entries();
        while (e.hasMoreElements()) {
            final ZipEntry ze = (ZipEntry) e.nextElement();
            final String fileName = ze.getName();
            final boolean accept = pattern.matcher(fileName).matches();
            logger.debug("fileName:" + fileName);
            logger.debug("File:" + file.toString());
            logger.debug("Match:" + accept);
            if (accept) {
                logger.debug("Adding File from Jar: " + fileName);
                retVal.add("/" + fileName);
            }
        }
        try {
            zf.close();
        } catch (final IOException e1) {
            throw new Error(e1);
        }
        return retVal;
    }

    private Collection<String> getResourcesFromDirectory(
            final File directory,
            final Pattern pattern) {
        final ArrayList<String> retval = new ArrayList<String>();
        final File[] fileList = directory.listFiles();
        for (final File file : fileList) {
            if (file.isDirectory()) {
                retval.addAll(getResourcesFromDirectory(file, pattern));
            } else {
                final String fileName = file.getName();
                final boolean accept = pattern.matcher(file.toString()).matches();
                if (accept) {
                    logger.debug("Adding File from directory: " + fileName);
                    retval.add("/" + fileName);
                }
            }
        }
        return retval;
    }
}