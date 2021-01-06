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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.pherf.exception.PherfException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * list resources available from the classpath @ *
 */
public class ResourceList {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceList.class);
    private final String rootResourceDir;
    // Lists the directories to ignore meant for testing something else
    // when getting the resources from classpath
    private List<String> dirsToIgnore = Lists.newArrayList("sql_files");

    public ResourceList(String rootResourceDir) {
        this.rootResourceDir = rootResourceDir;
    }

    public Collection<Path> getResourceList(final String pattern) throws Exception {
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
        Set<String> strResources = new HashSet<>();
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

            LOGGER.debug("Trying with the root append.");
            url = ResourceList.class.getResource(rName);
            if (url == null) {
                LOGGER.debug("Failed! Must be using a jar. Trying without the root append.");
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
            LOGGER.debug("Found the correct resource: " + path.toString());
            paths.add(path);
        }

        Collections.sort((List<Path>)paths);
        return paths;
    }

    private Collection<String> getResources(
            final String element,
            final Pattern pattern) {
        final List<String> retVal = new ArrayList<>();
        if (StringUtils.isBlank(element)) {
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

    // Visible for testing
    Collection<String> getResourcesFromJarFile(
            final File file,
            final Pattern pattern) {
        final List<String> retVal = new ArrayList<>();
        ZipFile zf;
        try {
            zf = new ZipFile(file);
        } catch (FileNotFoundException|NoSuchFileException e) {
            // Gracefully handle a jar listed on the classpath that doesn't actually exist.
            return Collections.emptyList();
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
            LOGGER.trace("fileName:" + fileName);
            LOGGER.trace("File:" + file.toString());
            LOGGER.trace("Match:" + accept);
            if (accept) {
                LOGGER.trace("Adding File from Jar: " + fileName);
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
            if (isIgnoredDir(file.getAbsolutePath())) continue;
            if (file.isDirectory()) {
                retval.addAll(getResourcesFromDirectory(file, pattern));
            } else {
                final String fileName = file.getName();
                final boolean accept = pattern.matcher(file.toString()).matches();
                if (accept) {
                    LOGGER.debug("Adding File from directory: " + fileName);
                    retval.add("/" + fileName);
                }
            }
        }
        return retval;
    }
    
    private boolean isIgnoredDir(String path) {
        for (String dir : dirsToIgnore) {
            if (path.contains(dir)) return true;
        }
        return false;
    }
}