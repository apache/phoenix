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

package org.apache.phoenix.pherf.configuration;

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.exception.FileLoaderException;
import org.apache.phoenix.pherf.util.ResourceList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class XMLConfigParser {

    private static final Logger logger = LoggerFactory.getLogger(XMLConfigParser.class);
    private String filePattern;
    private List<DataModel> dataModels;
    private List<Scenario> scenarios = null;
    private ResourceList resourceList;
    private Collection<Path> paths = null;

    public XMLConfigParser(String pattern) throws Exception {
        init(pattern);
    }

    public List<DataModel> getDataModels() {
        return dataModels;
    }

    public synchronized Collection<Path> getPaths(String strPattern) throws Exception {
        if (paths != null) {
            return paths;
        }
        paths = getResources(strPattern);
        return paths;
    }

    public synchronized List<Scenario> getScenarios() throws Exception {
        if (scenarios != null) {
            return scenarios;
        }

        scenarios = Collections.synchronizedList(new ArrayList<Scenario>());
        for (Path path : getPaths(getFilePattern())) {
            try {
                List<Scenario> scenarioList = XMLConfigParser.readDataModel(path).getScenarios();
                for (Scenario scenario : scenarioList) {
                    scenarios.add(scenario);
                }
            } catch (JAXBException e) {
                e.printStackTrace();
            }
        }
        return scenarios;
    }

    public String getFilePattern() {
        return filePattern;
    }

    /**
     * Unmarshall an XML data file
     *
     * @param file Name of File
     * @return
     * @throws JAXBException
     */
    // TODO Remove static calls
    public static DataModel readDataModel(Path file) throws JAXBException {
        JAXBContext jaxbContext = JAXBContext.newInstance(DataModel.class);
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
        String fName = PherfConstants.RESOURCE_SCENARIO + "/" + file.getFileName().toString();
        logger.info("Open config file: " + fName);
        return (DataModel) jaxbUnmarshaller
                .unmarshal(XMLConfigParser.class.getResourceAsStream(fName));
    }

    // TODO Remove static calls
    public static String parseSchemaName(String fullTableName) {
        String ret = null;
        if (fullTableName.contains(".")) {
            ret = fullTableName.substring(0, fullTableName.indexOf("."));
        }
        return ret;
    }

    // TODO Remove static calls
    public static String parseTableName(String fullTableName) {
        String ret = fullTableName;
        if (fullTableName.contains(".")) {
            ret = fullTableName.substring(fullTableName.indexOf(".") + 1, fullTableName.length());
        }
        return ret;
    }

    // TODO Remove static calls
    public static void writeDataModel(DataModel data, OutputStream output) throws JAXBException {
        // create JAXB context and initializing Marshaller
        JAXBContext jaxbContext = JAXBContext.newInstance(DataModel.class);
        Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

        // for getting nice formatted output
        jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

        // Writing to console
        jaxbMarshaller.marshal(data, output);
    }

    private void init(String pattern) throws Exception {
        if (dataModels != null) {
            return;
        }
        this.filePattern = pattern;
        this.dataModels = new ArrayList<>();
        this.resourceList = new ResourceList(PherfConstants.RESOURCE_SCENARIO);
        this.paths = getResources(this.filePattern);
        if (this.paths.isEmpty()) {
            throw new FileLoaderException(
                    "Could not load the resource files using the pattern: " + pattern);
        }
        for (Path path : this.paths) {
            System.out.println("Adding model for path:" + path.toString());
            this.dataModels.add(XMLConfigParser.readDataModel(path));
        }
    }

    private Collection<Path> getResources(String pattern) throws Exception {
        Collection<Path> resourceFiles = new ArrayList<Path>();
        resourceFiles = resourceList.getResourceList(pattern);
        return resourceFiles;
    }
}
