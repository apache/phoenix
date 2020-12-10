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

import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.exception.FileLoaderException;
import org.apache.phoenix.pherf.rules.DataValue;
import org.apache.phoenix.pherf.util.ResourceList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XMLConfigParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(XMLConfigParser.class);
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

    public DataModel getDataModelByName(String name) {
        for (DataModel dataModel : getDataModels()) {
            if (dataModel.getName().equals(name)) {
                return dataModel;
            }
        }
        return null;
    }

    public Scenario getScenarioByName(String name) throws Exception {
        for (Scenario scenario : getScenarios()) {
            if (scenario.getName().equals(name)) {
                return scenario;
            }
        }
        return null;
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
                LOGGER.error("Unable to parse scenario file "+path, e);
                throw e;
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
     * @return {@link org.apache.phoenix.pherf.configuration.DataModel} Returns DataModel from
     * XML configuration
     */
    // TODO Remove static calls
    public static DataModel readDataModel(Path file) throws JAXBException, XMLStreamException {
        XMLInputFactory xif = XMLInputFactory.newInstance();
        xif.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        JAXBContext jaxbContext = JAXBContext.newInstance(DataModel.class);
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
        String fName = PherfConstants.RESOURCE_SCENARIO + "/" + file.getFileName().toString();
        LOGGER.info("Open config file: " + fName);
        XMLStreamReader xmlReader = xif.createXMLStreamReader(
            new StreamSource(XMLConfigParser.class.getResourceAsStream(fName)));
        return (DataModel) jaxbUnmarshaller.unmarshal(xmlReader);
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
        // Remove any quotes that may be needed for multi-tenant tables
        ret = ret.replaceAll("\"", "");
        return ret;
    }

    // TODO Remove static calls
    @SuppressWarnings("unused")
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
            DataModel dataModel = XMLConfigParser.readDataModel(path);
            updateDataValueType(dataModel);
            this.dataModels.add(dataModel);
        }
    }
    
    private void updateDataValueType(DataModel dataModel) {
        for (Column column : dataModel.getDataMappingColumns()) {
            if (column.getDataValues() != null) {
                // DataValue type is inherited from the column
                for (DataValue value : column.getDataValues()) {
                    value.setType(column.getType());
                }
            }
        }
    }

    private Collection<Path> getResources(String pattern) throws Exception {
        return resourceList.getResourceList(pattern);
    }
}
