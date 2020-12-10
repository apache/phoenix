/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.pherf.result.impl;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;

import javax.xml.bind.UnmarshalException;
import javax.xml.stream.XMLStreamException;

import org.apache.phoenix.pherf.XMLConfigParserTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XMLResultHandlerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(XMLResultHandlerTest.class);

    @Test
    public void testDTDInResults() throws Exception {
        URL resultsUrl = XMLConfigParserTest.class.getResource("/malicious_results_with_dtd.xml");
        assertNotNull(resultsUrl);
        File resultsFile = new File(resultsUrl.getFile());
        XMLResultHandler handler = new XMLResultHandler();
        try {
          handler.readFromResultFile(resultsFile);
          fail("Expected to see an exception parsing the results with a DTD");
        } catch (UnmarshalException e) {
          // If we don't parse the DTD, the variable 'name' won't be defined in the XML
          LOGGER.debug("Caught expected exception", e);
          Throwable cause = e.getLinkedException();
          assertTrue("Cause was a " + cause.getClass(), cause instanceof XMLStreamException);
        }
    }
}
