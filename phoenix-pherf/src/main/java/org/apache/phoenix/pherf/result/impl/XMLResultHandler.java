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

package org.apache.phoenix.pherf.result.impl;

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.result.file.ResultFileDetails;
import org.apache.phoenix.pherf.result.*;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XMLResultHandler implements ResultHandler {
    private final String resultFileName;
    private final ResultFileDetails resultFileDetails;

    public XMLResultHandler(String resultFileName, ResultFileDetails resultFileDetails) {
        this(resultFileName, resultFileDetails, true);
    }

    public XMLResultHandler(String resultFileName, ResultFileDetails resultFileDetails, boolean generateFullFileName) {
        ResultUtil util = new ResultUtil();
        this.resultFileName = generateFullFileName ?
                PherfConstants.RESULT_DIR + PherfConstants.PATH_SEPARATOR
                        + PherfConstants.RESULT_PREFIX
                        + resultFileName + util.getSuffix()
                        + resultFileDetails.getExtension().toString()
                : resultFileName;
        this.resultFileDetails = resultFileDetails;
    }

    @Override
    public synchronized void write(Result result) throws Exception {
        FileOutputStream os = null;
        JAXBContext jaxbContext = JAXBContext.newInstance(DataModelResult.class);
        Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
        jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        try {
            os = new FileOutputStream(resultFileName);
            ResultValue resultValue = result.getResultValues().get(0);
            jaxbMarshaller.marshal(resultValue.getResultValue(), os);
        } finally {
            if (os != null) {
                os.flush();
                os.close();
            }
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        return;
    }

    @Override
    public synchronized void close() throws IOException {
        return;
    }

    @Override
    public synchronized List<Result> read() throws Exception {

        JAXBContext jaxbContext = JAXBContext.newInstance(DataModelResult.class);
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
        File XMLfile = new File(resultFileName);
        List<ResultValue> resultValue = new ArrayList();
        resultValue.add(new ResultValue<>((DataModelResult) jaxbUnmarshaller.unmarshal(XMLfile)));
        List<Result> results = new ArrayList<>();
        results.add(new Result(ResultFileDetails.XML, null, resultValue));
        return results;
    }

    @Override
    public boolean isClosed() {
        return true;
    }

    @Override
    public ResultFileDetails getResultFileDetails() {
        return resultFileDetails;
    }
}
