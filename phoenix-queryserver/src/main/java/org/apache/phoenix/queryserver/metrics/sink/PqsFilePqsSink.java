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

package org.apache.phoenix.queryserver.metrics.sink;


import org.apache.phoenix.queryserver.metrics.PqsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.io.FileOutputStream;
import java.io.PrintStream;


public class PqsFilePqsSink extends PqsSink {
    private static final String FILENAME_KEY = "filename";
    private PrintStream writer;
    private static final Logger LOG = LoggerFactory.getLogger(PqsFilePqsSink.class);
    private String filename = PqsConfiguration.getFileSinkFilename();

    public PqsFilePqsSink() {
        try {
            writer = filename == null ? System.out
                    : new PrintStream(new FileOutputStream(new File(filename)),
                    true, "UTF-8");
        } catch (FileNotFoundException e) {
            LOG.error("Error creating "+ filename, e);
        } catch (UnsupportedEncodingException e) {
            LOG.error("Error creating "+ filename, e);
        }
    }

    @Override
    public void close()  {
        writer.close();
    }


    @Override
    public void writeJson(String json){
        if (json != null)
        {
            writer.print(json);
        }
        writer.println();
    }

}
