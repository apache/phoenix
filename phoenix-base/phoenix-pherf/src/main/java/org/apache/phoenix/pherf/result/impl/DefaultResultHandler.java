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
import org.apache.phoenix.pherf.result.ResultHandler;
import org.apache.phoenix.pherf.result.ResultUtil;
import org.apache.phoenix.pherf.result.file.ResultFileDetails;

public abstract class DefaultResultHandler implements ResultHandler{
    protected String resultFileName;
    protected ResultFileDetails resultFileDetails;
    protected final String resultDir;
    protected final ResultUtil util;

    public DefaultResultHandler() {
        util = new ResultUtil();
        PherfConstants constants = PherfConstants.create();
        this.resultDir = constants.getProperty("pherf.default.results.dir");
    }

    /**
     * {@link DefaultResultHandler#setResultFileDetails(ResultFileDetails)} Must be called prior to
     * setting the file name. Otherwise you will get a NPE.
     *
     * TODO Change this so NPE is not possible. Needs a bit of refactoring here
     *
     * @param resultFileName Base name of file
     */
    @Override
    public void setResultFileName(String resultFileName) {
        this.resultFileName =
                resultDir + PherfConstants.PATH_SEPARATOR + PherfConstants.RESULT_PREFIX
                        + resultFileName + util.getSuffix() + getResultFileDetails()
                        .getExtension().toString();
    }

    @Override
    public void setResultFileDetails(ResultFileDetails details) {
        this.resultFileDetails = details;
    }

    @Override
    public String getResultFileName() {
        return resultFileName;
    }

    @Override
    public ResultFileDetails getResultFileDetails() {
        return resultFileDetails;
    }
}
