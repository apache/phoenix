/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.minikdc.MiniKdc;

import java.io.File;
import java.net.BindException;
import java.util.Properties;

public class KdcUtil extends HBaseCommonTestingUtility {

    private static final Log LOG = LogFactory.getLog(KdcUtil.class);

    public MiniKdc setupMiniKdc(File keytabFile) throws Exception {
        Properties conf = MiniKdc.createConf();
        conf.put(MiniKdc.DEBUG, true);
        MiniKdc kdc = null;
        File dir = null;
        // There is time lag between selecting a port and trying to bind with it. It's possible that
        // another service captures the port in between which'll result in BindException.
        boolean bindException;
        int numTries = 0;
        do {
            try {
                bindException = false;
                dir = new File(getDataTestDir("kdc").toUri().getPath());
                kdc = new MiniKdc(conf, dir);
                kdc.start();
            } catch (BindException e) {
                FileUtils.deleteDirectory(dir);  // clean directory
                numTries++;
                if (numTries == 3) {
                    LOG.error("Failed setting up MiniKDC. Tried " + numTries + " times.");
                    throw e;
                }
                LOG.error("BindException encountered when setting up MiniKdc. Trying again.");
                bindException = true;
            }
        } while (bindException);
        HBaseKerberosUtils.setKeytabFileForTesting(keytabFile.getAbsolutePath());
        return kdc;
    }

}
