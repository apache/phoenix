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
package org.apache.phoenix.tracingwebapp.http;

import java.net.URL;
import java.security.ProtectionDomain;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 * tracing web app runner
 */
public final class Main extends Configured implements Tool {

    protected static final Log LOG = LogFactory.getLog(Main.class);
    public static final String PHONIX_DBSERVER_PORT_KEY =
        "phoenix.dbserver.port";
    public static final int DEFAULT_DBSERVER_PORT = 2181;
    public static final String PHONIX_DBSERVER_HOST_KEY =
        "phoenix.dbserver.host";
    public static final String DEFAULT_DBSERVER_HOST = "localhost";
    public static final String TRACE_SERVER_HTTP_PORT_KEY =
            "phoenix.traceserver.http.port";
    public static final int DEFAULT_HTTP_PORT = 8864;
    public static final String TRACE_SERVER_HTTP_JETTY_HOME_KEY =
            "phoenix.traceserver.http.home";
    public static final String DEFAULT_HTTP_HOME = "/";

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(HBaseConfiguration.create(), new Main(), args);
        System.exit(ret);
    }

    @Override
    public int run(String[] arg0) throws Exception {
        // logProcessInfo(getConf());
        final int port = getConf().getInt(TRACE_SERVER_HTTP_PORT_KEY,
                DEFAULT_HTTP_PORT);
        BasicConfigurator.configure();
        final String home = getConf().get(TRACE_SERVER_HTTP_JETTY_HOME_KEY,
                DEFAULT_HTTP_HOME);
        //setting up the embedded server
        ProtectionDomain domain = Main.class.getProtectionDomain();
        URL location = domain.getCodeSource().getLocation();
        String webappDirLocation = location.toString().split("target")[0] +"src/main/webapp";
        Server server = new Server(port);
        WebAppContext root = new WebAppContext();

        root.setContextPath(home);
        root.setDescriptor(webappDirLocation + "/WEB-INF/web.xml");
        root.setResourceBase(webappDirLocation);
        root.setParentLoaderPriority(true);
        server.setHandler(root);

        server.start();
        server.join();
        return 0;
    }
}
