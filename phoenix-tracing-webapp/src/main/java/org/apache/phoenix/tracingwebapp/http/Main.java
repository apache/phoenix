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

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.URL;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 *
 * tracing web app runner
 *
 * @since 4.5.5
 */
public final class Main extends Configured implements Tool{

	protected static final Log LOG = LogFactory.getLog(Main.class);
	public static final String TRACE_SERVER_HTTP_PORT_KEY =
		      "phoenix.traceserver.http.port";
	public static final String DEFAULT_HTTP_PORT = "8865";
	public static final String TRACE_SERVER_ENV_LOGGING_KEY =
	          "phoenix.traceserver.envvars.logging.disabled";
	public static final String TRACE_SERVER_ENV_LOGGING_SKIPWORDS_KEY =
	          "phoenix.traceserver.envvars.logging.skipwords";
	
	  @SuppressWarnings("serial")
	  private static final Set<String> DEFAULT_SKIP_WORDS = new HashSet<String>() {
	    {
	      add("secret");
	      add("passwd");
	      add("password");
	      add("credential");
	    }
	  };
	  
    private Main() {
    }

	public static void logJVMInfo() {
		// Print out vm stats before starting up.
		RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
		if (runtime != null) {
			LOG.info("vmName=" + runtime.getVmName() + ", vmVendor="
					+ runtime.getVmVendor() + ", vmVersion="
					+ runtime.getVmVersion());
			LOG.info("vmInputArguments=" + runtime.getInputArguments());
		}
	}
    public static void main(String[] args) throws Exception {
    	logProcessInfo(getConf());
        final int port = Integer.parseInt(System.getProperty("port", DEFAULT_HTTP_PORT));
        final String home = System.getProperty("home", "");
        Server server = new Server(port);
        ProtectionDomain domain = Main.class.getProtectionDomain();
        URL location = domain.getCodeSource().getLocation();
        WebAppContext webapp = new WebAppContext();
        webapp.setContextPath("/");
        if (home.length() != 0) {
            webapp.setTempDirectory(new File(home));
        }
        //To-DO build war file and added in here
        webapp.setWar(location.toExternalForm());
        server.setHandler(webapp);
        server.start();
        server.join();
    }

    /**
     * Logs information about the currently running JVM process including
     * the environment variables. Logging of env vars can be disabled by
     * setting {@code "phoenix.envvars.logging.disabled"} to {@code "true"}.
     * <p>If enabled, you can also exclude environment variables containing
     * certain substrings by setting {@code "phoenix.envvars.logging.skipwords"}
     * to comma separated list of such substrings.
     */
    public static void logProcessInfo(Configuration conf) {
      // log environment variables unless asked not to
      if (conf == null || !conf.getBoolean(TRACE_SERVER_ENV_LOGGING_KEY, false)) {
        Set<String> skipWords = new HashSet<String>(DEFAULT_SKIP_WORDS);
        if (conf != null) {
          String[] confSkipWords = conf.getStrings(TRACE_SERVER_ENV_LOGGING_SKIPWORDS_KEY);
          if (confSkipWords != null) {
            skipWords.addAll(Arrays.asList(confSkipWords));
          }
        }

        nextEnv:
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
          String key = entry.getKey().toLowerCase();
          String value = entry.getValue().toLowerCase();
          // exclude variables which may contain skip words
          for(String skipWord : skipWords) {
            if (key.contains(skipWord) || value.contains(skipWord))
              continue nextEnv;
          }
          LOG.info("env:"+entry);
        }
      }
      // and JVM info
      logJVMInfo();
    }

	@Override
	public int run(String[] arg0) throws Exception {
		logProcessInfo(getConf());
		return 0;
	}
}
