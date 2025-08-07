/*
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
package org.apache.phoenix.hbase.index.write;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTrackingParallelWriterIndexCommitter extends TrackingParallelWriterIndexCommitter {
  private static final Logger LOGGER =
    LoggerFactory.getLogger(TestTrackingParallelWriterIndexCommitter.class);

  private HTableFactory testFactory;

  public static class TestConnectionFactory extends ServerUtil.ConnectionFactory {

    private static Map<ServerUtil.ConnectionType, Connection> connections =
      new ConcurrentHashMap<ServerUtil.ConnectionType, Connection>();

    public static Connection getConnection(final ServerUtil.ConnectionType connectionType,
      final RegionCoprocessorEnvironment env) {
      final String key =
        String.format("%s-%s", env.getServerName(), connectionType.name().toLowerCase());
      LOGGER.info("Connecting to {}", key);
      return connections.computeIfAbsent(connectionType,
        new Function<ServerUtil.ConnectionType, Connection>() {
          @Override
          public Connection apply(ServerUtil.ConnectionType t) {
            try {
              return env.createConnection(
                getTypeSpecificConfiguration(connectionType, env.getConfiguration()));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
    }

    public static void shutdown() {
      synchronized (ServerUtil.ConnectionFactory.class) {
        for (Connection connection : connections.values()) {
          try {
            LOGGER.info("Closing connection to {}", connection.getClusterId());
            connection.close();
          } catch (IOException e) {
            LOGGER.warn("Unable to close coprocessor connection", e);
          }
        }
        connections.clear();
      }
    }

    public static int getConnectionsCount() {
      return connections.size();
    }

  }

  public static class TestCoprocessorHConnectionTableFactory
    extends IndexWriterUtils.CoprocessorHConnectionTableFactory {

    @GuardedBy("TestCoprocessorHConnectionTableFactory.this")
    private RegionCoprocessorEnvironment env;
    private ServerUtil.ConnectionType connectionType;

    TestCoprocessorHConnectionTableFactory(RegionCoprocessorEnvironment env,
      ServerUtil.ConnectionType connectionType) {
      super(env, connectionType);
      this.env = env;
      this.connectionType = connectionType;
    }

    @Override
    public Connection getConnection() throws IOException {
      return TestConnectionFactory.getConnection(connectionType, env);
    }

    @Override
    public synchronized void shutdown() {
      TestConnectionFactory.shutdown();
    }
  }

  @Override
  void setup(HTableFactory factory, ExecutorService pool, Stoppable stop,
    RegionCoprocessorEnvironment env) {
    LOGGER.info("Setting up TestCoprocessorHConnectionTableFactory ");
    testFactory = new TestCoprocessorHConnectionTableFactory(env,
      ServerUtil.ConnectionType.INDEX_WRITER_CONNECTION_WITH_CUSTOM_THREADS);
    super.setup(testFactory, pool, stop, env);

  }

  @Override
  public void stop(String why) {
    LOGGER.info("Stopping TestTrackingParallelWriterIndexCommitter " + why);
    testFactory.shutdown();
    super.stop(why);
  }
}
