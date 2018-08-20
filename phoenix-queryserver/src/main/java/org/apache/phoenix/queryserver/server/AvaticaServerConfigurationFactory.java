package org.apache.phoenix.queryserver.server;

import org.apache.calcite.avatica.server.AvaticaServerConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;


public interface AvaticaServerConfigurationFactory {

    AvaticaServerConfiguration getAvaticaServerConfiguration(Configuration conf, UserGroupInformation ugi);

    class AvaticaServerConfigurationFactoryImpl implements AvaticaServerConfigurationFactory {

        @Override
        public AvaticaServerConfiguration getAvaticaServerConfiguration(Configuration conf, UserGroupInformation ugi) {
            return null;
        }
    }

}
