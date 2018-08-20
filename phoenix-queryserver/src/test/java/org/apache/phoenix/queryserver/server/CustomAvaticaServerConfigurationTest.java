package org.apache.phoenix.queryserver.server;

import org.apache.calcite.avatica.server.AvaticaServerConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class CustomAvaticaServerConfigurationTest {
    @Test
    public void testDefaultFactory() throws IOException {
        QueryServer queryServer = new QueryServer();
        UserGroupInformation ugi = queryServer.getUserGroupInformation();
        // the default factory creates null object
        AvaticaServerConfiguration avaticaServerConfiguration = queryServer.createAvaticaServerConfig(new Configuration(), ugi);
        Assert.assertNull(avaticaServerConfiguration);
    }
}
