package org.apache.phoenix.end2end.prefix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

@Category(NeedsOwnMiniClusterTest.class)
public class LongViewIndexEnabledBaseRowKeyPrefixIT extends BaseRowKeyPrefixTestIT {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        final Configuration conf = HBaseConfiguration.create();
        conf.set(QueryServices.PHOENIX_TABLE_TTL_ENABLED, String.valueOf(true));
        conf.set(QueryServices.LONG_VIEW_INDEX_ENABLED_ATTRIB, String.valueOf(true));
        conf.set(QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB, "true");
        conf.set(IndexManagementUtil.WAL_EDIT_CODEC_CLASS_KEY, "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec");

        // Clear the cached singletons so we can inject our own.
        InstanceResolver.clearSingletons();
        // Make sure the ConnectionInfo doesn't try to pull a default Configuration
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
            @Override
            public Configuration getConfiguration() {
                return conf;
            }

            @Override
            public Configuration getConfiguration(Configuration confToClone) {
                Configuration copy = new Configuration(conf);
                copy.addResource(confToClone);
                return copy;
            }
        });

        Map<String, String> DEFAULT_PROPERTIES = new HashMap() ;
        setUpTestDriver(new ReadOnlyProps(DEFAULT_PROPERTIES.entrySet().iterator()));
    }

    @Override
    protected boolean hasLongViewIndexEnabled() {
        return true;
    }
}
