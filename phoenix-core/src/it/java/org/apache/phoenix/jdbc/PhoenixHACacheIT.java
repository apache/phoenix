package org.apache.phoenix.jdbc;

import org.apache.curator.framework.CuratorFramework;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.junit.Assert.assertFalse;

/**
 * Integration tests for {@link PhoenixHACache}
 */
@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixHACacheIT extends BaseTest {

    private final PhoenixHAAdmin haAdmin = new PhoenixHAAdmin(config);
    private static final Long CACHE_TTL_MS = 30*1000L;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put("phoenix.ha.cache.ttl.ms", String.valueOf(CACHE_TTL_MS));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void before() throws Exception {
        // Clean up all the existing CRRs
        List<ClusterRoleRecord> crrs = haAdmin.listAllClusterRoleRecordsOnZookeeper();
        for (ClusterRoleRecord crr : crrs) {
            haAdmin.getCurator().delete().forPath(toPath(crr.getHaGroupName()));
        }
    }

    @Test
    public void testHACacheWithSingleCRR() throws Exception {
        PhoenixHACache phoenixHACache = PhoenixHACache.getInstance(config);
        ClusterRoleRecord crr = new ClusterRoleRecord("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 1L);
        haAdmin.createOrUpdateDataOnZookeeper(crr);
        Thread.sleep(1000);
        assertFalse(phoenixHACache.isClusterInActiveToStandby());

        // Now Update CRR so that current cluster has state ACTIVE_TO_STANDBY
        crr = new ClusterRoleRecord("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 2L);
        haAdmin.createOrUpdateDataOnZookeeper(crr);

        Thread.sleep(1000);
        // Check that now the cluster should be in ActiveToStandby
        assert phoenixHACache.isClusterInActiveToStandby();


        // Change it back to ACTIVE so that cluster is not in ACTIVE_TO_STANDBY state
        crr = new ClusterRoleRecord("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 3L);
        haAdmin.createOrUpdateDataOnZookeeper(crr);

        Thread.sleep(1000);
        assertFalse(phoenixHACache.isClusterInActiveToStandby());


        // Change it again to ACTIVE_TO_STANDBY so that we can validate watcher works repeatedly
        crr = new ClusterRoleRecord("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 4L);
        haAdmin.createOrUpdateDataOnZookeeper(crr);

        Thread.sleep(1000);
        assert phoenixHACache.isClusterInActiveToStandby();


        // Change peer cluster to ACTIVE_TO_STANDBY so that we can still process mutation on this cluster
        crr = new ClusterRoleRecord("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE,
                "random-zk-url", ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, 5L);
        haAdmin.createOrUpdateDataOnZookeeper(crr);

        Thread.sleep(1000);
        assertFalse(phoenixHACache.isClusterInActiveToStandby());
    }


    @Test
    public void testHACacheWithMultipleCRRs() throws Exception {
        PhoenixHACache phoenixHACache = PhoenixHACache.getInstance(config);
        // Setup initial CRRs
        ClusterRoleRecord crr1 = new ClusterRoleRecord("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 1L);
        ClusterRoleRecord crr2 = new ClusterRoleRecord("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 1L);
        haAdmin.createOrUpdateDataOnZookeeper(crr1);
        haAdmin.createOrUpdateDataOnZookeeper(crr2);

        Thread.sleep(1000);
        assertFalse(phoenixHACache.isClusterInActiveToStandby());

        // Now Update CRR so that current cluster has state ACTIVE_TO_STANDBY for only 1 crr
        crr1 = new ClusterRoleRecord("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 2L);
        crr2 = new ClusterRoleRecord("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 2L);
        haAdmin.createOrUpdateDataOnZookeeper(crr1);
        haAdmin.createOrUpdateDataOnZookeeper(crr2);

        Thread.sleep(1000);
        // Check that now the cluster should be in ActiveToStandby
        assert phoenixHACache.isClusterInActiveToStandby();


        // Change it back to ACTIVE so that cluster is not in ACTIVE_TO_STANDBY state
        crr1 = new ClusterRoleRecord("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 3L);
        crr2 = new ClusterRoleRecord("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 3L);
        haAdmin.createOrUpdateDataOnZookeeper(crr1);
        haAdmin.createOrUpdateDataOnZookeeper(crr2);

        Thread.sleep(1000);
        assertFalse(phoenixHACache.isClusterInActiveToStandby());


        // Change other crr to ACTIVE_TO_STANDBY and one in ACTIVE state so that we can validate watcher works repeatedly
        crr1 = new ClusterRoleRecord("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 4L);
        crr2 = new ClusterRoleRecord("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 4L);
        haAdmin.createOrUpdateDataOnZookeeper(crr1);
        haAdmin.createOrUpdateDataOnZookeeper(crr2);

        Thread.sleep(1000);
        assert phoenixHACache.isClusterInActiveToStandby();


        // Change peer cluster to ACTIVE_TO_STANDBY so that we can still process mutation on this cluster
        crr1 = new ClusterRoleRecord("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE,
                "random-zk-url", ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, 5L);
        crr2 = new ClusterRoleRecord("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE,
                "random-zk-url", ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, 5L);
        haAdmin.createOrUpdateDataOnZookeeper(crr1);
        haAdmin.createOrUpdateDataOnZookeeper(crr2);

        Thread.sleep(1000);
        assertFalse(phoenixHACache.isClusterInActiveToStandby());
    }

    @Test
    public void testHACacheWithCacheExpiration() throws Exception {
        PhoenixHACache phoenixHACache = PhoenixHACache.getInstance(config);
        ClusterRoleRecord crr1 = new ClusterRoleRecord("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 1L);
        ClusterRoleRecord crr2 = new ClusterRoleRecord("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 1L);
        haAdmin.createOrUpdateDataOnZookeeper(crr1);
        haAdmin.createOrUpdateDataOnZookeeper(crr2);
        Thread.sleep(1000);
        assert phoenixHACache.isClusterInActiveToStandby();
        //Now we delete one of the CRR which is in ACTIVE_TO_STANDBY state
        haAdmin.getCurator().delete().forPath(toPath("parallel"));
        Thread.sleep(1000);
        assertFalse(phoenixHACache.isClusterInActiveToStandby());


        //Now we add 1 new CRR which is in ACTIVE_TO_STANDBY state
        ClusterRoleRecord crr = new ClusterRoleRecord("newcrr",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 1L);
        haAdmin.createOrUpdateDataOnZookeeper(crr);

        // We sleep for time greater than CACHE_TTL_MS so that cache is entirely refreshed with any new entries
        Thread.sleep(CACHE_TTL_MS + (10*1000));
        phoenixHACache.cleanupCache();
        assert phoenixHACache.isClusterInActiveToStandby();

        // Now we don't change anything wrt ZK but we still wait for Cache to expire
        Thread.sleep(CACHE_TTL_MS + (10*1000));
        phoenixHACache.cleanupCache();
        assert phoenixHACache.isClusterInActiveToStandby();
    }


    private static CuratorFramework getCurator() throws IOException {
        String zkurl = "127.0.0.1:" + getZKClientPort(config);
        return HighAvailabilityGroup.getCurator(zkurl, new Properties());
    }


}
