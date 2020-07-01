package org.apache.phoenix.hbase.index;


import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.phoenix.hbase.index.builder.IndexBuilder;
import org.apache.phoenix.hbase.index.covered.IndexCodec;
import org.apache.phoenix.hbase.index.covered.NonTxIndexBuilder;
import org.apache.phoenix.hbase.index.write.IndexFailurePolicy;
import org.apache.phoenix.hbase.index.write.recovery.StoreFailuresInCachePolicy;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The IndexerTest
 *
 * @since 2020/6/14
 */
public class IndexerTest {
	private static final Log LOG = LogFactory.getLog(IndexerTest.class);

	@Test
	public void testSplitFailedRollBackOpen() throws Exception {
		ObserverContext<RegionCoprocessorEnvironment> ctx = Mockito.mock(ObserverContext.class);
		RegionCoprocessorEnvironment env = Mockito.mock(RegionCoprocessorEnvironment.class);
		RegionServerServices regionServer = Mockito.mock(RegionServerServices.class);
		ServerName serverName = Mockito.mock(ServerName.class);
		Region region = Mockito.mock(Region.class);
		Mockito.when(env.getRegionServerServices()).thenReturn(regionServer);
		Mockito.when(regionServer.getServerName()).thenReturn(serverName);
		Mockito.when(serverName.getServerName()).thenReturn("testrs");
		ConcurrentMap<String, Object> hashMap = new ConcurrentHashMap<>();
		Mockito.when(env.getSharedData()).thenReturn(hashMap);
		Configuration conf = Mockito.mock(Configuration.class);
		Mockito.when(env.getRegion()).thenReturn(region);
		Mockito.when(env.getConfiguration()).thenReturn(conf);
		Mockito.when(conf.getBoolean(Mockito.anyString(),Mockito.anyBoolean())).thenReturn(false);
		Mockito.when(conf.iterator()).thenReturn(new Configuration().iterator());
		conf.set(NonTxIndexBuilder.CODEC_CLASS_NAME_KEY, PhoenixIndexCodec.class.getName());
		Class c = Class.forName("org.apache.phoenix.index.PhoenixIndexCodec");
		Mockito.when(conf.getClass(NonTxIndexBuilder.CODEC_CLASS_NAME_KEY, null, IndexCodec.class)).thenReturn(c);
		Class c1 = Class.forName("org.apache.phoenix.hbase.index.covered.NonTxIndexBuilder");
		Mockito.when(conf.getClass(Indexer.INDEX_BUILDER_CONF_KEY, null, IndexBuilder.class)).thenReturn(c1);
		Class c2 = Class.forName("org.apache.phoenix.hbase.index.write.recovery.StoreFailuresInCachePolicy");
		Mockito.when(conf.getClass("org.apache.hadoop.hbase.index.recovery.failurepolicy",
			StoreFailuresInCachePolicy.class, IndexFailurePolicy.class)).thenReturn(c2);

		Mockito.when(ctx.getEnvironment()).thenReturn(env);
		Class<?> ungroupClass = (Class<Indexer>) Class.forName("org.apache.phoenix.hbase.index.Indexer");
		Object observer = ungroupClass.newInstance();
		Indexer index = (Indexer) observer;

		Method method = ungroupClass.getDeclaredMethod("initBuilderAndWriter", RegionCoprocessorEnvironment.class);
		method.setAccessible(true);
		method.invoke(observer,env);

		Field fieldTag = ungroupClass.getDeclaredField("stopped");
		fieldTag.setAccessible(true);
		Boolean stopped = (Boolean) fieldTag.get(observer);
		assertTrue("init Writer success after open region", !stopped);
	}

}
