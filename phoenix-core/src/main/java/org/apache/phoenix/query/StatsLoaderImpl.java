package org.apache.phoenix.query;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.apache.phoenix.schema.stats.StatisticsUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * {@link PhoenixStatsLoader} implementation for the Stats Loader.
 */
class StatsLoaderImpl implements PhoenixStatsLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(GuidePostsCacheImpl.class);

    @Override
    public boolean needsLoad() {
        // For now, whenever it's called, we try to load stats from stats table
        // no matter it has been updated or not.
        // Here are the possible optimizations we can do here:
        // 1. Load stats from the stats table only when the stats get updated on the server side.
        // 2. Support different refresh cycle for different tables.
        return true;
    }

    @Override
    public GuidePostsInfo loadStats(GuidePostsKey statsKey) throws Exception {
        return loadStats(statsKey, GuidePostsInfo.NO_GUIDEPOST);
    }

    @Override
    public GuidePostsInfo loadStats(GuidePostsKey statsKey, GuidePostsInfo prevGuidepostInfo) throws Exception {
        assert(prevGuidepostInfo != null);

        TableName tableName = SchemaUtil.getPhysicalName(
                PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES,
                queryServices.getProps());
        Table statsHTable = queryServices.getTable(tableName.getName());

        try {
            GuidePostsInfo guidePostsInfo = StatisticsUtil.readStatistics(statsHTable, statsKey,
                    HConstants.LATEST_TIMESTAMP);
            traceStatsUpdate(statsKey, guidePostsInfo);
            return guidePostsInfo;
        } catch (TableNotFoundException e) {
            // On a fresh install, stats might not yet be created, don't warn about this.
            LOGGER.debug("Unable to locate Phoenix stats table: " + tableName.toString(), e);
            return prevGuidepostInfo;
        } catch (IOException e) {
            LOGGER.warn("Unable to read from stats table: " + tableName.toString(), e);
            return prevGuidepostInfo;
        } finally {
            try {
                statsHTable.close();
            } catch (IOException e) {
                // Log, but continue. We have our stats anyway now.
                LOGGER.warn("Unable to close stats table: " + tableName.toString(), e);
            }
        }
    }

    /**
     * Logs a trace message for newly inserted entries to the stats cache.
     */
    void traceStatsUpdate(GuidePostsKey key, GuidePostsInfo info) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Updating local TableStats cache (id={}) for {}, size={}bytes",
                    new Object[] { Objects.hashCode(this), key, info.getEstimatedSize()});
        }
    }
}
