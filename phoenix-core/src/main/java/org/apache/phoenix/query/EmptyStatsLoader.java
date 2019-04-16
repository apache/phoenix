package org.apache.phoenix.query;

import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;

/**
 * {@link PhoenixStatsLoader} implementation for the Stats Loader.
 * Empty stats loader if stats are disabled
 */
class EmptyStatsLoader implements PhoenixStatsLoader {
    @Override
    public boolean needsLoad() {
        return false;
    }

    @Override
    public GuidePostsInfo loadStats(GuidePostsKey statsKey) throws Exception {
        return GuidePostsInfo.NO_GUIDEPOST;
    }

    @Override
    public GuidePostsInfo loadStats(GuidePostsKey statsKey, GuidePostsInfo prevGuidepostInfo) throws Exception {
        return GuidePostsInfo.NO_GUIDEPOST;
    }
}
