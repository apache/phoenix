package org.apache.phoenix.mapreduce.index;

import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;

public class IndexScrutinyMapperForTest extends IndexScrutinyMapper {

    public static final int TEST_TABLE_TTL = 3600;
    public static class ScrutinyTestClock extends EnvironmentEdge {
        long initialTime;
        long delta;

        public ScrutinyTestClock(long delta) {
            initialTime = System.currentTimeMillis() + delta;
            this.delta = delta;
        }

        @Override
        public long currentTime() {
            return System.currentTimeMillis() + delta;
        }
    }

    @Override
    public void preQueryTargetTable() {
        // change the current time past ttl
        ScrutinyTestClock clock = new ScrutinyTestClock(TEST_TABLE_TTL*1000);
        EnvironmentEdgeManager.injectEdge(clock);
    }
}
