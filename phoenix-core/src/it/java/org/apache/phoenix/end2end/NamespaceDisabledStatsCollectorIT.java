package org.apache.phoenix.end2end;

import org.apache.phoenix.schema.stats.BaseStatsCollectorIT;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

public class NamespaceDisabledStatsCollectorIT extends BaseStatsCollectorIT {

    public NamespaceDisabledStatsCollectorIT(boolean userTableNamespaceMapped, boolean collectStatsOnSnapshot) {
        super(userTableNamespaceMapped, collectStatsOnSnapshot);
    }

    @Parameterized.Parameters(name = "userTableNamespaceMapped={0},collectStatsOnSnapshot={1}")
    public static Collection<Object[]> provideData() {
        return Arrays.asList(
                new Object[][] {
                        // Collect stats on snapshots using UpdateStatisticsTool
                        { false, true },
                        // Collect stats via `UPDATE STATISTICS` SQL
                        { false, false }
                }
        );
    }

}
