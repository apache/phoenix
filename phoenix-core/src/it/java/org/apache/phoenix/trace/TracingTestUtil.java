package org.apache.phoenix.trace;

import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.phoenix.metrics.Metrics;

/**
 *
 */
public class TracingTestUtil {

    public static void registerSink(MetricsSink sink){
        Metrics.initialize().register("phoenix", "test sink gets logged", sink);
    }
}
