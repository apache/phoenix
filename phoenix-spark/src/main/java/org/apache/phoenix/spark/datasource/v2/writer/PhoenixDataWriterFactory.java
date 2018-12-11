package org.apache.phoenix.spark.datasource.v2.writer;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;

public class PhoenixDataWriterFactory implements DataWriterFactory<InternalRow> {

    private final PhoenixDataSourceWriteOptions options;

    public PhoenixDataWriterFactory(PhoenixDataSourceWriteOptions options) {
        this.options = options;
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
        return new PhoenixDataWriter(options);
    }
}
