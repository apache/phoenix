package org.apache.phoenix.spark.datasource.v2.writer;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

public class PhoenixDatasourceWriter implements DataSourceWriter {

    private final PhoenixDataSourceWriteOptions options;

    public PhoenixDatasourceWriter(PhoenixDataSourceWriteOptions options) {
        this.options = options;
    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new PhoenixDataWriterFactory(options);
    }

    @Override
    public boolean useCommitCoordinator() {
        return false;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
    }
}
