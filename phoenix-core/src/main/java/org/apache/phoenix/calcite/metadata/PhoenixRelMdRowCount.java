package org.apache.phoenix.calcite.metadata;

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.phoenix.calcite.rel.PhoenixAbstractAggregate;
import org.apache.phoenix.calcite.rel.PhoenixLimit;

public class PhoenixRelMdRowCount implements MetadataHandler<BuiltInMetadata.RowCount> {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                BuiltInMethod.ROW_COUNT.method, new PhoenixRelMdRowCount());
    
    private PhoenixRelMdRowCount() { }
    
    @Override
    public MetadataDef<BuiltInMetadata.RowCount> getDef() {
        return BuiltInMetadata.RowCount.DEF;
    }

    public Double getRowCount(Aggregate rel, RelMetadataQuery mq) {
        if (PhoenixAbstractAggregate.isSingleValueCheckAggregate(rel)) {
            return mq.getRowCount(rel.getInput());
        }
        
        ImmutableBitSet groupKey = rel.getGroupSet();
        // rowcount is the cardinality of the group by columns
        Double distinctRowCount =
                mq.getDistinctRowCount(
                        rel.getInput(),
                        groupKey,
                        null);
        if (distinctRowCount == null) {
            return rel.estimateRowCount(mq);
        } else {
            return distinctRowCount;
        }
    }
    
    public Double getRowCount(PhoenixLimit rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }
}
