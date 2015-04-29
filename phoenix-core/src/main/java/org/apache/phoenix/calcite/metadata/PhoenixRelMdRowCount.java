package org.apache.phoenix.calcite.metadata;

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.phoenix.calcite.rel.PhoenixAbstractAggregate;
import org.apache.phoenix.calcite.rel.PhoenixAbstractSort;
import org.apache.phoenix.calcite.rel.PhoenixLimit;

public class PhoenixRelMdRowCount {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                BuiltInMethod.ROW_COUNT.method, new PhoenixRelMdRowCount());

    public Double getRowCount(Aggregate rel) {
        if (PhoenixAbstractAggregate.isSingleValueCheckAggregate(rel)) {
            return RelMetadataQuery.getRowCount(rel.getInput());
        }
        
        ImmutableBitSet groupKey = rel.getGroupSet();
        // rowcount is the cardinality of the group by columns
        Double distinctRowCount =
                RelMetadataQuery.getDistinctRowCount(
                        rel.getInput(),
                        groupKey,
                        null);
        if (distinctRowCount == null) {
            return rel.getRows();
        } else {
            return distinctRowCount;
        }
    }
    
    public Double getRowCount(PhoenixAbstractSort rel) {
        return rel.getRows();
      }
    
    public Double getRowCount(PhoenixLimit rel) {
        return rel.getRows();
      }
}
