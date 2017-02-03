package org.apache.phoenix.calcite.metadata;

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.phoenix.calcite.PhoenixTable;
import org.apache.phoenix.calcite.rel.Limit;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;

public class PhoenixRelMdSize implements MetadataHandler<BuiltInMetadata.Size> {
    /** Source for
     * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Size}. */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(new PhoenixRelMdSize(),
            BuiltInMethod.AVERAGE_ROW_SIZE.method);

    private PhoenixRelMdSize() { }
    
    @Override
    public MetadataDef<BuiltInMetadata.Size> getDef() {
        return BuiltInMetadata.Size.DEF;
    }
    
    public Double averageRowSize(Union rel, RelMetadataQuery mq) {
        double rowSize = 0;
        for (RelNode input : rel.getInputs()) {
            rowSize += mq.getAverageRowSize(input);
        }
        
        return rowSize / rel.getInputs().size();
    }
    
    public Double averageRowSize(Aggregate rel, RelMetadataQuery mq) {
        RelNode input = rel.getInput();
        double rowSize = mq.getAverageRowSize(input);
        rowSize = rowSize * (rel.getGroupCount() + rel.getAggCallList().size()) / input.getRowType().getFieldCount();
        
        return rowSize;
    }
    
    public Double averageRowSize(Project rel, RelMetadataQuery mq) {
        RelNode input = rel.getInput();
        double rowSize = mq.getAverageRowSize(input);
        rowSize = rowSize * rel.getProjects().size() / input.getRowType().getFieldCount();
        
        return rowSize;
    }

    public Double averageRowSize(Filter rel, RelMetadataQuery mq) {
        return mq.getAverageRowSize(rel.getInput());
    }

    public Double averageRowSize(Correlate rel, RelMetadataQuery mq) {
        return mq.getAverageRowSize(rel.getLeft());
    }

    public Double averageRowSize(SemiJoin rel, RelMetadataQuery mq) {
        return mq.getAverageRowSize(rel.getLeft());
    }

    public Double averageRowSize(Join rel, RelMetadataQuery mq) {
        return mq.getAverageRowSize(rel.getLeft())
                + mq.getAverageRowSize(rel.getRight());
    }

    public Double averageRowSize(Sort rel, RelMetadataQuery mq) {
        return mq.getAverageRowSize(rel.getInput());
    }
    
    public Double averageRowSize(Limit rel, RelMetadataQuery mq) {
        return mq.getAverageRowSize(rel.getInput());
    }

    public Double averageRowSize(Uncollect rel, RelMetadataQuery mq) {
        return mq.getAverageRowSize(rel.getInput());
    }

    public Double averageRowSize(PhoenixTableScan rel, RelMetadataQuery mq) {
        PhoenixTable phoenixTable = rel.getTable().unwrap(PhoenixTable.class);
        return 1.0 * phoenixTable.byteCount / phoenixTable.rowCount;
    }
    
    public Double averageRowSize(RelSubset rel, RelMetadataQuery mq) {
        RelNode best = rel.getBest();
        if (best != null) {
            return mq.getAverageRowSize(best);
        }
        return Double.POSITIVE_INFINITY;
    }
    
    public Double averageRowSize(RelNode rel, RelMetadataQuery mq) {
        double rowSize = 0;
        for (RelNode input : rel.getInputs()) {
            rowSize += mq.getAverageRowSize(input);
        }
        
        return rowSize;
    }
}
