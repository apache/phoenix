package org.apache.phoenix.calcite.metadata;

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.phoenix.calcite.rel.Limit;

public class PhoenixRelMdMaxRowCount implements MetadataHandler<BuiltInMetadata.MaxRowCount> {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                BuiltInMethod.MAX_ROW_COUNT.method, new PhoenixRelMdMaxRowCount());
    
    private PhoenixRelMdMaxRowCount() { }
    
    @Override
    public MetadataDef<BuiltInMetadata.MaxRowCount> getDef() {
        return BuiltInMetadata.MaxRowCount.DEF;
    }
    
    public Double getMaxRowCount(HepRelVertex rel, RelMetadataQuery mq) {
        return mq.getMaxRowCount(rel.getCurrentRel());
    }
    
    public Double getMaxRowCount(Limit rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }
}
