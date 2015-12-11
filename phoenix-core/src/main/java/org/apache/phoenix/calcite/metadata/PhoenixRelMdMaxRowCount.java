package org.apache.phoenix.calcite.metadata;

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.BuiltInMethod;

public class PhoenixRelMdMaxRowCount {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                BuiltInMethod.MAX_ROW_COUNT.method, new PhoenixRelMdMaxRowCount());

    private PhoenixRelMdMaxRowCount() {
    }

    public Double getMaxRowCount(RelSubset rel) {
        for (RelNode node : rel.getRels()) {
            if (node instanceof Sort) {
                Sort sort = (Sort) node;
                if (sort.fetch != null) {
                    return (double) RexLiteral.intValue(sort.fetch);
                }
            }
        }
        
        return Double.POSITIVE_INFINITY;
    }
}
