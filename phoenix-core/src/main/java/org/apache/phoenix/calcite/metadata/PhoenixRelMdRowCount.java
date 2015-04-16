package org.apache.phoenix.calcite.metadata;

import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.phoenix.calcite.rel.PhoenixAbstractSort;
import org.apache.phoenix.calcite.rel.PhoenixLimit;

public class PhoenixRelMdRowCount {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                BuiltInMethod.ROW_COUNT.method, new PhoenixRelMdRowCount());
    
    public Double getRowCount(PhoenixAbstractSort rel) {
        return rel.getRows();
      }
    
    public Double getRowCount(PhoenixLimit rel) {
        return rel.getRows();
      }
}
