package org.apache.phoenix.calcite.metadata;

import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import com.google.common.collect.ImmutableList;

public class PhoenixRelMetadataProvider extends ChainedRelMetadataProvider {

    public PhoenixRelMetadataProvider() {
        super(ImmutableList.of(
                PhoenixRelMdRowCount.SOURCE, 
                PhoenixRelMdCollation.SOURCE,
                PhoenixRelMdSize.SOURCE,
                DefaultRelMetadataProvider.INSTANCE));
    }

}
