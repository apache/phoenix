package org.apache.phoenix.cache;

import java.io.Closeable;
import java.util.List;

import org.apache.phoenix.index.IndexMaintainer;

public interface IndexMetaDataCache extends Closeable {
    public List<IndexMaintainer> getIndexMaintainers();
}
