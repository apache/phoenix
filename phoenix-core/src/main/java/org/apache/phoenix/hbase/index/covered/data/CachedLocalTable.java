package org.apache.phoenix.hbase.index.covered.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

import java.util.HashMap;

public class CachedLocalTable implements LocalHBaseState {

    private final HashMap<ImmutableBytesPtr, List<Cell>> rowKeyPtrToCells;

    public CachedLocalTable(HashMap<ImmutableBytesPtr, List<Cell>> rowKeyPtrToCells)
    {
        this.rowKeyPtrToCells = rowKeyPtrToCells;
    }

    @Override
    public List<Cell> getCurrentRowState(
            Mutation mutation,
            Collection<? extends ColumnReference> columnReferences,
            boolean ignoreNewerMutations) throws IOException {
        byte[] rowKey = mutation.getRow();
        List<Cell> cells = this.rowKeyPtrToCells.get(new ImmutableBytesPtr(rowKey));

        if(cells == null || cells.isEmpty()) {
            return cells;
        }
        if(!ignoreNewerMutations) {
            return cells;
        }
        long timestamp =
                mutation.getFamilyCellMap().values().iterator().next().get(0).getTimestamp();
        List<Cell> newCells = new ArrayList<Cell>();
        for(Cell cell : cells) {
            if(cell.getTimestamp() < timestamp ) {
                newCells.add(cell);
            }
        }
        return newCells;
    }

}
