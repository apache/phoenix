package org.apache.phoenix.schema.tuple;

import static org.apache.phoenix.hbase.index.util.ImmutableBytesPtr.copyBytesIfNecessary;

import java.io.IOException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

public class ValueGetterTuple extends BaseTuple {
	private ValueGetter valueGetter;
    
    public ValueGetterTuple(ValueGetter valueGetter) {
        this.valueGetter = valueGetter;
    }
    
    public ValueGetterTuple() {
    }
    
    @Override
    public void getKey(ImmutableBytesWritable ptr) {
        ptr.set(valueGetter.getRowKey());
    }

    @Override
    public boolean isImmutable() {
        return true;
    }

    @Override
    public KeyValue getValue(byte[] family, byte[] qualifier) {
    	ImmutableBytesPtr value = null;
        try {
            value = valueGetter.getLatestValue(new ColumnReference(family, qualifier));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    	return new KeyValue(valueGetter.getRowKey(), family, qualifier, HConstants.LATEST_TIMESTAMP, Type.Put, value!=null? copyBytesIfNecessary(value) : null);
    }

    @Override
    public String toString() {
    	// TODO is this correct?
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
    	// TODO is this correct?
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValue getValue(int index) {
    	// TODO is this correct?
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getValue(byte[] family, byte[] qualifier,
            ImmutableBytesWritable ptr) {
        KeyValue kv = getValue(family, qualifier);
        if (kv == null)
            return false;
        ptr.set(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
        return true;
    }

}
