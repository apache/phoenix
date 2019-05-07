package org.apache.phoenix.util;

import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.junit.Test;

public class PhoenixKeyValueUtilTest {

  private final static byte[] A = Bytes.toBytes("a");

  @Test
  public void testTupleSerDe() throws IOException {
    Tuple t = new SingleKeyValueTuple(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
        .setRow(A).setFamily(SINGLE_COLUMN_FAMILY).setQualifier(SINGLE_COLUMN)
        .setType(Type.Put).setValue(Bytes.toBytes(1)).build());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    PhoenixKeyValueUtil.serializeResult(out, t);
    byte[] data = baos.toByteArray();

    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    DataInputStream in = new DataInputStream(bais);
    Result r = PhoenixKeyValueUtil.deserializeResult(in);

    CellScanner cs = r.cellScanner();
    assertTrue(cs.advance());
    CellUtil.equals(t.getValue(0), cs.current());
    assertFalse(cs.advance());
  }
}
