/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import static org.apache.phoenix.schema.types.PDataType.FALSE_BYTES;
import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixSyncTableRegionScanner.Chunk;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.util.SHA256DigestUtil;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.junit.Test;

/**
 * Unit tests for PhoenixSyncTableRegionScanner.Chunk class. Tests the core chunk logic without
 * requiring HBase minicluster or mocking.
 */
public class PhoenixSyncTableRegionScannerTest {

  private static final byte[] TEST_FAMILY = Bytes.toBytes("0");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("Q");
  private static final byte[] TEST_VALUE = Bytes.toBytes("value");
  private static final long TEST_TIMESTAMP = 1000L;

  @Test
  public void testConstructorWithNullDigest() {
    Chunk chunk = new Chunk(null);

    assertNotNull(chunk);
    assertTrue(chunk.isEmpty());
    assertEquals(0, chunk.getRowCount());
    assertEquals(0, chunk.getSize());
    assertNull(chunk.getStartKey());
    assertNull(chunk.getEndKey());
  }

  @Test
  public void testConstructorWithContinuedDigest() throws IOException {
    // Create a digest with some state
    SHA256Digest originalDigest = new SHA256Digest();
    originalDigest.update(Bytes.toBytes("test"), 0, 4);
    // Create chunk with continued digest
    Chunk chunk = new Chunk(originalDigest);
    assertNotNull(chunk);
    assertTrue(chunk.isEmpty());
  }

  @Test
  public void testAddFirstRowSetsStartKeyAndEndKey() {
    Chunk chunk = new Chunk(null);
    byte[] rowKey = Bytes.toBytes("row1");
    List<Cell> cells =
      createTestRow(rowKey, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE);
    chunk.addRow(cells);
    assertArrayEquals(rowKey, chunk.getStartKey());
    assertArrayEquals(rowKey, chunk.getEndKey());
  }

  @Test
  public void testAddMultipleRowsUpdatesEndKeyOnly() {
    Chunk chunk = new Chunk(null);
    byte[] rowKey1 = Bytes.toBytes("row1");
    byte[] rowKey2 = Bytes.toBytes("row2");
    byte[] rowKey3 = Bytes.toBytes("row3");

    chunk.addRow(createTestRow(rowKey1, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE));
    chunk.addRow(createTestRow(rowKey2, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE));
    chunk.addRow(createTestRow(rowKey3, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE));

    assertArrayEquals(rowKey1, chunk.getStartKey());
    assertArrayEquals(rowKey3, chunk.getEndKey());
  }

  @Test
  public void testAddRowAccumulatesSize() {
    Chunk chunk = new Chunk(null);
    byte[] rowKey = Bytes.toBytes("row1");
    List<Cell> cells =
      createTestRow(rowKey, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE);

    long initialSize = chunk.getSize();
    chunk.addRow(cells);
    long sizeAfterFirstRow = chunk.getSize();

    assertTrue(sizeAfterFirstRow > initialSize);

    chunk.addRow(createTestRow(Bytes.toBytes("row2"), TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP,
      TEST_VALUE));
    long sizeAfterSecondRow = chunk.getSize();

    assertTrue(sizeAfterSecondRow > sizeAfterFirstRow);
  }

  @Test
  public void testCalculateRowSizeMultipleCells() {
    Chunk chunk = new Chunk(null);
    byte[] rowKey = Bytes.toBytes("row1");
    List<Cell> cells = createTestRowWithMultipleCells(rowKey, 3);

    chunk.addRow(cells);
    long sizeWithMultipleCells = chunk.getSize();

    Chunk chunkSingleCell = new Chunk(null);
    List<Cell> singleCell =
      createTestRow(rowKey, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE);
    chunkSingleCell.addRow(singleCell);
    long sizeWithSingleCell = chunkSingleCell.getSize();

    assertTrue(sizeWithMultipleCells > sizeWithSingleCell);
  }

  @Test
  public void testCalculateRowSizeLargeValues() {
    Chunk chunk = new Chunk(null);
    byte[] rowKey = Bytes.toBytes("row1");
    byte[] largeValue = new byte[1000];
    Arrays.fill(largeValue, (byte) 'X');
    List<Cell> cells =
      createTestRow(rowKey, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, largeValue);

    chunk.addRow(cells);

    assertTrue(chunk.getSize() > 1000);
  }

  @Test
  public void testCalculateRowSizeEmptyCells() {
    Chunk chunk = new Chunk(null);
    byte[] rowKey = Bytes.toBytes("row1");
    List<Cell> cells = new ArrayList<>();
    // Create a cell with the rowKey to avoid NPE when extracting row key
    cells.add(new KeyValue(rowKey, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, new byte[0]));

    chunk.addRow(cells);

    assertTrue(chunk.getSize() > 0);
    assertEquals(1, chunk.getRowCount());
  }

  @Test
  public void testSameDataProducesSameHash() throws IOException {
    byte[] rowKey = Bytes.toBytes("row1");
    List<Cell> cells =
      createTestRow(rowKey, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE);

    Chunk chunk1 = new Chunk(null);
    chunk1.addRow(cells);
    byte[] hash1 = chunk1.finalizeHash();

    Chunk chunk2 = new Chunk(null);
    chunk2.addRow(cells);
    byte[] hash2 = chunk2.finalizeHash();

    assertArrayEquals(hash1, hash2);
  }

  @Test
  public void testDifferentDataProducesDifferentHash() throws IOException {
    byte[] rowKey1 = Bytes.toBytes("row1");
    byte[] rowKey2 = Bytes.toBytes("row2");

    Chunk chunk1 = new Chunk(null);
    chunk1.addRow(createTestRow(rowKey1, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE));
    byte[] hash1 = chunk1.finalizeHash();

    Chunk chunk2 = new Chunk(null);
    chunk2.addRow(createTestRow(rowKey2, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE));
    byte[] hash2 = chunk2.finalizeHash();

    assertFalse(Arrays.equals(hash1, hash2));
  }

  @Test
  public void testHashIncludesFamily() throws IOException {
    byte[] rowKey = Bytes.toBytes("row1");
    byte[] family1 = Bytes.toBytes("f1");
    byte[] family2 = Bytes.toBytes("f2");

    Chunk chunk1 = new Chunk(null);
    chunk1.addRow(createTestRow(rowKey, family1, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE));
    byte[] hash1 = chunk1.finalizeHash();

    Chunk chunk2 = new Chunk(null);
    chunk2.addRow(createTestRow(rowKey, family2, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE));
    byte[] hash2 = chunk2.finalizeHash();

    assertFalse(Arrays.equals(hash1, hash2));
  }

  @Test
  public void testHashIncludesQualifier() throws IOException {
    byte[] rowKey = Bytes.toBytes("row1");
    byte[] qualifier1 = Bytes.toBytes("q1");
    byte[] qualifier2 = Bytes.toBytes("q2");

    Chunk chunk1 = new Chunk(null);
    chunk1.addRow(createTestRow(rowKey, TEST_FAMILY, qualifier1, TEST_TIMESTAMP, TEST_VALUE));
    byte[] hash1 = chunk1.finalizeHash();

    Chunk chunk2 = new Chunk(null);
    chunk2.addRow(createTestRow(rowKey, TEST_FAMILY, qualifier2, TEST_TIMESTAMP, TEST_VALUE));
    byte[] hash2 = chunk2.finalizeHash();

    assertFalse(Arrays.equals(hash1, hash2));
  }

  @Test
  public void testHashIncludesTimestamp() throws IOException {
    byte[] rowKey = Bytes.toBytes("row1");
    long timestamp1 = 1000L;
    long timestamp2 = 2000L;

    Chunk chunk1 = new Chunk(null);
    chunk1.addRow(createTestRow(rowKey, TEST_FAMILY, TEST_QUALIFIER, timestamp1, TEST_VALUE));
    byte[] hash1 = chunk1.finalizeHash();

    Chunk chunk2 = new Chunk(null);
    chunk2.addRow(createTestRow(rowKey, TEST_FAMILY, TEST_QUALIFIER, timestamp2, TEST_VALUE));
    byte[] hash2 = chunk2.finalizeHash();

    assertFalse(Arrays.equals(hash1, hash2));
  }

  @Test
  public void testHashIncludesCellType() throws IOException {
    byte[] rowKey = Bytes.toBytes("row1");

    Chunk chunk1 = new Chunk(null);
    List<Cell> putCells = createTestRowWithType(rowKey, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP,
      TEST_VALUE, Type.Put);
    chunk1.addRow(putCells);
    byte[] hash1 = chunk1.finalizeHash();

    Chunk chunk2 = new Chunk(null);
    List<Cell> deleteCells = createTestRowWithType(rowKey, TEST_FAMILY, TEST_QUALIFIER,
      TEST_TIMESTAMP, TEST_VALUE, Type.Delete);
    chunk2.addRow(deleteCells);
    byte[] hash2 = chunk2.finalizeHash();

    assertFalse(Arrays.equals(hash1, hash2));
  }

  @Test
  public void testHashIncludesValue() throws IOException {
    byte[] rowKey = Bytes.toBytes("row1");
    byte[] value1 = Bytes.toBytes("value1");
    byte[] value2 = Bytes.toBytes("value2");

    Chunk chunk1 = new Chunk(null);
    chunk1.addRow(createTestRow(rowKey, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, value1));
    byte[] hash1 = chunk1.finalizeHash();

    Chunk chunk2 = new Chunk(null);
    chunk2.addRow(createTestRow(rowKey, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, value2));
    byte[] hash2 = chunk2.finalizeHash();

    assertFalse(Arrays.equals(hash1, hash2));
  }

  @Test
  public void testContinuedDigestProducesSameHashAsCombined() throws IOException {
    byte[] rowKey1 = Bytes.toBytes("row1");
    byte[] rowKey2 = Bytes.toBytes("row2");
    List<Cell> cells1 =
      createTestRow(rowKey1, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE);
    List<Cell> cells2 =
      createTestRow(rowKey2, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE);

    // Create combined chunk with both rows
    Chunk combined = new Chunk(null);
    combined.addRow(cells1);
    combined.addRow(cells2);
    byte[] combinedHash = combined.finalizeHash();

    // Create first chunk with first row, get digest state
    Chunk chunk1 = new Chunk(null);
    chunk1.addRow(cells1);
    byte[] digestState = chunk1.getDigestState();

    // Create second chunk continuing from first
    SHA256Digest continuedDigest = SHA256DigestUtil.decodeDigestState(digestState);
    Chunk chunk2 = new Chunk(continuedDigest);
    chunk2.addRow(cells2);
    byte[] continuedHash = chunk2.finalizeHash();

    assertArrayEquals(combinedHash, continuedHash);
  }

  @Test
  public void testIsEmptyFalseAfterAddingRow() {
    Chunk chunk = new Chunk(null);
    byte[] rowKey = Bytes.toBytes("row1");
    chunk.addRow(createTestRow(rowKey, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE));

    assertFalse(chunk.isEmpty());
  }

  @Test
  public void testExceedsSizeTrueWhenExceeded() {
    Chunk chunk = new Chunk(null);
    byte[] rowKey = Bytes.toBytes("row1");
    byte[] largeValue = new byte[1000];
    chunk.addRow(createTestRow(rowKey, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, largeValue));

    assertTrue(chunk.exceedsSize(100));
  }

  @Test
  public void testExceedsSizeFalseWhenWithinLimit() {
    Chunk chunk = new Chunk(null);
    byte[] rowKey = Bytes.toBytes("row1");
    chunk.addRow(createTestRow(rowKey, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE));

    assertFalse(chunk.exceedsSize(10000));
  }

  @Test
  public void testExceedsSizeBoundaryCondition() {
    Chunk chunk = new Chunk(null);
    byte[] rowKey = Bytes.toBytes("row1");
    chunk.addRow(createTestRow(rowKey, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE));

    long exactSize = chunk.getSize();
    assertFalse(chunk.exceedsSize(exactSize));
    assertTrue(chunk.exceedsSize(exactSize - 1));
  }

  @Test
  public void testGetSizeReturnsCorrectValue() {
    Chunk chunk = new Chunk(null);
    assertEquals(0, chunk.getSize());

    byte[] rowKey = Bytes.toBytes("row1");
    chunk.addRow(createTestRow(rowKey, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE));

    assertTrue(chunk.getSize() > 0);
  }

  @Test
  public void testGetRowCountReturnsCorrectValue() {
    Chunk chunk = new Chunk(null);
    assertEquals(0, chunk.getRowCount());

    chunk.addRow(createTestRow(Bytes.toBytes("row1"), TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP,
      TEST_VALUE));
    assertEquals(1, chunk.getRowCount());

    chunk.addRow(createTestRow(Bytes.toBytes("row2"), TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP,
      TEST_VALUE));
    assertEquals(2, chunk.getRowCount());
  }

  @Test
  public void testBuildChunkMetadataResultCompleteChunkRowKeyIsEndKey() {
    byte[] startRow = Bytes.toBytes("row1");
    byte[] endRow = Bytes.toBytes("row5");
    Chunk chunk = createChunkWithRows(startRow, endRow);
    List<Cell> results = new ArrayList<>();

    PhoenixSyncTableRegionScanner.buildChunkMetadataResult(results, chunk, false);

    for (Cell cell : results) {
      assertArrayEquals(endRow, CellUtil.cloneRow(cell));
    }
  }

  @Test
  public void testBuildChunkMetadataResultCompleteChunkStartKeyCell() {
    byte[] startRow = Bytes.toBytes("row1");
    byte[] endRow = Bytes.toBytes("row5");
    Chunk chunk = createChunkWithRows(startRow, endRow);
    List<Cell> results = new ArrayList<>();

    PhoenixSyncTableRegionScanner.buildChunkMetadataResult(results, chunk, false);

    Cell startKeyCell =
      findCell(results, BaseScannerRegionObserverConstants.SYNC_TABLE_START_KEY_QUALIFIER);
    assertNotNull(startKeyCell);
    assertArrayEquals(startRow, CellUtil.cloneValue(startKeyCell));
  }

  @Test
  public void testBuildChunkMetadataResultCompleteChunkRowCountCell() {
    Chunk chunk =
      createChunkWithRows(Bytes.toBytes("row1"), Bytes.toBytes("row2"), Bytes.toBytes("row3"));
    List<Cell> results = new ArrayList<>();

    PhoenixSyncTableRegionScanner.buildChunkMetadataResult(results, chunk, false);

    Cell rowCountCell =
      findCell(results, BaseScannerRegionObserverConstants.SYNC_TABLE_ROW_COUNT_QUALIFIER);
    assertNotNull(rowCountCell);
    assertEquals(3, Bytes.toLong(CellUtil.cloneValue(rowCountCell)));
  }

  @Test
  public void testBuildChunkMetadataResultCompleteChunkHashIsFinalizedHash() {
    byte[] startRow = Bytes.toBytes("row1");
    byte[] endRow = Bytes.toBytes("row5");
    Chunk referenceChunk = createChunkWithRows(startRow, endRow);
    byte[] expectedHash = referenceChunk.finalizeHash();

    Chunk chunk = createChunkWithRows(startRow, endRow);
    List<Cell> results = new ArrayList<>();

    PhoenixSyncTableRegionScanner.buildChunkMetadataResult(results, chunk, false);

    Cell hashCell = findCell(results, BaseScannerRegionObserverConstants.SYNC_TABLE_HASH_QUALIFIER);
    assertNotNull(hashCell);
    assertArrayEquals(expectedHash, CellUtil.cloneValue(hashCell));
  }

  @Test
  public void testBuildChunkMetadataResultCompleteChunkIsPartialFalse() {
    Chunk chunk = createChunkWithRows(Bytes.toBytes("row1"));
    List<Cell> results = new ArrayList<>();

    PhoenixSyncTableRegionScanner.buildChunkMetadataResult(results, chunk, false);

    Cell isPartialCell =
      findCell(results, BaseScannerRegionObserverConstants.SYNC_TABLE_IS_PARTIAL_CHUNK_QUALIFIER);
    assertNotNull(isPartialCell);
    assertArrayEquals(FALSE_BYTES, CellUtil.cloneValue(isPartialCell));
  }

  @Test
  public void testBuildChunkMetadataResultPartialChunkIsPartialTrue() {
    Chunk chunk = createChunkWithRows(Bytes.toBytes("row1"));
    List<Cell> results = new ArrayList<>();

    PhoenixSyncTableRegionScanner.buildChunkMetadataResult(results, chunk, true);

    Cell isPartialCell =
      findCell(results, BaseScannerRegionObserverConstants.SYNC_TABLE_IS_PARTIAL_CHUNK_QUALIFIER);
    assertNotNull(isPartialCell);
    assertArrayEquals(TRUE_BYTES, CellUtil.cloneValue(isPartialCell));
  }

  @Test
  public void testBuildChunkMetadataResultPartialChunkHashIsDigestState() {
    Chunk chunk = createChunkWithRows(Bytes.toBytes("row1"), Bytes.toBytes("row2"));
    byte[] expectedDigestState = chunk.getDigestState();
    List<Cell> results = new ArrayList<>();

    PhoenixSyncTableRegionScanner.buildChunkMetadataResult(results, chunk, true);

    Cell hashCell = findCell(results, BaseScannerRegionObserverConstants.SYNC_TABLE_HASH_QUALIFIER);
    assertNotNull(hashCell);
    assertArrayEquals(expectedDigestState, CellUtil.cloneValue(hashCell));
  }

  /**
   * Creates a test row with a single cell.
   */
  private List<Cell> createTestRow(byte[] rowKey, byte[] family, byte[] qualifier, long timestamp,
    byte[] value) {
    List<Cell> cells = new ArrayList<>();
    Cell cell = new KeyValue(rowKey, family, qualifier, timestamp, value);
    cells.add(cell);
    return cells;
  }

  /**
   * Creates a test row with a single cell of specified type.
   */
  private List<Cell> createTestRowWithType(byte[] rowKey, byte[] family, byte[] qualifier,
    long timestamp, byte[] value, Type type) {
    List<Cell> cells = new ArrayList<>();
    Cell cell = new KeyValue(rowKey, family, qualifier, timestamp, type, value);
    cells.add(cell);
    return cells;
  }

  /**
   * Creates a test row with multiple cells.
   */
  private List<Cell> createTestRowWithMultipleCells(byte[] rowKey, int numCells) {
    List<Cell> cells = new ArrayList<>();
    for (int i = 0; i < numCells; i++) {
      byte[] qualifier = Bytes.toBytes("Q" + i);
      byte[] value = Bytes.toBytes("value" + i);
      Cell cell = new KeyValue(rowKey, TEST_FAMILY, qualifier, TEST_TIMESTAMP, value);
      cells.add(cell);
    }
    return cells;
  }

  private Chunk createChunkWithRows(byte[]... rowKeys) {
    Chunk chunk = new Chunk(null);
    for (byte[] rowKey : rowKeys) {
      chunk.addRow(createTestRow(rowKey, TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_VALUE));
    }
    return chunk;
  }

  private Cell findCell(List<Cell> cells, byte[] qualifier) {
    for (Cell cell : cells) {
      if (Bytes.equals(CellUtil.cloneQualifier(cell), qualifier)) {
        return cell;
      }
    }
    return null;
  }

}
