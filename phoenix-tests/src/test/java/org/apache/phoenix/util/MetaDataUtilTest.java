/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.util;

import static org.apache.phoenix.coprocessorclient.MetaDataEndpointImplConstants.VIEW_MODIFIED_PROPERTY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_DDL_TIMESTAMP_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY_BYTES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.RawCellBuilderFactory;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Iterator;
import java.util.List;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;

public class MetaDataUtilTest {

    private static final byte[] ROW = Bytes.toBytes("row");
    private static final byte[] QUALIFIER = Bytes.toBytes("qual");
    private static final byte[] ORIGINAL_VALUE = Bytes.toBytes("generic-value");
    private static final byte[] DUMMY_TAGS = Bytes.toBytes("tags");
    private final ExtendedCellBuilder mockBuilder = Mockito.mock(ExtendedCellBuilder.class);
    private final ExtendedCell mockCellWithTags = Mockito.mock(ExtendedCell.class);

    @Before
    public void setupMockCellBuilder() {
        Mockito.when(mockBuilder.setRow(Mockito.any(byte[].class), Mockito.anyInt(),
                Mockito.anyInt())).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.setFamily(Mockito.any(byte[].class), Mockito.anyInt(),
                Mockito.anyInt())).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.setQualifier(Mockito.any(byte[].class), Mockito.anyInt(),
                Mockito.anyInt())).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.setValue(Mockito.any(byte[].class), Mockito.anyInt(),
                Mockito.anyInt())).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.setTimestamp(Mockito.anyLong())).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.setType(Mockito.any(Cell.Type.class)))
                .thenReturn(mockBuilder);
        Mockito.when(mockBuilder.setTags(Mockito.any(byte[].class)))
                .thenReturn(mockBuilder);
        Mockito.when(mockBuilder.build()).thenReturn(mockCellWithTags);
    }

    @Test
    public void testEncode() {
        assertEquals(VersionUtil.encodeVersion("0.94.5"),VersionUtil.encodeVersion("0.94.5-mapR"));
        assertTrue(VersionUtil.encodeVersion("0.94.6")>VersionUtil.encodeVersion("0.94.5-mapR"));
        assertTrue(VersionUtil.encodeVersion("0.94.6")>VersionUtil.encodeVersion("0.94.5"));
        assertTrue(VersionUtil.encodeVersion("0.94.1-mapR")>VersionUtil.encodeVersion("0.94"));
        assertTrue(VersionUtil.encodeVersion("1", "1", "3")>VersionUtil.encodeVersion("1", "1", "1"));
    }

    @Test
    public void testDecode() {
        int encodedVersion = VersionUtil.encodeVersion("4.15.5");
        assertEquals(VersionUtil.decodeMajorVersion(encodedVersion), 4);
        assertEquals(VersionUtil.decodeMinorVersion(encodedVersion), 15);
        assertEquals(VersionUtil.decodePatchVersion(encodedVersion), 5);
    }

    @Test
    public void testCompatibility() {
        assertTrue(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(1,2,1), 1, 2).getIsCompatible());
        assertTrue(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(1,2,10), 1, 1).getIsCompatible());
        assertTrue(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(1,2,0), 1, 2).getIsCompatible());
        assertTrue(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(1,2,255), 1, 2).getIsCompatible());
        assertTrue(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(2,2,0), 2, 0).getIsCompatible());
        assertTrue(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(2,10,36), 2, 9).getIsCompatible());
        assertFalse(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(3,1,10), 4, 0).getIsCompatible());
        assertFalse(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(3,1,10), 2, 0).getIsCompatible());
        assertFalse(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(3,1,10), 3, 2).getIsCompatible());
        assertFalse(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(3,1,10), 3, 5).getIsCompatible());
    }

    @Test
    public void testCompatibilityNewerClient() {
        MetaDataUtil.ClientServerCompatibility compatibility1 = MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(3,1,10), 4, 0);
        assertFalse(compatibility1.getIsCompatible());
        assertEquals(compatibility1.getErrorCode(), SQLExceptionCode.OUTDATED_JARS.getErrorCode());
        MetaDataUtil.ClientServerCompatibility compatibility2 = MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(3,1,10), 3, 2);
        assertFalse(compatibility2.getIsCompatible());
        assertEquals(compatibility2.getErrorCode(), SQLExceptionCode.OUTDATED_JARS.getErrorCode());
    }

    @Test
    public void testCompatibilityMismatchedMajorVersions() {
        MetaDataUtil.ClientServerCompatibility compatibility = MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(3,1,10), 2, 0);
        assertFalse(compatibility.getIsCompatible());
        assertEquals(compatibility.getErrorCode(), SQLExceptionCode.INCOMPATIBLE_CLIENT_SERVER_JAR.getErrorCode());
    }

    @Test
    public void testMutatingAPut() throws Exception {
        Put put = generateOriginalPut();
        byte[] newValue = Bytes.toBytes("new-value");
        Cell cell = put.get(TABLE_FAMILY_BYTES, QUALIFIER).get(0);
        assertEquals(Bytes.toString(ORIGINAL_VALUE),
                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        MetaDataUtil.mutatePutValue(put, TABLE_FAMILY_BYTES, QUALIFIER, newValue);
        cell = put.get(TABLE_FAMILY_BYTES, QUALIFIER).get(0);
        assertEquals(Bytes.toString(newValue),
                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
    }

    @Test
    public void testTaggingAPutWrongQualifier() throws Exception {
        Put put = generateOriginalPut();
        Cell initialCell = put.get(TABLE_FAMILY_BYTES, QUALIFIER).get(0);

        // Different qualifier, so no tags should be set
        MetaDataUtil.conditionallyAddTagsToPutCells(put, TABLE_FAMILY_BYTES, EMPTY_BYTE_ARRAY,
                mockBuilder, EMPTY_BYTE_ARRAY, DUMMY_TAGS);
        verify(mockBuilder, never()).setTags(Mockito.any(byte[].class));
        Cell newCell = put.getFamilyCellMap().get(TABLE_FAMILY_BYTES).get(0);
        assertEquals(initialCell, newCell);
        assertNull(TagUtil.carryForwardTags(newCell));
    }

    @Test
    public void testTaggingAPutUnconditionally() throws Exception {
        Put put = generateOriginalPut();

        // valueArray is null so we always set tags
        MetaDataUtil.conditionallyAddTagsToPutCells(put, TABLE_FAMILY_BYTES, QUALIFIER,
                mockBuilder, null, DUMMY_TAGS);
        verify(mockBuilder, times(1)).setTags(Mockito.any(byte[].class));
        Cell newCell = put.getFamilyCellMap().get(TABLE_FAMILY_BYTES).get(0);
        assertEquals(mockCellWithTags, newCell);
    }

    @Test
    public void testSkipTaggingAPutDueToSameCellValue() throws Exception {
        Put put = generateOriginalPut();
        Cell initialCell = put.get(TABLE_FAMILY_BYTES, QUALIFIER).get(0);

        // valueArray is set as the value stored in the cell, so we skip tagging the cell
        MetaDataUtil.conditionallyAddTagsToPutCells(put, TABLE_FAMILY_BYTES, QUALIFIER,
                mockBuilder, ORIGINAL_VALUE, DUMMY_TAGS);
        verify(mockBuilder, never()).setTags(Mockito.any(byte[].class));
        Cell newCell = put.getFamilyCellMap().get(TABLE_FAMILY_BYTES).get(0);
        assertEquals(initialCell, newCell);
        assertNull(TagUtil.carryForwardTags(newCell));
    }

    @Test
    public void testTaggingAPutDueToDifferentCellValue() throws Exception {
        Put put = generateOriginalPut();

        // valueArray is set to a value different than the one in the cell, so we tag the cell
        MetaDataUtil.conditionallyAddTagsToPutCells(put, TABLE_FAMILY_BYTES, QUALIFIER,
                mockBuilder, EMPTY_BYTE_ARRAY, DUMMY_TAGS);
        verify(mockBuilder, times(1)).setTags(Mockito.any(byte[].class));
        Cell newCell = put.getFamilyCellMap().get(TABLE_FAMILY_BYTES).get(0);
        assertEquals(mockCellWithTags, newCell);
    }

    /**
   * Ensure it supports {@link GenericKeyValueBuilder}
   * @throws Exception on failure
   */
  @Test
  public void testGetMutationKeyValue() throws Exception {
    String version = VersionInfo.getVersion();
    KeyValueBuilder builder = KeyValueBuilder.get(version);
    KeyValue kv = builder.buildPut(wrap(ROW), wrap(TABLE_FAMILY_BYTES), wrap(QUALIFIER),
            wrap(ORIGINAL_VALUE));
    Put put = new Put(ROW);
    KeyValueBuilder.addQuietly(put, kv);

    // read back out the value
    ImmutableBytesPtr ptr = new ImmutableBytesPtr();
    assertTrue(MetaDataUtil.getMutationValue(put, QUALIFIER, builder, ptr));
    assertEquals("Value returned doesn't match stored value for " + builder.getClass().getName()
        + "!", 0,
      ByteUtil.BYTES_PTR_COMPARATOR.compare(ptr, wrap(ORIGINAL_VALUE)));

    // try again, this time with the clientkeyvalue builder
    if (builder != GenericKeyValueBuilder.INSTANCE) {
        builder = GenericKeyValueBuilder.INSTANCE;
        byte[] value = Bytes.toBytes("client-value");
        kv = builder.buildPut(wrap(ROW), wrap(TABLE_FAMILY_BYTES), wrap(QUALIFIER), wrap(value));
        put = new Put(ROW);
        KeyValueBuilder.addQuietly(put, kv);
    
        // read back out the value
        assertTrue(MetaDataUtil.getMutationValue(put, QUALIFIER, builder, ptr));
        assertEquals("Value returned doesn't match stored value for " + builder.getClass().getName()
            + "!", 0,
          ByteUtil.BYTES_PTR_COMPARATOR.compare(ptr, wrap(value)));
    
        // ensure that we don't get matches for qualifiers that don't match
        assertFalse(MetaDataUtil.getMutationValue(put, Bytes.toBytes("not a match"), builder, ptr));
    }
  }

  private static ImmutableBytesPtr wrap(byte[] bytes) {
    return new ImmutableBytesPtr(bytes);
  }

    @Test
    public void testEncodeDecode() {
        String hbaseVersionStr = "0.98.14";
        Configuration config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config.setBoolean(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, false);
        config.setBoolean(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, false);

        long version = MetaDataUtil.encodeVersion(hbaseVersionStr, config);
        int hbaseVersion = MetaDataUtil.decodeHBaseVersion(version);
        int expectedHBaseVersion = VersionUtil.encodeVersion(0, 98, 14);
        assertEquals(expectedHBaseVersion, hbaseVersion);
        boolean isTableNamespaceMappingEnabled = MetaDataUtil.decodeTableNamespaceMappingEnabled(version);
        assertFalse(isTableNamespaceMappingEnabled);
        int phoenixVersion = MetaDataUtil.decodePhoenixVersion(version);
        int expectedPhoenixVersion = VersionUtil.encodeVersion(MetaDataProtocol.PHOENIX_MAJOR_VERSION,
                MetaDataProtocol.PHOENIX_MINOR_VERSION, MetaDataProtocol.PHOENIX_PATCH_NUMBER);
        assertEquals(expectedPhoenixVersion, phoenixVersion);

        config.setBoolean(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, true);

        version = MetaDataUtil.encodeVersion(hbaseVersionStr, config);
        hbaseVersion = MetaDataUtil.decodeHBaseVersion(version);
        expectedHBaseVersion = VersionUtil.encodeVersion(0, 98, 14);
        assertEquals(expectedHBaseVersion, hbaseVersion);
        isTableNamespaceMappingEnabled = MetaDataUtil.decodeTableNamespaceMappingEnabled(version);
        assertTrue(isTableNamespaceMappingEnabled);
        phoenixVersion = MetaDataUtil.decodePhoenixVersion(version);
        expectedPhoenixVersion = VersionUtil.encodeVersion(MetaDataProtocol.PHOENIX_MAJOR_VERSION,
                MetaDataProtocol.PHOENIX_MINOR_VERSION, MetaDataProtocol.PHOENIX_PATCH_NUMBER);
        assertEquals(expectedPhoenixVersion, phoenixVersion);
    }

    private Put generateOriginalPut() {
        String version = VersionInfo.getVersion();
        KeyValueBuilder builder = KeyValueBuilder.get(version);
        KeyValue kv = builder.buildPut(wrap(ROW), wrap(TABLE_FAMILY_BYTES), wrap(QUALIFIER),
                wrap(ORIGINAL_VALUE));
        Put put = new Put(ROW);
        KeyValueBuilder.addQuietly(put, kv);
        return put;
    }

    @Test
    public void testConditionallyAddTagsToPutCells( ) {
        List<Tag> tags = TagUtil.asList(VIEW_MODIFIED_PROPERTY_BYTES, 0, VIEW_MODIFIED_PROPERTY_BYTES.length);
        assertEquals(tags.size(), 1);
        Tag expectedTag = tags.get(0);

        String version = VersionInfo.getVersion();
        KeyValueBuilder builder = KeyValueBuilder.get(version);
        KeyValue kv = builder.buildPut(wrap(ROW), wrap(TABLE_FAMILY_BYTES), wrap(UPDATE_CACHE_FREQUENCY_BYTES), wrap(
                PLong.INSTANCE.toBytes(0)));
        Put put = new Put(ROW);
        KeyValueBuilder.addQuietly(put, kv);

        ExtendedCellBuilder cellBuilder = (ExtendedCellBuilder) RawCellBuilderFactory.create();
        MetaDataUtil.conditionallyAddTagsToPutCells(put, TABLE_FAMILY_BYTES, UPDATE_CACHE_FREQUENCY_BYTES, cellBuilder,
                PInteger.INSTANCE.toBytes(1), VIEW_MODIFIED_PROPERTY_BYTES);

        Cell cell = put.getFamilyCellMap().get(TABLE_FAMILY_BYTES).get(0);

        // To check the cell tag whether view has modified this property
        assertTrue(Bytes.compareTo(expectedTag.getValueArray(), TagUtil.concatTags(EMPTY_BYTE_ARRAY, cell)) == 0);
        assertTrue(Bytes.contains(TagUtil.concatTags(EMPTY_BYTE_ARRAY, cell), expectedTag.getValueArray()));

        // To check tag data can be correctly deserialized
        Iterator<Tag> tagIterator = PrivateCellUtil.tagsIterator(cell);
        assertTrue(tagIterator.hasNext());
        Tag actualTag = tagIterator.next();
        assertTrue(Bytes.compareTo(actualTag.getValueArray(), actualTag.getValueOffset(), actualTag.getValueLength(),
                expectedTag.getValueArray(), expectedTag.getValueOffset(), expectedTag.getValueLength()) == 0);
        assertFalse(tagIterator.hasNext());
    }

    @Test
    public void testGetLastDDLTimestampUpdate() throws Exception {
      byte[] tableHeaderRowKey = SchemaUtil.getTableKey("TenantId", "schema", "table");
      long serverTimestamp = EnvironmentEdgeManager.currentTimeMillis();
        long clientTimestamp = serverTimestamp - 1000L;
      Put p = MetaDataUtil.getLastDDLTimestampUpdate(tableHeaderRowKey, clientTimestamp,
          serverTimestamp);
      assertNotNull(p);
      assertFalse("Mutation is empty!", p.isEmpty());
      assertArrayEquals(tableHeaderRowKey, p.getRow());
      assertEquals(clientTimestamp, p.getTimestamp());
      assertTrue(p.cellScanner().advance());
      List<Cell> cells = p.get(TABLE_FAMILY_BYTES, LAST_DDL_TIMESTAMP_BYTES);
      assertNotNull(cells);
      assertTrue(cells.size() > 0);
      Cell c = cells.get(0);
      assertNotNull("Cell is null!", c);
      assertEquals(serverTimestamp, PLong.INSTANCE.getCodec().decodeLong(CellUtil.cloneValue(c),
          0, SortOrder.ASC));
    }
}

