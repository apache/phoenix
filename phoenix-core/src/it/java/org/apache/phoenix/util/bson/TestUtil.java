/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.util.bson;

import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBinaryReader;
import org.bson.BsonBoolean;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.io.ByteBufferBsonInput;
import org.bson.types.Decimal128;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test Utility class for BSON.
 */
public class TestUtil {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static byte[] getRawBsonBytes(final TestFieldsMap map) {
    return Bytes.toBytes(getRawBsonDocument(map).getByteBuffer().asNIO());
  }

  private static BsonDocument fromRawToBsonDocument(RawBsonDocument rawDocument) {
    try (BsonBinaryReader bsonReader = new BsonBinaryReader(
        new ByteBufferBsonInput(rawDocument.getByteBuffer()))) {
      return new BsonDocumentCodec().decode(bsonReader, DecoderContext.builder().build());
    }
  }

  public static RawBsonDocument getRawBsonDocument(final TestFieldsMap map) {
    BsonDocument bsonDocument = new BsonDocument();
    for (Map.Entry<String, TestFieldValue> entry : map.getMap().entrySet()) {
      updateBsonDocEntries(entry, bsonDocument);
    }
    return new RawBsonDocumentCodec().decode(new BsonDocumentReader(bsonDocument),
            DecoderContext.builder().build());
  }

  public static BsonDocument getBsonDocument(final TestFieldsMap map) {
    return fromRawToBsonDocument(getRawBsonDocument(map));
  }

  public static TestFieldsMap getPhoenixFieldMap(final BsonDocument bsonDocument) {
    Map<String, TestFieldValue> map = new HashMap<>();
    for (Map.Entry<String, BsonValue> entry : bsonDocument.entrySet()) {
      updateMapEntries(entry, map);
    }
    TestFieldsMap testFieldsMap = new TestFieldsMap();
    testFieldsMap.setMap(map);
    return testFieldsMap;
  }

  public static TestFieldsMap getPhoenixFieldMap(byte[] bytes) {
    return getPhoenixFieldMap(bytes, 0, bytes.length);
  }

  public static TestFieldsMap getPhoenixFieldMap(byte[] bytes, int offset, int length) {
    RawBsonDocument rawBsonDocument = new RawBsonDocument(bytes, offset, length);
    return getPhoenixFieldMap(rawBsonDocument);
  }

  private static void updateBsonDocEntries(Map.Entry<String, TestFieldValue> entry,
      BsonDocument bsonDocument) {
    TestFieldValue testFieldValue = entry.getValue();
    bsonDocument.append(entry.getKey(), getBsonValue(testFieldValue));
  }

  private static void updateMapEntries(Map.Entry<String, BsonValue> entry,
      Map<String, TestFieldValue> map) {
    BsonValue bsonValue = entry.getValue();
    map.put(entry.getKey(), getPhoenixFieldValue(bsonValue));
  }

  private static TestFieldValue getPhoenixFieldValue(BsonValue bsonValue) {
    if (bsonValue.isString()) {
      return new TestFieldValue().withS(((BsonString) bsonValue).getValue());
    } else if (bsonValue.isNumber() || bsonValue.isDecimal128()) {
      return getNumber((BsonNumber) bsonValue);
    } else if (bsonValue.isBinary()) {
      BsonBinary bsonBinary = (BsonBinary) bsonValue;
      return new TestFieldValue().withB(new SerializableBytesPtr(bsonBinary.getData()));
    } else if (bsonValue.isBoolean()) {
      return new TestFieldValue().withBOOL(((BsonBoolean) bsonValue).getValue());
    } else if (bsonValue.isNull()) {
      return new TestFieldValue().withNULL(true);
    } else if (bsonValue.isDocument()) {
      BsonDocument bsonDocument = (BsonDocument) bsonValue;
      if (bsonDocument.size() == 1 && bsonDocument.containsKey("$set")) {
        BsonValue value = bsonDocument.get("$set");
        if (!value.isArray()) {
          throw new IllegalArgumentException("$set is reserved for Set datatype");
        }
        BsonArray bsonArray = (BsonArray) value;
        if (bsonArray.isEmpty()) {
          throw new IllegalArgumentException("Set cannot be empty");
        }
        BsonValue firstElement = bsonArray.get(0);
        if (firstElement.isString()) {
          TestFieldValue testFieldValue = new TestFieldValue().withSS();
          bsonArray.getValues()
              .forEach(val -> testFieldValue.withSS(((BsonString) val).getValue()));
          return testFieldValue;
        } else if (firstElement.isNumber() || firstElement.isDecimal128()) {
          TestFieldValue testFieldValue = new TestFieldValue().withNS();
          bsonArray.getValues().forEach(
              val -> testFieldValue.withNS(getNumberFromBsonNumber((BsonNumber) val)));
          return testFieldValue;
        } else if (firstElement.isBinary()) {
          TestFieldValue testFieldValue = new TestFieldValue().withBS();
          bsonArray.getValues().forEach(val -> testFieldValue.withBS(
              new SerializableBytesPtr(((BsonBinary) val).getData())));
          return testFieldValue;
        }
        throw new IllegalArgumentException("Invalid set type");
      } else {
        Map<String, TestFieldValue> map = new HashMap<>();
        for (Map.Entry<String, BsonValue> entry : bsonDocument.entrySet()) {
          updateMapEntries(entry, map);
        }
        return new TestFieldValue().withM(map);
      }
    } else if (bsonValue.isArray()) {
      BsonArray bsonArray = (BsonArray) bsonValue;
      List<TestFieldValue> phoenixFieldList = new ArrayList<>();
      for (BsonValue bsonArrayValue : bsonArray.getValues()) {
        phoenixFieldList.add(getPhoenixFieldValue(bsonArrayValue));
      }
      return new TestFieldValue().withL(phoenixFieldList);
    }
    throw new RuntimeException("Invalid data type of BsonValue");
  }

  private static BsonValue getBsonValue(TestFieldValue testFieldValue) {
    if (testFieldValue.getVarchar() != null) {
      return new BsonString(testFieldValue.getVarchar());
    } else if (testFieldValue.getNumber() != null) {
      return getBsonNumber(testFieldValue);
    } else if (testFieldValue.getBinary() != null) {
      return new BsonBinary(testFieldValue.getBinary().getB());
    } else if (testFieldValue.getBoolean() != null) {
      return new BsonBoolean(testFieldValue.getBoolean());
    } else if (testFieldValue.getNull() != null) {
      return new BsonNull();
    } else if (testFieldValue.getMap() != null) {
      BsonDocument bsonDocument = new BsonDocument();
      for (Map.Entry<String, TestFieldValue> entry : testFieldValue.getMap()
          .entrySet()) {
        updateBsonDocEntries(entry, bsonDocument);
      }
      return bsonDocument;
    } else if (testFieldValue.getList() != null) {
      BsonArray bsonArray = new BsonArray();
      for(TestFieldValue listValue : testFieldValue.getList()) {
        bsonArray.add(getBsonValue(listValue));
      }
      return bsonArray;
    } else if (testFieldValue.getVarcharset() != null) {
      BsonDocument bsonDocument = new BsonDocument();
      List<BsonString> list = new ArrayList<>();
      testFieldValue.getVarcharset().forEach(val -> list.add(new BsonString(val)));
      bsonDocument.put("$set", new BsonArray(list));
      return bsonDocument;
    } else if (testFieldValue.getNumberset() != null) {
      BsonDocument bsonDocument = new BsonDocument();
      List<BsonNumber> list = new ArrayList<>();
      testFieldValue.getNumberset().forEach(val -> list.add(getBsonNumberFromNumber(val)));
      bsonDocument.put("$set", new BsonArray(list));
      return bsonDocument;
    } else if (testFieldValue.getBinaryset() != null) {
      BsonDocument bsonDocument = new BsonDocument();
      List<BsonBinary> list = new ArrayList<>();
      testFieldValue.getBinaryset().forEach(val -> list.add(new BsonBinary(val.getB())));
      bsonDocument.put("$set", new BsonArray(list));
      return bsonDocument;
    }
    throw new RuntimeException("Invalid data type of PhoenixFieldValue");
  }

  private static BsonNumber getBsonNumberFromNumber(Number number) {
    BsonNumber bsonNumber;
    if (number instanceof Integer || number instanceof Short || number instanceof Byte) {
      bsonNumber = new BsonInt32(number.intValue());
    } else if (number instanceof Long) {
      bsonNumber = new BsonInt64(number.longValue());
    } else if (number instanceof Double || number instanceof Float) {
      bsonNumber = new BsonDouble(number.doubleValue());
    } else if (number instanceof BigDecimal) {
      bsonNumber = new BsonDecimal128(new Decimal128((BigDecimal) number));
    } else {
      throw new IllegalArgumentException("Unsupported Number type: " + number.getClass());
    }
    return bsonNumber;
  }

  private static BsonNumber getBsonNumber(TestFieldValue testFieldValue) {
    Number number = testFieldValue.getNumber();
    return getBsonNumberFromNumber(number);
  }

  private static Number getNumberFromBsonNumber(BsonNumber bsonNumber) {
    if (bsonNumber instanceof BsonInt32) {
      return ((BsonInt32) bsonNumber).getValue();
    } else if (bsonNumber instanceof BsonInt64) {
      return ((BsonInt64) bsonNumber).getValue();
    } else if (bsonNumber instanceof BsonDouble) {
      return ((BsonDouble) bsonNumber).getValue();
    } else if (bsonNumber instanceof BsonDecimal128) {
      return ((BsonDecimal128) bsonNumber).getValue().bigDecimalValue();
    } else {
      throw new IllegalArgumentException("Unsupported BsonNumber type: " + bsonNumber.getClass());
    }
  }

  private static TestFieldValue getNumber(BsonNumber bsonNumber) {
    TestFieldValue testFieldValue = new TestFieldValue();
    testFieldValue.withN(getNumberFromBsonNumber(bsonNumber));
    return testFieldValue;
  }

  public static void main(String[] args) throws Exception {
    TestFieldsMap map = getFieldMap1();
    byte[] bytes = getRawBsonBytes(map);
    TestFieldsMap map2 = getPhoenixFieldMap(bytes);
    System.out.println(map.equals(map2));
  }

  private static TestFieldsMap getFieldMap1() {
    TestFieldsMap testFieldsMap = new TestFieldsMap();
    Map<String, TestFieldValue> map = new HashMap<>();
    map.put("attr_0", new TestFieldValue().withS("str_val_0"));
    map.put("attr_1", new TestFieldValue().withN(1295.03));
    map.put("attr_5", new TestFieldValue().withL(
        new TestFieldValue().withN(1234),
        new TestFieldValue().withS("str001"),
        new TestFieldValue().withB(new SerializableBytesPtr(
            new byte[] {0, 1, 2, 3, 4, 5}))));
    Map<String, TestFieldValue> nMap1 = new HashMap<>();
    nMap1.put("n_attr_0", new TestFieldValue().withS("str_val_0"));
    nMap1.put("n_attr_1", new TestFieldValue().withN(1295.03));
    String bytesFieldVal1 = "2048u5nblwjeiWFGTH(4bf930";
    byte[] bytesAttrVal1 = bytesFieldVal1.getBytes();
    nMap1.put("n_attr_2", new TestFieldValue().withB(new SerializableBytesPtr(
        bytesAttrVal1)));
    nMap1.put("n_attr_3", new TestFieldValue().withBOOL(true));
    nMap1.put("n_attr_4", new TestFieldValue().withNULL(true));
    map.put("attr_6", new TestFieldValue().withM(nMap1));
    map.put("Id", new TestFieldValue().withN(101.01));
    map.put("IdS", new TestFieldValue().withS("101.01"));
    map.put("Id2", new TestFieldValue().withN(101.01));
    map.put("ColorBytes",
        new TestFieldValue().withB(new SerializableBytesPtr(Bytes.toBytes("Black"))));
    map.put("RelatedItems",
        new TestFieldValue().withNS(1234, -485.45582904, 123.0948, 0.111));
    map.put("Pictures", new TestFieldValue().withSS(
        "http://example.com/products/123_rear.jpg",
        "http://example.com/products/xyz_rear.jpg",
        "http://example.com/products/123_front.jpg",
        "http://example.com/products/123_front.jpg"
    ));
    map.put("PictureBinarySet", new TestFieldValue().withBS(
        new SerializableBytesPtr(Bytes.toBytes("http://example.com/products/123_rear.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("http://example.com/products/xyz_rear.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("http://example.com/products/123_front.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("http://example.com/products/123_front.jpg"))
    ));
    map.put("Title", new TestFieldValue().withS("Book 101 Title"));
    map.put("ISBN", new TestFieldValue().withS("111-1111111111"));
    map.put("InPublication", new TestFieldValue().withBOOL(false));
    Map<String, TestFieldValue> nestedMap1 = new HashMap<>();
    nestedMap1.put("Id", new TestFieldValue().withN(101.01));
    nestedMap1.put("Title", new TestFieldValue().withS("Book 101 Title"));
    nestedMap1.put("ISBN", new TestFieldValue().withS("111-1111111111"));
    nestedMap1.put("InPublication", new TestFieldValue().withBOOL(false));
    nestedMap1.put("NList1",
        new TestFieldValue().withL(new TestFieldValue().withS("NListVal01"),
            new TestFieldValue().withN(-0.00234)));
    map.put("NestedMap1", new TestFieldValue().withM(nestedMap1));
    Map<String, TestFieldValue> nestedList1Map1 = new HashMap<>();
    nestedList1Map1.put("Id", new TestFieldValue().withN(101.01));
    nestedList1Map1.put("Title", new TestFieldValue().withS("Book 101 Title"));
    nestedList1Map1.put("ISBN", new TestFieldValue().withS("111-1111111111"));
    nestedList1Map1.put("InPublication", new TestFieldValue().withBOOL(false));
    nestedList1Map1.put("IdSet",
        new TestFieldValue().withNS(204850.69703847596, -39457860.486939476, 20576024,
            19306873, 4869067048362749590684D));
    nestedList1Map1.put("TitleSet",
        new TestFieldValue().withSS("Book 1010 Title", "Book 1011 Title",
            "Book 1111 Title"));
    nestedList1Map1.put("BinaryTitleSet", new TestFieldValue().withBS(
        new SerializableBytesPtr(Bytes.toBytes("Book 1010 Title Binary")),
        new SerializableBytesPtr(Bytes.toBytes("Book 1011 Title Binary")),
        new SerializableBytesPtr(Bytes.toBytes("Book 1111 Title Binary"))
        ));
    map.put("NestedList1",
        new TestFieldValue().withL(new TestFieldValue().withN(-485.34),
            new TestFieldValue().withS("1234abcd"),
            new TestFieldValue().withL(new TestFieldValue().withS("xyz0123"),
                new TestFieldValue().withM(nestedList1Map1))));
    testFieldsMap.setMap(map);
    return testFieldsMap;
  }

  public static TestFieldsMap getPhoenixFieldMap(String value) {
      TestFieldsMap map;
      try {
          map = MAPPER.readerFor(TestFieldsMap.class).readValue(value);
      } catch (JsonProcessingException e) {
          throw new UncheckedIOException(e);
      }
      return map;
  }

}
