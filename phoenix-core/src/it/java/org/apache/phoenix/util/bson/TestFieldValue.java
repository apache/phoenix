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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

/**
 * The value of specific data type (scalar or recursive structure).
 */
public class TestFieldValue implements Serializable, Cloneable {

  /**
   * An attribute of type String.
   */
  private String varchar;
  /**
   * An attribute of type Number.
   */
  private Number number;
  /**
   * An attribute of type Binary.
   */
  private SerializableBytesPtr binary;
  /**
   * An attribute of type String Set.
   */
  private Set<String> varcharset;
  /**
   * An attribute of type Number Set.
   */
  private Set<Number> numberset;
  /**
   * An attribute of type Binary Set.
   */
  private Set<SerializableBytesPtr> binaryset;
  /**
   * An attribute of type Map.
   */
  private Map<String, TestFieldValue> map;
  /**
   * An attribute of type List.
   */
  private List<TestFieldValue> list;
  /**
   * An attribute of type Null.
   */
  private Boolean nULLValue;
  /**
   * An attribute of type Boolean.
   */
  private Boolean bool;

  /**
   * Default constructor for TestFieldValue object. Callers should use the setter or fluent setter (with...) methods
   * to initialize the object after creating it.
   */
  public TestFieldValue() {
  }

  /**
   * Constructs a new TestFieldValue object. Callers should use the setter or fluent setter (with...) methods to
   * initialize any additional object members.
   *
   * @param varchar An attribute of type String. For example:</p>
   *          <p>
   *          <code>"S": "Hello"</code>
   */
  public TestFieldValue(String varchar) {
    setVarchar(varchar);
  }

  /**
   * Constructs a new TestFieldValue object. Callers should use the setter or fluent setter (with...) methods to
   * initialize any additional object members.
   *
   * @param varcharset An attribute of type String Set. For example:</p>
   *           <p>
   *           <code>"SS": ["Giraffe", "Hippo" ,"Zebra"]</code>
   */
  public TestFieldValue(List<String> varcharset) {
    setVarcharset(varcharset);
  }

  /**
   * <p>
   * An attribute of type String. For example:
   * </p>
   * <p>
   * <code>"S": "Hello"</code>
   * </p>
   *
   * @param s An attribute of type String. For example:</p>
   *          <p>
   *          <code>"S": "Hello"</code>
   */

  public void setVarchar(String s) {
    this.varchar = s;
  }

  /**
   * <p>
   * An attribute of type String. For example:
   * </p>
   * <p>
   * <code>"S": "Hello"</code>
   * </p>
   *
   * @return An attribute of type String. For example:</p>
   * <p>
   * <code>"S": "Hello"</code>
   */

  public String getVarchar() {
    return this.varchar;
  }

  /**
   * <p>
   * An attribute of type String. For example:
   * </p>
   * <p>
   * <code>"S": "Hello"</code>
   * </p>
   *
   * @param s An attribute of type String. For example:</p>
   *          <p>
   *          <code>"S": "Hello"</code>
   * @return Returns a reference to this object so that method calls can be chained together.
   */

  public TestFieldValue withS(String s) {
    setVarchar(s);
    return this;
  }

  /**
   * <p>
   * An attribute of type Number. For example:
   * </p>
   * <p>
   * <code>"N": "123.45"</code>
   * </p>
   *
   * @param number An attribute of type Number. For example:</p>
   *          <p>
   *          <code>"N": "123.45"</code>
   *          </p>
   */

  public void setNumber(Number number) {
    this.number = number;
  }

  /**
   * <p>
   * An attribute of type Number. For example:
   * </p>
   * <p>
   * <code>"N": "123.45"</code>
   * </p>
   *
   * @return An attribute of type Number. For example:</p>
   * <p>
   * <code>"N": "123.45"</code>
   * </p>
   */

  public Number getNumber() {
    return this.number;
  }

  /**
   * <p>
   * An attribute of type Number. For example:
   * </p>
   * <p>
   * <code>"N": "123.45"</code>
   * </p>
   *
   * @param n An attribute of type Number. For example:</p>
   *          <p>
   *          <code>"N": "123.45"</code>
   *          </p>
   * @return Returns a reference to this object so that method calls can be chained together.
   */

  public TestFieldValue withN(Number n) {
    setNumber(n);
    return this;
  }

  /**
   * <p>
   * An attribute of type Binary. For example:
   * </p>
   * <p>
   * <code>"B": "dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"</code>
   * </p>
   * <p>
   * This String represents Base64 encoded bytes.
   * </p>
   *
   * @param binary An attribute of type Binary. For example:</p>
   *          <p>
   *          <code>"B": "dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"</code>
   */
  public void setBinary(SerializableBytesPtr binary) {
    this.binary = binary;
  }

  /**
   * <p>
   * An attribute of type Binary. For example:
   * </p>
   * <p>
   * <code>"B": "dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"</code>
   * </p>
   * <p>
   * This String represents Base64 encoded bytes.
   * </p>
   *
   * @return An attribute of type Binary. For example:</p>
   * <p>
   * <code>"B": "dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"</code>
   */
  public SerializableBytesPtr getBinary() {
    return this.binary;
  }

  /**
   * <p>
   * An attribute of type Binary. For example:
   * </p>
   * <p>
   * <code>"B": "dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"</code>
   * </p>
   * <p>
   * This String represents Base64 encoded bytes.
   * </p>
   *
   * @param b An attribute of type Binary. For example:</p>
   *          <p>
   *          <code>"B": "dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"</code>
   * @return Returns a reference to this object so that method calls can be chained together.
   */

  public TestFieldValue withB(SerializableBytesPtr b) {
    setBinary(b);
    return this;
  }

  /**
   * <p>
   * An attribute of type String Set. For example:
   * </p>
   * <p>
   * <code>"SS": ["Giraffe", "Hippo" ,"Zebra"]</code>
   * </p>
   *
   * @return An attribute of type String Set. For example:</p>
   * <p>
   * <code>"SS": ["Giraffe", "Hippo" ,"Zebra"]</code>
   */
  public Set<String> getVarcharset() {
    return varcharset;
  }

  /**
   * <p>
   * An attribute of type String Set. For example:
   * </p>
   * <p>
   * <code>"SS": ["Giraffe", "Hippo" ,"Zebra"]</code>
   * </p>
   *
   * @param sS An attribute of type String Set. For example:</p>
   *           <p>
   *           <code>"SS": ["Giraffe", "Hippo" ,"Zebra"]</code>
   */
  public void setVarcharset(Collection<String> sS) {
    if (sS == null) {
      this.varcharset = null;
      return;
    }

    this.varcharset = new HashSet<>(sS);
  }

  /**
   * <p>
   * An attribute of type String Set. For example:
   * </p>
   * <p>
   * <code>"SS": ["Giraffe", "Hippo" ,"Zebra"]</code>
   * </p>
   * <p>
   * <b>NOTE:</b> This method appends the values to the existing list (if any). Use
   * {@link #setVarcharset(Collection)} or {@link #withSS(Collection)} if you want to override the
   * existing values.
   * </p>
   *
   * @param sS An attribute of type String Set. For example:</p>
   *           <p>
   *           <code>"SS": ["Giraffe", "Hippo" ,"Zebra"]</code>
   * @return Returns a reference to this object so that method calls can be chained together.
   */
  public TestFieldValue withSS(String... sS) {
    if (this.varcharset == null) {
      setVarcharset(new ArrayList<String>(sS.length));
    }
    for (String ele : sS) {
      this.varcharset.add(ele);
    }
    Preconditions.checkArgument(new HashSet<>(this.varcharset).size() == this.varcharset.size(),
        "Set should not contain duplicate elements");
    return this;
  }

  /**
   * <p>
   * An attribute of type String Set. For example:
   * </p>
   * <p>
   * <code>"SS": ["Giraffe", "Hippo" ,"Zebra"]</code>
   * </p>
   *
   * @param sS An attribute of type String Set. For example:</p>
   *           <p>
   *           <code>"SS": ["Giraffe", "Hippo" ,"Zebra"]</code>
   * @return Returns a reference to this object so that method calls can be chained together.
   */

  public TestFieldValue withSS(Collection<String> sS) {
    setVarcharset(sS);
    return this;
  }

  /**
   * <p>
   * An attribute of type Number Set. For example:
   * </p>
   * <p>
   * <code>"NS": ["42.2", "-19", "7.5", "3.14"]</code>
   * </p>
   *
   * @return An attribute of type Number Set. For example:</p>
   * <p>
   * <code>"NS": ["42.2", "-19", "7.5", "3.14"]</code>
   * </p>
   */
  public Set<Number> getNumberset() {
    return numberset;
  }

  /**
   * <p>
   * An attribute of type Number Set. For example:
   * </p>
   * <p>
   * <code>"NS": ["42.2", "-19", "7.5", "3.14"]</code>
   * </p>
   *
   * @param nS An attribute of type Number Set. For example:</p>
   *           <p>
   *           <code>"NS": ["42.2", "-19", "7.5", "3.14"]</code>
   *           </p>
   */
  public void setNumberset(Collection<Number> nS) {
    if (nS == null) {
      this.numberset = null;
      return;
    }

    this.numberset = new HashSet<>(nS);
  }

  /**
   * <p>
   * An attribute of type Number Set. For example:
   * </p>
   * <p>
   * <code>"NS": ["42.2", "-19", "7.5", "3.14"]</code>
   * </p>
   * <p>
   * <b>NOTE:</b> This method appends the values to the existing list (if any). Use
   * {@link #setNumberset(Collection)} or {@link #withNS(Collection)} if you want to override the
   * existing values.
   * </p>
   *
   * @param nS An attribute of type Number Set. For example:</p>
   *           <p>
   *           <code>"NS": ["42.2", "-19", "7.5", "3.14"]</code>
   *           </p>
   * @return Returns a reference to this object so that method calls can be chained together.
   */
  public TestFieldValue withNS(Number... nS) {
    if (this.numberset == null) {
      setNumberset(new ArrayList<>(nS.length));
    }
    for (Number ele : nS) {
      this.numberset.add(ele);
    }
    Preconditions.checkArgument(new HashSet<>(this.numberset).size() == this.numberset.size(),
        "Set should not contain duplicate elements");
    return this;
  }

  /**
   * <p>
   * An attribute of type Number Set. For example:
   * </p>
   * <p>
   * <code>"NS": ["42.2", "-19", "7.5", "3.14"]</code>
   * </p>
   *
   * @param nS An attribute of type Number Set. For example:</p>
   *           <p>
   *           <code>"NS": ["42.2", "-19", "7.5", "3.14"]</code>
   *           </p>
   * @return Returns a reference to this object so that method calls can be chained together.
   */
  public TestFieldValue withNS(Collection<Number> nS) {
    setNumberset(nS);
    return this;
  }

  /**
   * <p>
   * An attribute of type Binary Set. For example:
   * </p>
   * <p>
   * <code>"BS": ["U3Vubnk=", "UmFpbnk=", "U25vd3k="]</code>
   * </p>
   *
   * @return An attribute of type Binary Set. For example:</p>
   * <p>
   * <code>"BS": ["U3Vubnk=", "UmFpbnk=", "U25vd3k="]</code>
   */
  public Set<SerializableBytesPtr> getBinaryset() {
    return binaryset;
  }

  /**
   * <p>
   * An attribute of type Binary Set. For example:
   * </p>
   * <p>
   * <code>"BS": ["U3Vubnk=", "UmFpbnk=", "U25vd3k="]</code>
   * </p>
   * <p>
   * The String represents Base64 encoded bytes.
   * </p>
   *
   * @param bS An attribute of type Binary Set. For example:</p>
   *           <p>
   *           <code>"BS": ["U3Vubnk=", "UmFpbnk=", "U25vd3k="]</code>
   */
  public void setBinaryset(Collection<SerializableBytesPtr> bS) {
    if (bS == null) {
      this.binaryset = null;
      return;
    }

    this.binaryset = new HashSet<>(bS);
  }

  /**
   * <p>
   * An attribute of type Binary Set. For example:
   * </p>
   * <p>
   * <code>"BS": ["U3Vubnk=", "UmFpbnk=", "U25vd3k="]</code>
   * </p>
   * <p>
   * <b>NOTE:</b> This method appends the values to the existing list (if any). Use
   * {@link #setBinaryset(Collection)} or {@link #withBS(Collection)} if you want to override the
   * existing values.
   * </p>
   *
   * @param bS An attribute of type Binary Set. For example:</p>
   *           <p>
   *           <code>"BS": ["U3Vubnk=", "UmFpbnk=", "U25vd3k="]</code>
   * @return Returns a reference to this object so that method calls can be chained together.
   */
  public TestFieldValue withBS(SerializableBytesPtr... bS) {
    if (this.binaryset == null) {
      setBinaryset(new ArrayList<>(bS.length));
    }
    for (SerializableBytesPtr ele : bS) {
      this.binaryset.add(ele);
    }
    Preconditions.checkArgument(new HashSet<>(this.binaryset).size() == this.binaryset.size(),
        "Set should not contain duplicate elements");
    return this;
  }

  /**
   * <p>
   * An attribute of type Binary Set. For example:
   * </p>
   * <p>
   * <code>"BS": ["U3Vubnk=", "UmFpbnk=", "U25vd3k="]</code>
   * </p>
   *
   * @param bS An attribute of type Binary Set. For example:</p>
   *           <p>
   *           <code>"BS": ["U3Vubnk=", "UmFpbnk=", "U25vd3k="]</code>
   * @return Returns a reference to this object so that method calls can be chained together.
   */
  public TestFieldValue withBS(Collection<SerializableBytesPtr> bS) {
    setBinaryset(bS);
    return this;
  }

  /**
   * <p>
   * An attribute of type Map. For example:
   * </p>
   * <p>
   * <code>"M": {"Name": {"S": "Joe"}, "Age": {"N": "35"}}</code>
   * </p>
   *
   * @return An attribute of type Map. For example:</p>
   * <p>
   * <code>"M": {"Name": {"S": "Joe"}, "Age": {"N": "35"}}</code>
   */
  public Map<String, TestFieldValue> getMap() {
    return map;
  }

  /**
   * <p>
   * An attribute of type Map. For example:
   * </p>
   * <p>
   * <code>"M": {"Name": {"S": "Joe"}, "Age": {"N": "35"}}</code>
   * </p>
   *
   * @param map An attribute of type Map. For example:</p>
   *          <p>
   *          <code>"M": {"Name": {"S": "Joe"}, "Age": {"N": "35"}}</code>
   */
  public void setMap(Map<String, TestFieldValue> map) {
    this.map = map;
  }

  /**
   * <p>
   * An attribute of type Map. For example:
   * </p>
   * <p>
   * <code>"M": {"Name": {"S": "Joe"}, "Age": {"N": "35"}}</code>
   * </p>
   *
   * @param m An attribute of type Map. For example:</p>
   *          <p>
   *          <code>"M": {"Name": {"S": "Joe"}, "Age": {"N": "35"}}</code>
   * @return Returns a reference to this object so that method calls can be chained together.
   */
  public TestFieldValue withM(Map<String, TestFieldValue> m) {
    setMap(m);
    return this;
  }

  /**
   * Add a single M entry
   *
   * @returns a reference to this object so that method calls can be chained together.
   * @see TestFieldValue#withM
   */
  public TestFieldValue addMEntry(String key, TestFieldValue value) {
    if (null == this.map) {
      this.map = new HashMap<>();
    }
    if (this.map.containsKey(key)) {
      throw new IllegalArgumentException("Duplicated keys (" + key.toString() + ") are provided.");
    }
    this.map.put(key, value);
    return this;
  }

  /**
   * Removes all the entries added into M.
   *
   * @return Returns a reference to this object so that method calls can be chained together.
   */
  public TestFieldValue clearMEntries() {
    this.map = null;
    return this;
  }

  /**
   * <p>
   * An attribute of type List. For example:
   * </p>
   * <p>
   * <code>"L": [ {"S": "Cookies"} , {"S": "Coffee"}, {"N": "3.14159"}]</code>
   * </p>
   *
   * @return An attribute of type List. For example:</p>
   * <p>
   * <code>"L": [ {"S": "Cookies"} , {"S": "Coffee"}, {"N": "3.14159"}]</code>
   */

  public List<TestFieldValue> getList() {
    return list;
  }

  /**
   * <p>
   * An attribute of type List. For example:
   * </p>
   * <p>
   * <code>"L": [ {"S": "Cookies"} , {"S": "Coffee"}, {"N": "3.14159"}]</code>
   * </p>
   *
   * @param list An attribute of type List. For example:</p>
   *          <p>
   *          <code>"L": [ {"S": "Cookies"} , {"S": "Coffee"}, {"N": "3.14159"}]</code>
   */

  public void setList(Collection<TestFieldValue> list) {
    if (list == null) {
      this.list = null;
      return;
    }

    this.list = new ArrayList<TestFieldValue>(list);
  }

  /**
   * <p>
   * An attribute of type List. For example:
   * </p>
   * <p>
   * <code>"L": [ {"S": "Cookies"} , {"S": "Coffee"}, {"N": "3.14159"}]</code>
   * </p>
   * <p>
   * <b>NOTE:</b> This method appends the values to the existing list (if any). Use
   * {@link #setList(Collection)} or {@link #withL(Collection)} if you want to override the existing
   * values.
   * </p>
   *
   * @param l An attribute of type List. For example:</p>
   *          <p>
   *          <code>"L": [ {"S": "Cookies"} , {"S": "Coffee"}, {"N": "3.14159"}]</code>
   * @return Returns a reference to this object so that method calls can be chained together.
   */

  public TestFieldValue withL(TestFieldValue... l) {
    if (this.list == null) {
      setList(new ArrayList<TestFieldValue>(l.length));
    }
    for (TestFieldValue ele : l) {
      this.list.add(ele);
    }
    return this;
  }

  /**
   * <p>
   * An attribute of type List. For example:
   * </p>
   * <p>
   * <code>"L": [ {"S": "Cookies"} , {"S": "Coffee"}, {"N": "3.14159"}]</code>
   * </p>
   *
   * @param l An attribute of type List. For example:</p>
   *          <p>
   *          <code>"L": [ {"S": "Cookies"} , {"S": "Coffee"}, {"N": "3.14159"}]</code>
   * @return Returns a reference to this object so that method calls can be chained together.
   */

  public TestFieldValue withL(Collection<TestFieldValue> l) {
    setList(l);
    return this;
  }

  /**
   * <p>
   * An attribute of type Null. For example:
   * </p>
   * <p>
   * <code>"NULL": true</code>
   * </p>
   *
   * @param nULLValue An attribute of type Null. For example:</p>
   *                  <p>
   *                  <code>"NULL": true</code>
   */

  public void setNull(Boolean nULLValue) {
    this.nULLValue = nULLValue;
  }

  /**
   * <p>
   * An attribute of type Null. For example:
   * </p>
   * <p>
   * <code>"NULL": true</code>
   * </p>
   *
   * @return An attribute of type Null. For example:</p>
   * <p>
   * <code>"NULL": true</code>
   */
  public Boolean getNull() {
    return this.nULLValue;
  }

  /**
   * <p>
   * An attribute of type Null. For example:
   * </p>
   * <p>
   * <code>"NULL": true</code>
   * </p>
   *
   * @param nULLValue An attribute of type Null. For example:</p>
   *                  <p>
   *                  <code>"NULL": true</code>
   * @return Returns a reference to this object so that method calls can be chained together.
   */
  public TestFieldValue withNULL(Boolean nULLValue) {
    setNull(nULLValue);
    return this;
  }

  /**
   * <p>
   * An attribute of type Null. For example:
   * </p>
   * <p>
   * <code>"NULL": true</code>
   * </p>
   *
   * @return An attribute of type Null. For example:</p>
   * <p>
   * <code>"NULL": true</code>
   */
  public Boolean isNULL() {
    return this.nULLValue;
  }

  /**
   * <p>
   * An attribute of type Boolean. For example:
   * </p>
   * <p>
   * <code>"BOOL": true</code>
   * </p>
   *
   * @param bOOL An attribute of type Boolean. For example:</p>
   *             <p>
   *             <code>"BOOL": true</code>
   */
  public void setBoolean(Boolean bOOL) {
    this.bool = bOOL;
  }

  /**
   * <p>
   * An attribute of type Boolean. For example:
   * </p>
   * <p>
   * <code>"BOOL": true</code>
   * </p>
   *
   * @return An attribute of type Boolean. For example:</p>
   * <p>
   * <code>"BOOL": true</code>
   */
  public Boolean getBoolean() {
    return this.bool;
  }

  /**
   * <p>
   * An attribute of type Boolean. For example:
   * </p>
   * <p>
   * <code>"BOOL": true</code>
   * </p>
   *
   * @param bOOL An attribute of type Boolean. For example:</p>
   *             <p>
   *             <code>"BOOL": true</code>
   * @return Returns a reference to this object so that method calls can be chained together.
   */

  public TestFieldValue withBOOL(Boolean bOOL) {
    setBoolean(bOOL);
    return this;
  }


  /**
   * Returns a string representation of this object. This is useful for testing and debugging. Sensitive data will be
   * redacted from this string using a placeholder value.
   *
   * @return A string representation of this object.
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    if (getVarchar() != null)
      sb.append("S: ").append(getVarchar());
    if (getNumber() != null)
      sb.append("N: ").append(getNumber());
    if (getBinary() != null)
      sb.append("B: ").append(getBinary());
    if (getVarcharset() != null)
      sb.append("SS: ").append(getVarcharset());
    if (getNumberset() != null)
      sb.append("NS: ").append(getNumberset());
    if (getBinaryset() != null)
      sb.append("BS: ").append(getBinaryset());
    if (getMap() != null)
      sb.append("M: ").append(getMap());
    if (getList() != null)
      sb.append("L: ").append(getList());
    if (getNull() != null)
      sb.append("NULL: ").append(getNull());
    if (getBoolean() != null)
      sb.append("BOOL: ").append(getBoolean());
    sb.append("}");
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;

    if (!(obj instanceof TestFieldValue)) {
      return false;
    }
    TestFieldValue other = (TestFieldValue) obj;
    if (other.getVarchar() == null ^ this.getVarchar() == null) {
      return false;
    }
    if (other.getVarchar() != null && !other.getVarchar().equals(this.getVarchar())) {
      return false;
    }
    if (other.getNumber() == null ^ this.getNumber() == null) {
      return false;
    }
    if (other.getNumber() != null && !other.getNumber().equals(this.getNumber())) {
      return false;
    }
    if (other.getBinary() == null ^ this.getBinary() == null) {
      return false;
    }
    if (other.getBinary() != null && !other.getBinary().equals(this.getBinary())) {
      return false;
    }
    if (other.getVarcharset() == null ^ this.getVarcharset() == null) {
      return false;
    }
    if (other.getVarcharset() != null && !other.getVarcharset().equals(this.getVarcharset())) {
      return false;
    }
    if (other.getNumberset() == null ^ this.getNumberset() == null) {
      return false;
    }
    if (other.getNumberset() != null && !other.getNumberset().equals(this.getNumberset())) {
      return false;
    }
    if (other.getBinaryset() == null ^ this.getBinaryset() == null) {
      return false;
    }
    if (other.getBinaryset() != null && !other.getBinaryset().equals(this.getBinaryset())) {
      return false;
    }
    if (other.getMap() == null ^ this.getMap() == null) {
      return false;
    }
    if (other.getMap() != null && !other.getMap().equals(this.getMap())) {
      return false;
    }
    if (other.getList() == null ^ this.getList() == null) {
      return false;
    }
    if (other.getList() != null && !other.getList().equals(this.getList())) {
      return false;
    }
    if (other.getNull() == null ^ this.getNull() == null) {
      return false;
    }
    if (other.getNull() != null && !other.getNull().equals(this.getNull())) {
      return false;
    }
    if (other.getBoolean() == null ^ this.getBoolean() == null) {
      return false;
    }
    if (other.getBoolean() != null && !other.getBoolean().equals(this.getBoolean())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int hashCode = 1;

    hashCode = prime * hashCode + ((getVarchar() == null) ? 0 : getVarchar().hashCode());
    hashCode = prime * hashCode + ((getNumber() == null) ? 0 : getNumber().hashCode());
    hashCode = prime * hashCode + ((getBinary() == null) ? 0 : getBinary().hashCode());
    hashCode = prime * hashCode + ((getVarcharset() == null) ? 0 : getVarcharset().hashCode());
    hashCode = prime * hashCode + ((getNumberset() == null) ? 0 : getNumberset().hashCode());
    hashCode = prime * hashCode + ((getBinaryset() == null) ? 0 : getBinaryset().hashCode());
    hashCode = prime * hashCode + ((getMap() == null) ? 0 : getMap().hashCode());
    hashCode = prime * hashCode + ((getList() == null) ? 0 : getList().hashCode());
    hashCode = prime * hashCode + ((getNull() == null) ? 0 : getNull().hashCode());
    hashCode = prime * hashCode + ((getBoolean() == null) ? 0 : getBoolean().hashCode());
    return hashCode;
  }

  @Override
  public TestFieldValue clone() {
    try {
      return (TestFieldValue) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(
          "Got a CloneNotSupportedException from Object.clone() even though we're Cloneable!",
          e);
    }
  }

}
