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
package org.apache.phoenix.expression.function;

import static org.junit.Assert.assertEquals;

import java.util.regex.Pattern;
import org.junit.Test;

/**
 * Unit tests for {@link RegexpLikeFunction#parseMatchParameter(String)}.
 */
public class RegexpLikeFunctionTest {

  @Test
  public void testNullMatchParameter() {
    assertEquals(0, RegexpLikeFunction.parseMatchParameter(null));
  }

  @Test
  public void testEmptyMatchParameter() {
    assertEquals(0, RegexpLikeFunction.parseMatchParameter(""));
  }

  @Test
  public void testCaseInsensitiveFlag() {
    int flags = RegexpLikeFunction.parseMatchParameter("i");
    assertEquals(Pattern.CASE_INSENSITIVE, flags & Pattern.CASE_INSENSITIVE);
  }

  @Test
  public void testCaseSensitiveFlag() {
    // 'c' alone should result in no CASE_INSENSITIVE flag (default behavior)
    int flags = RegexpLikeFunction.parseMatchParameter("c");
    assertEquals(0, flags & Pattern.CASE_INSENSITIVE);
    assertEquals(0, flags); // no flags set at all
  }

  @Test
  public void testMultilineFlag() {
    int flags = RegexpLikeFunction.parseMatchParameter("m");
    assertEquals(Pattern.MULTILINE, flags & Pattern.MULTILINE);
  }

  @Test
  public void testDotallFlag() {
    int flags = RegexpLikeFunction.parseMatchParameter("s");
    assertEquals(Pattern.DOTALL, flags & Pattern.DOTALL);
  }

  @Test
  public void testCaseInsensitiveThenCaseSensitive_LastWins() {
    // 'ic' — 'c' comes last, so case-sensitive (no CASE_INSENSITIVE flag)
    int flags = RegexpLikeFunction.parseMatchParameter("ic");
    assertEquals(0, flags & Pattern.CASE_INSENSITIVE);
  }

  @Test
  public void testCaseSensitiveThenCaseInsensitive_LastWins() {
    // 'ci' — 'i' comes last, so case-insensitive
    int flags = RegexpLikeFunction.parseMatchParameter("ci");
    assertEquals(Pattern.CASE_INSENSITIVE, flags & Pattern.CASE_INSENSITIVE);
  }

  @Test
  public void testCombinedCaseInsensitiveAndDotall() {
    // 'is' — both case-insensitive and dotall
    int flags = RegexpLikeFunction.parseMatchParameter("is");
    assertEquals(Pattern.CASE_INSENSITIVE, flags & Pattern.CASE_INSENSITIVE);
    assertEquals(Pattern.DOTALL, flags & Pattern.DOTALL);
  }

  @Test
  public void testCombinedMultilineAndDotall() {
    // 'ms' — both multiline and dotall
    int flags = RegexpLikeFunction.parseMatchParameter("ms");
    assertEquals(Pattern.MULTILINE, flags & Pattern.MULTILINE);
    assertEquals(Pattern.DOTALL, flags & Pattern.DOTALL);
  }

  @Test
  public void testAllFlagsCombined() {
    // 'ims' — case-insensitive, multiline, and dotall
    int flags = RegexpLikeFunction.parseMatchParameter("ims");
    assertEquals(Pattern.CASE_INSENSITIVE, flags & Pattern.CASE_INSENSITIVE);
    assertEquals(Pattern.MULTILINE, flags & Pattern.MULTILINE);
    assertEquals(Pattern.DOTALL, flags & Pattern.DOTALL);
  }

  @Test
  public void testAllFlagsWithCaseSensitiveOverride() {
    // 'imsc' — 'c' at end overrides 'i', so no CASE_INSENSITIVE, but multiline and dotall remain
    int flags = RegexpLikeFunction.parseMatchParameter("imsc");
    assertEquals(0, flags & Pattern.CASE_INSENSITIVE);
    assertEquals(Pattern.MULTILINE, flags & Pattern.MULTILINE);
    assertEquals(Pattern.DOTALL, flags & Pattern.DOTALL);
  }

  @Test
  public void testDuplicateFlags() {
    // 'iimm' — duplicate flags should be idempotent
    int flags = RegexpLikeFunction.parseMatchParameter("iimm");
    assertEquals(Pattern.CASE_INSENSITIVE, flags & Pattern.CASE_INSENSITIVE);
    assertEquals(Pattern.MULTILINE, flags & Pattern.MULTILINE);
  }

  @Test
  public void testComplexOverrideSequence() {
    // 'icims' — i, then c (clears i), then i (sets again), then m, then s
    int flags = RegexpLikeFunction.parseMatchParameter("icims");
    assertEquals(Pattern.CASE_INSENSITIVE, flags & Pattern.CASE_INSENSITIVE);
    assertEquals(Pattern.MULTILINE, flags & Pattern.MULTILINE);
    assertEquals(Pattern.DOTALL, flags & Pattern.DOTALL);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFlagThrowsException() {
    RegexpLikeFunction.parseMatchParameter("x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFlagInCombination() {
    // 'iz' — 'z' is invalid, should throw even though 'i' is valid
    RegexpLikeFunction.parseMatchParameter("iz");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNumericFlagThrowsException() {
    RegexpLikeFunction.parseMatchParameter("1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUpperCaseFlagThrowsException() {
    // 'I' (uppercase) is not valid — only lowercase 'i' is
    RegexpLikeFunction.parseMatchParameter("I");
  }
}
