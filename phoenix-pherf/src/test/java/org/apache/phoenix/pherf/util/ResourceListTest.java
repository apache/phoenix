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
package org.apache.phoenix.pherf.util;

import java.io.File;
import java.util.Collections;
import java.util.regex.Pattern;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ResourceListTest {

  @Test
  public void testMissingJarFileReturnsGracefully() {
    ResourceList rl = new ResourceList("foo");
    File missingFile = new File("abracadabraphoenix.txt");
    assertFalse("Did not expect a fake test file to actually exist", missingFile.exists());
    assertEquals(Collections.emptyList(), rl.getResourcesFromJarFile(missingFile, Pattern.compile("pattern")));
  }

}
