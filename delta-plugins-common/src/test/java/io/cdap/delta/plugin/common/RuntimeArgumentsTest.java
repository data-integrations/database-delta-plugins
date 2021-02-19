/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.delta.plugin.common;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Test for RuntimeArguments.
 */
public class RuntimeArgumentsTest {

  @Test
  public void testRemovePrefix() {
    Map<String, String> arguments = ImmutableMap.of("source.connector.database.host", "1.1.1.1",
                                                    "source.connector.name", "somename",
                                                    "somekey", "somevalue");
    Map<String, String> expected = ImmutableMap.of("database.host", "1.1.1.1",
                                                   "name", "somename");

    Assert.assertEquals(expected, RuntimeArguments.extractPrefixed("source.connector.", arguments));
  }
}
