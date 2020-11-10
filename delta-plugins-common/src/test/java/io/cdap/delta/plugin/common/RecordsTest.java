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

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test case for {@link Records} class.
 */
public class RecordsTest {

  @Test
  public void testConvert() {
    String fieldName = "priority";
    Schema dataSchema = SchemaBuilder.struct().name("TinyIntSchema").field(fieldName, Schema.INT16_SCHEMA).build();
    Struct struct = new Struct(dataSchema);
    Short val = 1;
    struct.put(fieldName, val);
    StructuredRecord convert = Records.convert(struct);
    io.cdap.cdap.api.data.schema.Schema.Field priority = convert.getSchema().getField(fieldName);
    Assert.assertNotNull(priority);
    Assert.assertEquals(priority.getSchema(), io.cdap.cdap.api.data.schema.Schema.of(
      io.cdap.cdap.api.data.schema.Schema.Type.INT));
    Assert.assertEquals(val.intValue(), (int) convert.get(fieldName));
  }
}
