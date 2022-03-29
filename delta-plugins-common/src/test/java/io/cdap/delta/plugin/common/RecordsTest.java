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
import io.cdap.cdap.api.data.schema.Schema;
import io.debezium.time.ZonedTimestamp;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Test case for {@link Records} class.
 */
public class RecordsTest {

  @Test
  public void testConvertTinyInt() {
    String fieldName = "priority";
    org.apache.kafka.connect.data.Schema dataSchema = SchemaBuilder
                                                        .struct()
                                                        .name("TinyIntSchema")
                                                        .field(fieldName,
                                                               org.apache.kafka.connect.data.Schema.INT16_SCHEMA)
                                                        .build();
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

  @Test
  public void testConvertTimeStamp() {
    String fieldName = "timeCreated";
    org.apache.kafka.connect.data.Schema dataSchema =
      SchemaBuilder.struct()
        .name(fieldName).field(fieldName, ZonedTimestamp.schema()).build();
    Struct struct = new Struct(dataSchema);
    String val = "2011-12-03T10:15:30.030431+01:00";
    struct.put(fieldName, val);
    StructuredRecord converted = Records.convert(struct);
    Assert.assertEquals(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS),
                        converted.getSchema().getField(fieldName).getSchema());
    Assert.assertEquals(converted.getTimestamp(fieldName),
                        ZonedDateTime.parse(val).withZoneSameInstant(ZoneId.of("UTC")));


  }
}
