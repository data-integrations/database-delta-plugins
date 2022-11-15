/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.delta.sqlserver;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import io.cdap.delta.api.DeltaFailureRuntimeException;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.Offset;
import io.cdap.delta.plugin.mock.MockContext;
import io.cdap.delta.plugin.mock.MockEventEmitter;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class SqlServerRecordConsumerTest {
  private static final String DATABASE = "AdventureWorks2014";
  private static final String TOPICNAME = "dbo.testreplication.npe";

  @Test(expected = DeltaFailureRuntimeException.class)
  public void testTableWithoutPrimaryKey() {
    DeltaSourceContext context = new MockContext(SQLServerDriver.class);
    MockEventEmitter eventEmitter = new MockEventEmitter(5);
    SqlServerRecordConsumer sqlServerRecordConsumer = new SqlServerRecordConsumer
      (context, eventEmitter, DATABASE, new HashSet<>(), new HashMap<>(), new Offset(), true);
    SourceRecord sourceRecordMock = Mockito.mock(SourceRecord.class);
    // the topic name should be in this form: [db.server.name].[schema].[table]
    Mockito.when(sourceRecordMock.topic()).thenReturn(TOPICNAME);
    List<Field> fields = Arrays.asList(new Field("op", 0, new ConnectSchema(Schema.Type.STRING)));
    Struct valueStruct = new Struct(new ConnectSchema(Schema.Type.STRUCT, false, null,
                                                      null, null, null, null, fields, null, null));
    valueStruct.put("op", "c");
    Mockito.when(sourceRecordMock.value()).thenReturn(valueStruct);
    Mockito.when(sourceRecordMock.sourceOffset()).thenReturn(new HashMap() {{
      put("snapshot", true);
    }});
    //setting primary key as NULL
    Mockito.when(sourceRecordMock.key()).thenReturn(null);
    sqlServerRecordConsumer.accept(sourceRecordMock);
  }

}
