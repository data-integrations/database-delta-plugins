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

package io.cdap.delta.sqlserver;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableRegistry;

import java.util.List;

/**
 * Sql Server delta source.
 */
@Plugin(type = DeltaSource.PLUGIN_TYPE)
@Name(SqlServerDeltaSource.NAME)
@Description("Delta source for SqlServer.")
public class SqlServerDeltaSource implements DeltaSource {
  public static final String NAME = "sqlserver";
  private final SqlServerConfig config;

  public SqlServerDeltaSource(SqlServerConfig config) {
    this.config = config;
  }


  @Override
  public void configure(Configurer configurer) {

  }

  @Override
  public EventReader createReader(EventReaderDefinition definition, DeltaSourceContext context, EventEmitter emitter) {
    // TODO: use the tables passed in to read the required tables and columns
    return new SqlServerEventReader(definition.getTables(), config, context, emitter);
  }

  @Override
  public TableRegistry createTableRegistry(Configurer configurer) {
    return null;
  }

  @Override
  public TableAssessor<TableDetail> createTableAssessor(Configurer configurer) throws Exception {
    // TODO: implement accesssment, this is to fix complile error
    return null;
  }
}
