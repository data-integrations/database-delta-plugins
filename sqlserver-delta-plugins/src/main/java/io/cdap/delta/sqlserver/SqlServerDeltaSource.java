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
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.common.DriverCleanup;

import java.sql.Driver;

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
    // add Sql-Server JDBC Plugin usage
    configurer.usePluginClass("jdbc", config.getJdbcPluginName(), config.getJDBCPluginId(),
                              PluginProperties.builder().build());
  }

  @Override
  public EventReader createReader(EventReaderDefinition definition, DeltaSourceContext context, EventEmitter emitter) {
    return new SqlServerEventReader(definition.getTables(), config, context, emitter);
  }

  @Override
  public TableRegistry createTableRegistry(Configurer configurer) {
    Class<? extends Driver> jdbcDriverClass = configurer.usePluginClass("jdbc", config.getJdbcPluginName(),
                                                                        config.getJDBCPluginId(),
                                                                        PluginProperties.builder().build());
    if (jdbcDriverClass == null) {
      throw new IllegalArgumentException("JDBC plugin " + config.getJdbcPluginName() + " not found.");
    }
    try {
      DriverCleanup cleanup = DriverCleanup.ensureJDBCDriverIsAvailable(
        jdbcDriverClass, String.format("jdbc:sqlserver://%s:%d", config.getHost(), config.getPort()));
      return new SqlServerTableRegistry(config, cleanup);
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate JDBC driver", e);
    }
  }

  @Override
  public TableAssessor<TableDetail> createTableAssessor(Configurer configurer) throws Exception {
    return new SqlServerTableAssessor();
  }
}
