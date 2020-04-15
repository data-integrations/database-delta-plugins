/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.delta.mysql;

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
import io.cdap.delta.plugin.common.DriverCleanup;

import java.sql.Driver;
import java.util.UUID;

/**
 * Mysql origin.
 */
@Plugin(type = DeltaSource.PLUGIN_TYPE)
@Name(MySqlDeltaSource.NAME)
@Description("Delta source for MySQL.")
public class MySqlDeltaSource implements DeltaSource {
  public static final String NAME = "mysql";
  private final MySqlConfig conf;

  public MySqlDeltaSource(MySqlConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configure(Configurer configurer) {
    // add MySql JDBC Plugin usage at configure time
    configurer.usePluginClass("jdbc", conf.getJdbcPluginName(), conf.getJDBCPluginId(),
                              PluginProperties.builder().build());
  }

  @Override
  public EventReader createReader(EventReaderDefinition definition, DeltaSourceContext context,
                                  EventEmitter eventEmitter) {
    return new MySqlEventReader(definition.getTables(), conf, context, eventEmitter);
  }

  @Override
  public TableRegistry createTableRegistry(Configurer configurer) throws Exception {
    return new MySqlTableRegistry(conf, getDriverCleanup(configurer));
  }

  @Override
  public TableAssessor<TableDetail> createTableAssessor(Configurer configurer) throws Exception {
    return new MySqlTableAssessor(conf, getDriverCleanup(configurer));
  }

  private DriverCleanup getDriverCleanup(Configurer configurer) throws Exception {
    Class<? extends Driver> jdbcDriverClass = configurer.usePluginClass("jdbc", conf.getJdbcPluginName(),
                                                                        conf.getJDBCPluginId() + "." +
                                                                          UUID.randomUUID().toString(),
                                                                        PluginProperties.builder().build());
    if (jdbcDriverClass == null) {
      throw new IllegalArgumentException("JDBC plugin " + conf.getJdbcPluginName() + " not found.");
    }

    return DriverCleanup.ensureJDBCDriverIsAvailable(jdbcDriverClass, conf.getJdbcURL());
  }
}
