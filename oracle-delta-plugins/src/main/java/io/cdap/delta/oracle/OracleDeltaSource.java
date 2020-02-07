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

package io.cdap.delta.oracle;

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

import java.io.IOException;
import java.sql.Driver;
import java.sql.SQLException;

/**
 * For oracle source plugin.
 */
@Plugin(type = DeltaSource.PLUGIN_TYPE)
@Name(OracleDeltaSource.NAME)
@Description("Delta source for Oracle.")
public class OracleDeltaSource implements DeltaSource {
  public static final String NAME = "oracle";
  private final OracleConfig conf;

  public OracleDeltaSource(OracleConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configure(Configurer configurer) {
    // no-op
  }

  @Override
  public EventReader createReader(EventReaderDefinition definition, DeltaSourceContext context,
                                  EventEmitter eventEmitter) {
    return new OracleEventReader(conf, context, eventEmitter, definition);
  }

  @Override
  public TableRegistry createTableRegistry(Configurer configurer) {
    Class<? extends Driver> driverClass = configurer.usePluginClass("jdbc",
                                                                    conf.getJdbcPluginName(),
                                                                    "jdbc",
                                                                    PluginProperties.builder().build());
    if (driverClass == null) {
      throw new RuntimeException("Unable to find jdbc driver plugin : " + conf.getJdbcPluginName());
    }

    try (DriverCleanup driverCleanup =
           DriverCleanup.ensureJDBCDriverIsAvailable(driverClass, conf.getConnectionString())) {
      return new OracleTableRegistry(conf, driverCleanup);
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      throw new RuntimeException("Unable to instantiate jdbc driver plugin: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new RuntimeException("Unable to de-register jdbc driver plugin: " + e.getMessage(), e);
    }
  }

  @Override
  public TableAssessor<TableDetail> createTableAssessor(Configurer configurer) throws Exception {
    // TODO: implement accesssment
    throw new UnsupportedOperationException("createTableAssessor is not implemented yet");
  }
}
