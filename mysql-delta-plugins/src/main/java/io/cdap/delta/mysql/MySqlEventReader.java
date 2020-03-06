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

import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.common.DBSchemaHistory;
import io.cdap.delta.common.NotifyingCompletionCallback;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Reads events from MySQL
 */
public class MySqlEventReader implements EventReader {
  public static final Logger LOG = LoggerFactory.getLogger(MySqlEventReader.class);
  private final MySqlConfig config;
  private final EventEmitter emitter;
  private final ExecutorService executorService;
  private final DeltaSourceContext context;
  private final Set<SourceTable> sourceTables;
  private EmbeddedEngine engine;

  public MySqlEventReader(Set<SourceTable> sourceTables, MySqlConfig config,
                          DeltaSourceContext context, EventEmitter emitter) {
    this.sourceTables = sourceTables;
    this.config = config;
    this.context = context;
    this.emitter = emitter;
    this.executorService = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void start(Offset offset) {
    // For MySQL, the unique table identifier in debezium is 'databaseName.tableName'
    Map<String, SourceTable> sourceTableMap = sourceTables.stream().collect(
      Collectors.toMap(t -> config.getDatabase() + "." + t.getTable(), t -> t));

    /*
        OffsetBackingStore is pretty weird... keys are ByteBuffer representations of Strings like:

        {"schema":null,"payload":["mysql-connector",{"server":"dummy"}]}

        and keys are ByteBuffer representations of Strings like:

        {"file":"mysql-bin.000003","pos":16838027}
     */

    String fileStr = offset.get().getOrDefault("file", "");
    String pos = offset.get().getOrDefault("pos", "");
    // have to pass config to the offset storage implementation through 'offset.storage.file.filename'
    // since embedded engine only passes a hardcoded set of config properties to the offset store.
    Configuration debeziumConf = Configuration.create()
      .with("connector.class", MySqlConnector.class.getName())
      .with("offset.storage", MySqlConstantOffsetBackingStore.class.getName())
      .with("offset.storage.file.filename", pos + "|" + fileStr)
      .with("offset.flush.interval.ms", 1000)
      /* begin connector properties */
      .with("name", "delta")
      .with("database.hostname", config.getHost())
      .with("database.port", config.getPort())
      .with("database.user", config.getUser())
      .with("database.password", config.getPassword())
      .with("server.id", config.getConsumerID())
      .with("database.history", DBSchemaHistory.class.getName())
      .with("database.whitelist", config.getDatabase())
      .with("database.server.name", "dummy") // this is the kafka topic for hosted debezium - it doesn't matter
      .with("database.serverTimezone", config.getServerTimezone())
      .with("table.whitelist", String.join(",", sourceTableMap.keySet()))
      .build();
    MySqlConnectorConfig mysqlConf = new MySqlConnectorConfig(debeziumConf);
    DBSchemaHistory.deltaRuntimeContext = context;

    MySqlValueConverters mySqlValueConverters = getValueConverters(mysqlConf);
    DdlParser ddlParser = mysqlConf.getDdlParsingMode().getNewParserInstance(mySqlValueConverters, tableId -> true);

    ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      // Create the engine with this configuration ...
      engine = EmbeddedEngine.create()
        .using(debeziumConf)
        .notifying(new MySqlRecordConsumer(context, emitter, ddlParser, mySqlValueConverters, new Tables()))
        .using(new NotifyingCompletionCallback(context))
        .build();
      executorService.submit(engine);
    } finally {
      Thread.currentThread().setContextClassLoader(oldCL);
    }
  }

  public void stop() throws InterruptedException {
    if (engine != null && engine.stop()) {
      engine.await(1, TimeUnit.MINUTES);
    }
    executorService.shutdown();
  }

  private static MySqlValueConverters getValueConverters(MySqlConnectorConfig configuration) {
    // Use MySQL-specific converters and schemas for values ...

    String timePrecisionModeStr = configuration.getConfig().getString(MySqlConnectorConfig.TIME_PRECISION_MODE);
    TemporalPrecisionMode timePrecisionMode = TemporalPrecisionMode.parse(timePrecisionModeStr);

    JdbcValueConverters.DecimalMode decimalMode = configuration.getDecimalMode();

    String bigIntUnsignedHandlingModeStr = configuration.getConfig()
      .getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
    MySqlConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode =
      MySqlConnectorConfig.BigIntUnsignedHandlingMode.parse(bigIntUnsignedHandlingModeStr);
    JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode = bigIntUnsignedHandlingMode.asBigIntUnsignedMode();

    boolean timeAdjusterEnabled = configuration.getConfig().getBoolean(MySqlConnectorConfig.ENABLE_TIME_ADJUSTER);
    return new MySqlValueConverters(decimalMode, timePrecisionMode, bigIntUnsignedMode,
                                    timeAdjusterEnabled ? MySqlEventReader::adjustTemporal : x -> x);
  }

  private static Temporal adjustTemporal(Temporal temporal) {
    if (temporal.isSupported(ChronoField.YEAR)) {
      int year = temporal.get(ChronoField.YEAR);
      if (0 <= year && year <= 69) {
        temporal = temporal.plus(2000, ChronoUnit.YEARS);
      } else if (70 <= year && year <= 99) {
        temporal = temporal.plus(1900, ChronoUnit.YEARS);
      }
    }
    return temporal;
  }
}
