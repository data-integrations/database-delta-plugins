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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.common.DBSchemaHistory;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.embedded.EmbeddedEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Event reader for oracle.
 */
public class OracleEventReader implements EventReader {
  private static final Logger LOG = LoggerFactory.getLogger(OracleEventReader.class);
  private final OracleConfig config;
  private final DeltaSourceContext context;
  private final ExecutorService executorService;
  private final EventEmitter emitter;
  private final EventReaderDefinition definition;
  private EmbeddedEngine engine;

  public OracleEventReader(OracleConfig config, DeltaSourceContext context,
                           EventEmitter emitter, EventReaderDefinition definition) {
    this.config = config;
    this.context = context;
    this.emitter = emitter;
    this.definition = definition;
    this.executorService = Executors.newSingleThreadExecutor();
  }

  @Override
  public void start(Offset offset) {

    // load native jdbc libraries into jvm during runtime
    try {
      System.setProperty("java.library.path", config.getLibPath());
      Field fieldSysPath = ClassLoader.class.getDeclaredField("sys_paths");
      fieldSysPath.setAccessible(true);
      fieldSysPath.set(null, null);
    } catch (Exception e) {
      throw new RuntimeException("Unable to load jdbc native libraries.");
    }

    // This is a hacky way to load xstream jar. What we do here is using URLClassLoader reflection to invoke
    // protected 'addURL(URL url)' method to bind xstream jar into our current class loader which is a
    // PluginClassLoader. The reason why this is hacky is that Java 9+ will warn that
    // URLClassLoader.class.getDeclaredMethod("addURL", URL.class) is an illegal use of reflection.
    // TODO: CDAP-16311 Have a proper way to register user jars into CDAP
    ClassLoader classLoader = getClass().getClassLoader();
    try {
      String jarPath = config.getLibPath() + "/xstreams.jar";
      File file = new File(jarPath);
      URL url = file.toURI().toURL();
      Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
      method.setAccessible(true);
      method.invoke(classLoader, url);
    } catch (Exception e) {
      throw new RuntimeException("Unable to load xstream.jar to class loader.");
    }

    Map<String, SourceTable> sourceTableMap = definition.getTables().stream().collect(
      Collectors.toMap(
        t -> {
          String schema = t.getSchema();
          String table = t.getTable();
          return schema == null ? table : schema + "." + table;
          }, t -> t));

    Configuration.Builder builder = Configuration.create()
      .with("connector.class", OracleConnector.class.getName())
      .with("offset.storage", OracleConstantOffsetBackingStore.class.getName())
      .with("offset.flush.interval.ms", 1000);

    // bind offset configs with debezium config
    convertOffsets(offset.get()).forEach(builder::with);

    Configuration debeziumConf = builder
      /* begin connector properties */
      .with("name", "delta")
      .with("database.hostname", config.getHost())
      .with("database.port", config.getPort())
      .with("database.user", config.getUser())
      .with("database.password", config.getPassword())
      .with("database.dbname", config.getDbName())
      .with("database.pdb.name", config.getPdbName())
      .with("database.out.server.name", config.getOutServerName())
      .with("database.history", DBSchemaHistory.class.getName())
      // below is a workaround fix for ORA-21560 issue, Jira to track: https://issues.cask.co/browse/PLUGIN-105
      .with("database.oracle.version", 11)
      .with("database.server.name", "dummy") // this is the kafka topic for hosted debezium - it doesn't matter
      .with("table.whitelist", String.join(",", sourceTableMap.keySet()))
      .build();

    DBSchemaHistory.deltaRuntimeContext = context;
    ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classLoader);

    try {
      // Create the engine with this configuration ...
      engine = EmbeddedEngine.create()
        .using(debeziumConf)
        .notifying(new OracleSourceRecordConsumer(config.getDbName(), emitter, sourceTableMap))
        .using((success, message, error) -> {
          if (!success) {
            LOG.error("Failed - {}", message, error);
          }
        })
        .build();

      executorService.submit(engine);
    } finally {
      Thread.currentThread().setContextClassLoader(oldCL);
    }
  }

  @Override
  public void stop() throws InterruptedException {
    if (engine != null && engine.stop()) {
      engine.await(1, TimeUnit.MINUTES);
    }
    executorService.shutdown();
  }

  // This method is used for converting a CDAP offset into a debezium offset
  private Map<String, String> convertOffsets(Map<String, byte[]> offsets) {
    Map<String, String> offsetConfigMap = new HashMap<>();
    String scn = Bytes.toString(offsets.get(SourceInfo.SCN_KEY));
    String lcrPosition = Bytes.toString(offsets.get(SourceInfo.LCR_POSITION_KEY));
    String snapshot = Bytes.toString(offsets.get(SourceInfo.SNAPSHOT_KEY));
    String snapshotCompleted = Bytes.toString(offsets.get(OracleConstantOffsetBackingStore.SNAPSHOT_COMPLETED));

    offsetConfigMap.put(SourceInfo.SCN_KEY, scn == null ? "" : scn);
    offsetConfigMap.put(SourceInfo.LCR_POSITION_KEY, lcrPosition == null ? "" : lcrPosition);
    offsetConfigMap.put(SourceInfo.SNAPSHOT_KEY, snapshot == null ? "" : snapshot);
    offsetConfigMap.put(OracleConstantOffsetBackingStore.SNAPSHOT_COMPLETED,
                        snapshotCompleted == null ? "" : snapshotCompleted);

    return offsetConfigMap;
  }
}
