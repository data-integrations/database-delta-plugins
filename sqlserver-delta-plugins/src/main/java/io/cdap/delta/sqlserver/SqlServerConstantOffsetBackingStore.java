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

import com.google.gson.Gson;
import io.cdap.cdap.api.common.Bytes;
import io.debezium.connector.sqlserver.SourceInfo;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * The offset store class for sql server. Sql server expects the offset which contains:
 * 1. change_lsn, 2. commit_lsn, 3. snapshot, 4. snapshot_completed. 5. snapshot_tables
 */
public class SqlServerConstantOffsetBackingStore extends MemoryOffsetBackingStore {
  private static final Gson GSON = new Gson();
  private static final String KEY = "{\"schema\":null,\"payload\":[\"delta\",{\"server\":\"dummy\"}]}";
  static final String SNAPSHOT_COMPLETED = "snapshot_completed";

  @Override
  public void configure(WorkerConfig config) {
    // the custom config will be in original configs, config.getString() will throw ConfigException saying this is
    // an unknown config
    Map<String, String> originalConfig = config.originalsStrings();
    String changeStr = originalConfig.get(SourceInfo.CHANGE_LSN_KEY);
    String commitStr = originalConfig.get(SourceInfo.COMMIT_LSN_KEY);
    String snapshot = originalConfig.get(SourceInfo.SNAPSHOT_KEY);
    String snapshotCompleted = originalConfig.get(SNAPSHOT_COMPLETED);

    Map<String, Object> offset = new HashMap<>();
    if (!changeStr.isEmpty()) {
      offset.put(SourceInfo.CHANGE_LSN_KEY, changeStr);
    }
    if (!commitStr.isEmpty()) {
      offset.put(SourceInfo.COMMIT_LSN_KEY, commitStr);
    }
    if (!snapshot.isEmpty()) {
      offset.put(SourceInfo.SNAPSHOT_KEY, Boolean.valueOf(snapshot));
    }
    if (!snapshotCompleted.isEmpty()) {
      offset.put(SNAPSHOT_COMPLETED, Boolean.valueOf(snapshotCompleted));
    }

    // if this is missing and we add an empty map, for some reason, the connector will still consider there is some
    // value about the offset, and thus skip reading some values
    if (offset.isEmpty()) {
      return;
    }
    byte[] offsetBytes = Bytes.toBytes(GSON.toJson(offset));

    data.put(ByteBuffer.wrap(Bytes.toBytes(KEY)), ByteBuffer.wrap(offsetBytes));
  }
}
