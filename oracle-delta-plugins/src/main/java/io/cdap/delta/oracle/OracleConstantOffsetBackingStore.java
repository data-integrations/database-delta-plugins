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

import com.google.gson.Gson;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.util.Strings;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Returns an offset based on a configuration setting and doesn't actually store anything.
 * This is because the Delta app needs to have control over when offsets are stored and not Debezium.
 *
 * OffsetBackingStore is pretty weird... keys are ByteBuffer representations of Strings like:
 *
 * {"schema":null,"payload":["delta",{"server":"dummy"}]}
 *
 * and values are ByteBuffer representations of Strings like:
 *
 * {"scn":16838027,"lcr_position":"090909090909sfsdfsfsd09","snapshot":true,"snapshot_completed":false}
 */
public class OracleConstantOffsetBackingStore extends MemoryOffsetBackingStore {
  static final String SNAPSHOT_COMPLETED = "snapshot_completed";
  // The key is hardcoded here
  private static final ByteBuffer KEY =
    StandardCharsets.UTF_8.encode("{\"schema\":null,\"payload\":[\"delta\",{\"server\":\"dummy\"}]}");

  @Override
  public void configure(WorkerConfig config) {
    Map<String, String> originalConfig = config.originalsStrings();
    String scnStr = originalConfig.get(SourceInfo.SCN_KEY);
    String lcrPositionStr = originalConfig.get(SourceInfo.LCR_POSITION_KEY);
    String snapshotStr = originalConfig.get(SourceInfo.SNAPSHOT_KEY);
    String snapshotCompletedStr = originalConfig.get(SNAPSHOT_COMPLETED);

    Map<String, Object> offset = new HashMap<>();
    if (!Strings.isNullOrEmpty(scnStr)) {
      offset.put(SourceInfo.SCN_KEY, Long.valueOf(scnStr));
    }
    if (!Strings.isNullOrEmpty(lcrPositionStr)) {
      offset.put(SourceInfo.LCR_POSITION_KEY, lcrPositionStr);
    }
    if (!Strings.isNullOrEmpty(snapshotStr)) {
      offset.put(SourceInfo.SNAPSHOT_KEY, Boolean.valueOf(snapshotStr));
    }
    if (!Strings.isNullOrEmpty(snapshotCompletedStr)) {
      offset.put(SNAPSHOT_COMPLETED, Boolean.valueOf(snapshotCompletedStr));
    }

    if (offset.isEmpty()) {
      return;
    }
    Gson gson = new Gson();
    data.put(KEY, StandardCharsets.UTF_8.encode(gson.toJson(offset)));
  }
}
