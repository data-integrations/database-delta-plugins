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
import io.cdap.cdap.api.common.Bytes;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.util.Strings;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import java.nio.ByteBuffer;
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
  private static final Gson GSON = new Gson();
  private static final String SOURCE = "source";
  private static final String KEY = "{\"schema\":null,\"payload\":[\"delta\",{\"server\":\"dummy\"}]}";

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

    byte[] offsetBytes = Bytes.toBytes(GSON.toJson(offset));
    data.put(ByteBuffer.wrap(Bytes.toBytes(KEY)), ByteBuffer.wrap(offsetBytes));
  }

  static Map<String, String> deserializeOffsets(Map<String, byte[]> offsets) {
    Map<String, String> offsetConfigMap = new HashMap<>();
    String scn = Bytes.toString(offsets.get(SourceInfo.SCN_KEY));
    String lcrPosition = Bytes.toString(offsets.get(SourceInfo.LCR_POSITION_KEY));
    String snapshot = Bytes.toString(offsets.get(SourceInfo.SNAPSHOT_KEY));
    String snapshotCompleted = Bytes.toString(offsets.get(SNAPSHOT_COMPLETED));

    offsetConfigMap.put(SourceInfo.SCN_KEY, scn == null ? "" : scn);
    offsetConfigMap.put(SourceInfo.LCR_POSITION_KEY, lcrPosition == null ? "" : lcrPosition);
    offsetConfigMap.put(SourceInfo.SNAPSHOT_KEY, snapshot == null ? "" : snapshot);
    offsetConfigMap.put(SNAPSHOT_COMPLETED, snapshotCompleted == null ? "" : snapshotCompleted);

    return offsetConfigMap;
  }

  static Map<String, byte[]> serializeOffsets(SourceRecord sourceRecord) {
    Map<String, ?> sourceOffset = sourceRecord.sourceOffset();
    Struct source = (Struct) ((Struct) sourceRecord.value()).get(SOURCE);
    Boolean snapshotCompleted = (Boolean) sourceOffset.get(SNAPSHOT_COMPLETED);
    Long scn = (Long) source.get(SourceInfo.SCN_KEY);
    String lcrPosition = (String) source.get(SourceInfo.LCR_POSITION_KEY);
    Boolean snapshot = (Boolean) source.get(SourceInfo.SNAPSHOT_KEY);;

    Map<String, byte[]> deltaOffset = new HashMap<>();

    if (scn != null) {
      deltaOffset.put(SourceInfo.SCN_KEY, Bytes.toBytes(scn.toString()));
    }
    if (lcrPosition != null) {
      deltaOffset.put(SourceInfo.LCR_POSITION_KEY, Bytes.toBytes(lcrPosition));
    }
    if (snapshot != null) {
      deltaOffset.put(SourceInfo.SNAPSHOT_KEY, Bytes.toBytes(snapshot.toString()));
    }
    if (snapshotCompleted != null) {
      deltaOffset.put(SNAPSHOT_COMPLETED, Bytes.toBytes(snapshotCompleted.toString()));
    }

    return deltaOffset;
  }
}
