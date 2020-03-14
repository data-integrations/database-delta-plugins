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

import com.google.gson.Gson;
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
 * {"schema":null,"payload":["delta",{"server":"dummy"}]}
 *
 * and values will contain following items: 1. file; 2. pos; 3. snapshot.
 */
public class MySqlConstantOffsetBackingStore extends MemoryOffsetBackingStore {
  static final String FILE = "file";
  static final String POS = "pos";
  static final String SNAPSHOT = "snapshot";
  static final String ROW = "row";
  static final String EVENT = "event";
  static final String GTID_SET = "gtids";

  private static final Gson GSON = new Gson();
  // The key is hardcoded here
  private static final ByteBuffer KEY =
    StandardCharsets.UTF_8.encode("{\"schema\":null,\"payload\":[\"delta\",{\"server\":\"dummy\"}]}");

  @Override
  public void configure(WorkerConfig config) {
    Map<String, String> originalConfig = config.originalsStrings();
    String fileStr = originalConfig.get(FILE);
    String posStr = originalConfig.get(POS);
    String snapshotStr = originalConfig.get(SNAPSHOT);
    String rowStr = originalConfig.get(ROW);
    String eventStr = originalConfig.get(EVENT);
    String gtidSetStr = originalConfig.get(GTID_SET);

    Map<String, Object> offset = new HashMap<>();
    if (!Strings.isNullOrEmpty(fileStr)) {
      offset.put(FILE, fileStr);
    }
    if (!Strings.isNullOrEmpty(posStr)) {
      offset.put(POS, Long.valueOf(posStr));
    }
    if (!Strings.isNullOrEmpty(snapshotStr)) {
      offset.put(SNAPSHOT, Boolean.valueOf(snapshotStr));
    }
    if (!Strings.isNullOrEmpty(rowStr)) {
      offset.put(ROW, Long.valueOf(rowStr));
    }
    if (!Strings.isNullOrEmpty(eventStr)) {
      offset.put(EVENT, Long.valueOf(eventStr));
    }
    if (!Strings.isNullOrEmpty(gtidSetStr)) {
      offset.put(GTID_SET, gtidSetStr);
    }

    if (offset.isEmpty()) {
      return;
    }

    data.put(KEY, StandardCharsets.UTF_8.encode(GSON.toJson(offset)));
  }
}
