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
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Returns an offset based on a configuration setting and doesn't actually store anything.
 * This is because the Delta app needs to have control over when offsets are stored and not Debezium.
 *
 *
 * OffsetBackingStore is pretty weird... keys are ByteBuffer representations of Strings like:
 *
 * {"schema":null,"payload":["delta",{"server":"dummy"}]}
 *
 * and values are ByteBuffer representations of Strings like:
 *
 * {"scn":16838027}
 */
public class OracleConstantOffsetBackingStore extends MemoryOffsetBackingStore {
  private static final Gson GSON = new Gson();
  private static final String KEY = "{\"schema\":null,\"payload\":[\"delta\",{\"server\":\"dummy\"}]}";

  @Override
  public void configure(WorkerConfig config) {
    // TODO: remove hack once EmbeddedEngine is no longer used

    String offsetStr = config.getString("offset.storage.file.filename");
    if (offsetStr.isEmpty()) {
      return;
    }

    Map<String, Object> offset = new HashMap<>();
    offset.put("scn", Long.parseLong(offsetStr));
    byte[] offsetBytes = Bytes.toBytes(GSON.toJson(offset));

    data.put(ByteBuffer.wrap(Bytes.toBytes(KEY)), ByteBuffer.wrap(offsetBytes));
  }
}
