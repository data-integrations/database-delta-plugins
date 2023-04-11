/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.delta.plugin.common;

import com.google.common.collect.MapMaker;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Cache which stores mapping of Debezium schema to CDAP schema
 * Uses reference equality (==) for keys for performance reasons as Schema objects are immutable
 * Weak references are used  for keys to ensure entries are cleaned up by GC when no longer used by the program
 */
@ThreadSafe
public class SchemaMappingCache {

  private final Map<org.apache.kafka.connect.data.Schema, Schema> cache = new MapMaker()
    .weakKeys().makeMap();


  public void reset() {
    cache.clear();
  }

  public void put(org.apache.kafka.connect.data.Schema key, Schema mappedSchema) {
    cache.put(key, mappedSchema);
  }

  public Schema get(org.apache.kafka.connect.data.Schema key) {
    return cache.get(key);
  }
}
