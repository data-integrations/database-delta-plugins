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

import io.debezium.connector.sqlserver.SourceInfo;
import io.debezium.util.Strings;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Record offset information for SqlServer.
 */
public class SqlServerOffset {
  static final String SNAPSHOT_TABLES = "snapshot_tables";

  private final Map<String, String> state;

  public SqlServerOffset() {
    this.state = new ConcurrentHashMap<>();
  }

  public SqlServerOffset(Map<String, String> state) {
    this.state = new ConcurrentHashMap<>(state);
  }

  public Map<String, String> get() {
    return state;
  }

  Map<String, String> generateCdapOffsets(SourceRecord sourceRecord) {
    Map<String, ?> sourceOffset = sourceRecord.sourceOffset();
    String changLsn = (String) sourceOffset.get(SourceInfo.CHANGE_LSN_KEY);
    String commitLsn = (String) sourceOffset.get(SourceInfo.COMMIT_LSN_KEY);
    Boolean snapshot = (Boolean) sourceOffset.get(SourceInfo.SNAPSHOT_KEY);
    Boolean snapshotCompleted = (Boolean) sourceOffset.get(SqlServerConstantOffsetBackingStore.SNAPSHOT_COMPLETED);
    String snapshotTables = state.get(SNAPSHOT_TABLES);

    Map<String, String> deltaOffset = new HashMap<>(5);
    if (changLsn != null) {
      deltaOffset.put(SourceInfo.CHANGE_LSN_KEY, changLsn);
    }
    if (commitLsn != null) {
      deltaOffset.put(SourceInfo.COMMIT_LSN_KEY, commitLsn);
    }
    if (snapshot != null) {
      deltaOffset.put(SourceInfo.SNAPSHOT_KEY, String.valueOf(snapshot));
    }
    if (snapshotCompleted != null) {
      deltaOffset.put(SqlServerConstantOffsetBackingStore.SNAPSHOT_COMPLETED, String.valueOf(snapshotCompleted));
    }
    if (!Strings.isNullOrEmpty(snapshotTables)) {
      deltaOffset.put(SNAPSHOT_TABLES, snapshotTables);
    }

    return deltaOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SqlServerOffset that = (SqlServerOffset) o;
    return Objects.equals(state, that.state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(state);
  }
}
