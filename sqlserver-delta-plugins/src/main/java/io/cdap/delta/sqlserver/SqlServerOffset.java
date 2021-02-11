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

import io.cdap.delta.api.Offset;
import io.debezium.connector.sqlserver.Lsn;
import io.debezium.connector.sqlserver.SourceInfo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Record offset information for SqlServer.
 */
public class SqlServerOffset {
  static final String DELIMITER = ",";
  static final String SNAPSHOT_TABLES = "snapshot_tables";

  private final String changeLsn;
  private final String commitLsn;
  private final Boolean isSnapshot;
  private final Boolean isSnapshotCompleted;
  private Set<String> snapshotTables;

  SqlServerOffset(Map<String, ?> properties) {
    this.changeLsn = (String) properties.get(SourceInfo.CHANGE_LSN_KEY);
    this.commitLsn = (String) properties.get(SourceInfo.COMMIT_LSN_KEY);
    this.isSnapshot = (Boolean) properties.get(SourceInfo.SNAPSHOT_KEY);
    this.isSnapshotCompleted = (Boolean) properties.get(SqlServerConstantOffsetBackingStore.SNAPSHOT_COMPLETED);
    this.snapshotTables = new HashSet<>();
  }

  boolean isSnapshot() {
    return isSnapshot;
  }

  void setSnapshotTables(Set<String> snapshotTables) {
    this.snapshotTables = new HashSet<>(snapshotTables);
  }

  void addSnapshotTable(String table) {
    snapshotTables.add(table);
  }

  Offset getAsOffset() {
    Map<String, String> deltaOffset = new HashMap<>();
    if (changeLsn != null) {
      deltaOffset.put(SourceInfo.CHANGE_LSN_KEY, changeLsn);
    }
    if (commitLsn != null) {
      deltaOffset.put(SourceInfo.COMMIT_LSN_KEY, commitLsn);
    }
    if (isSnapshot != null) {
      deltaOffset.put(SourceInfo.SNAPSHOT_KEY, String.valueOf(isSnapshot));
    }
    if (isSnapshotCompleted != null) {
      deltaOffset.put(SqlServerConstantOffsetBackingStore.SNAPSHOT_COMPLETED, String.valueOf(isSnapshotCompleted));
    }
    if (snapshotTables != null && !snapshotTables.isEmpty()) {
      deltaOffset.put(SNAPSHOT_TABLES, String.join(DELIMITER, snapshotTables));
    }

    return new Offset(deltaOffset);
  }

  /**
   * Returns whether the this {@link SqlServerOffset SqlServerOffset} instance is before or at the specified delta
   * offset. If it's true that means this {@link SqlServerOffset SqlServerOffset} was once seen by the SQLServer
   * Debezium connector at the specified delta offset
   * @param deltaOffset the delta offset to compare
   * @return whether the this {@link SqlServerOffset SqlServerOffset} instance is before or at the specified delta
   * offset.
   */
  public boolean isBeforeOrAt(Offset deltaOffset) {
    return Lsn.valueOf(this.changeLsn)
      .compareTo(Lsn.valueOf(deltaOffset.get().get(SourceInfo.CHANGE_LSN_KEY))) < 1;
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
    return Objects.equals(changeLsn, that.changeLsn)
      && Objects.equals(commitLsn, that.commitLsn)
      && Objects.equals(isSnapshot, that.isSnapshot)
      && Objects.equals(isSnapshotCompleted, that.isSnapshotCompleted)
      && Objects.equals(snapshotTables, that.snapshotTables);
  }

  @Override
  public int hashCode() {
    return Objects.hash(changeLsn, commitLsn, isSnapshot, isSnapshotCompleted, snapshotTables);
  }
}
