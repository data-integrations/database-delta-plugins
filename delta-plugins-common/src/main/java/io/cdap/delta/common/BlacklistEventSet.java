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

package io.cdap.delta.common;

import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLOperation;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Defines a set of DML event blacklist and a set of DDL event blacklist.
 */
public class BlacklistEventSet {
  private final Set<DMLOperation> dmlBlacklist;
  private final Set<DDLOperation> ddlBlacklist;

  public BlacklistEventSet(Set<DMLOperation> dmlBlacklist, Set<DDLOperation> ddlBlacklist) {
    this.dmlBlacklist = Collections.unmodifiableSet(new HashSet<>(dmlBlacklist));
    this.ddlBlacklist = Collections.unmodifiableSet(new HashSet<>(ddlBlacklist));
  }

  /**
   * @return set of DML operations that should always be ignored.
   */
  public Set<DMLOperation> getDmlBlacklist() {
    return dmlBlacklist;
  }

  /**
   * @return set of DDL operations that should always be ignored
   */
  public Set<DDLOperation> getDdlBlacklist() {
    return ddlBlacklist;
  }
}
