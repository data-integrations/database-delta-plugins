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

package io.cdap.delta.plugin.common;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for manipulating runtime arguments.
 */
public final class RuntimeArguments {
  private RuntimeArguments() {
  }

  /**
   * Identifies arguments with a given prefix, removes them from the arguments and adds new arguments back without
   * the prefix. Arguments which do not start with the given prefix will be ignored.
   *
   * @param prefix prefix to look for in arguments
   * @param arguments the runtime arguments
   * @return a new map that contains the arguments without prefix
   */
  public static Map<String, String> extractPrefixed(String prefix, Map<String, String> arguments) {
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, String> entry : arguments.entrySet()) {
      if (entry.getKey().startsWith(prefix)) {
        result.put(entry.getKey().substring(prefix.length()), entry.getValue());
      }
    }
    return result;
  }
}
