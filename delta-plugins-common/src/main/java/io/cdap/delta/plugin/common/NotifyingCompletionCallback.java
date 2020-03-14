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

import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.ReplicationError;
import io.debezium.embedded.EmbeddedEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Notifies the app and updates the replication state if the embedded engine fails.
 */
public class NotifyingCompletionCallback implements EmbeddedEngine.CompletionCallback {
  private static final Logger LOG = LoggerFactory.getLogger(NotifyingCompletionCallback.class);
  private final DeltaSourceContext context;

  public NotifyingCompletionCallback(DeltaSourceContext context) {
    this.context = context;
  }

  @Override
  public void handle(boolean success, String message, Throwable error) {
    if (!success) {
      // ignore the message, since it's a generic message unrelated to the cause
      // "Failed to start connector with invalid configuration (see logs for actual errors)".
      try {
        context.setError(new ReplicationError(error));
      } catch (IOException e) {
        LOG.warn("Failed to update in the state store that the source is having issues", e);
      }
      context.notifyFailed(error);
    }
  }
}
