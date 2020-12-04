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

package io.cdap.delta.plugin.mock;

import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.ReplicationError;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Used to return the jdbc driver class.
 */
public class MockContext implements DeltaSourceContext {
  private final Class<?> driverClass;
  private final Map<String, byte[]> state = new HashMap<>();

  public MockContext(Class<?> driverClass) {
    this.driverClass = driverClass;
  }

  @Override
  public void setError(ReplicationError replicationError) {
    // no-op
  }

  @Override
  public void setOK() {
    // no-op
  }

  @Override
  public String getApplicationName() {
    return "app";
  }

  @Override
  public String getRunId() {
    return "runid";
  }

  @Override
  public Metrics getMetrics() {
    return new Metrics() {
      @Override
      public void count(String metricName, int delta) {
        // no-op
      }

      @Override
      public void gauge(String metricName, long value) {
        // no-op
      }

      @Override
      public Metrics child(Map<String, String> tags) {
        return this;
      }

      @Override
      public Map<String, String> getTags() {
        return Collections.emptyMap();
      }
    };
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return new HashMap<>();
  }

  @Override
  public int getInstanceId() {
    return 0;
  }

  @Override
  public int getMaxRetrySeconds() {
    return 0;
  }

  @Nullable
  @Override
  public byte[] getState(String s) {
    return state.get(s);
  }

  @Override
  public void putState(String s, byte[] bytes) {
    state.put(s, bytes);
  }

  @Override
  public DeltaPipelineId getPipelineId() {
    return new DeltaPipelineId("default", "app", 0L);
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return PluginProperties.builder().build();
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId, MacroEvaluator evaluator) {
    return PluginProperties.builder().build();
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return (Class<T>) driverClass;
  }

  @Override
  public <T> T newPluginInstance(String pluginId) {
    return null;
  }

  @Override
  public <T> T newPluginInstance(String pluginId, MacroEvaluator evaluator) {
    return null;
  }

  @Override
  public void notifyFailed(Throwable throwable) {
    // no-op
  }
}
