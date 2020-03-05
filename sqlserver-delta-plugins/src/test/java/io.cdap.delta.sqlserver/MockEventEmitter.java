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

import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.EventEmitter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Used to keep track of events emitted for tests.
 */
class MockEventEmitter implements EventEmitter {
  private final List<DDLEvent> ddlEvents;
  private final List<DMLEvent> dmlEvents;
  private final CountDownLatch latch;

  public MockEventEmitter(int numExpectedEvents) {
    this.ddlEvents = new ArrayList<>();
    this.dmlEvents = new ArrayList<>();
    this.latch = new CountDownLatch(numExpectedEvents);
  }

  @Override
  public void emit(DDLEvent ddlEvent) {
    ddlEvents.add(ddlEvent);
    latch.countDown();
  }

  @Override
  public void emit(DMLEvent dmlEvent) {
    dmlEvents.add(dmlEvent);
    latch.countDown();
  }

  public List<DDLEvent> getDdlEvents() {
    return ddlEvents;
  }

  public List<DMLEvent> getDmlEvents() {
    return dmlEvents;
  }

  public void waitForExpectedEvents(long dur, TimeUnit unit) throws InterruptedException {
    latch.await(dur, unit);
  }
}
