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

import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.EventEmitter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * EventEmitter for tests that can block with emitting events.
 */
public class BlockingEventEmitter implements EventEmitter {
  private final BlockingQueue<DDLEvent> ddlQueue;
  private final BlockingQueue<DMLEvent> dmlQueue;

  public BlockingEventEmitter(BlockingQueue<DDLEvent> ddlQueue, BlockingQueue<DMLEvent> dmlQueue) {
    this.ddlQueue = ddlQueue;
    this.dmlQueue = dmlQueue;
  }

  @Override
  public void emit(DDLEvent ddlEvent) throws InterruptedException {
    ddlQueue.put(ddlEvent);
  }

  @Override
  public void emit(DMLEvent dmlEvent) throws InterruptedException {
    dmlQueue.put(dmlEvent);
  }
}
