/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PlacementSet<N extends SchedulerNode> {
  private N nextAvailable;
  private Map<NodeId, N> allSchedulableNodes;
  private String partition;

  public PlacementSet(N nextAvailable, Map<NodeId, N> allSchedulable,
      String partition) {
    this.nextAvailable = nextAvailable;
    this.allSchedulableNodes = allSchedulable;
    this.partition = partition;
  }

  /*
   * "I don't care, just give me next node to allocate"
   */
  public N getNextAvailable() {
    return nextAvailable;
  }

  /*
   * "I'm picky, give me all you have and I will decide"
   */
  public Map<NodeId, N> getAllSchedulableNodes() {
    return allSchedulableNodes;
  }

  public String getPartition() {
    return partition;
  }
}
