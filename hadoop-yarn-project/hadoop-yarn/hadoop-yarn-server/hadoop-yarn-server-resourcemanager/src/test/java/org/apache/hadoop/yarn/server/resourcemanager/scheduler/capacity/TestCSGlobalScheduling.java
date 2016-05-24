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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

// TODO, writing tests here..
public class TestCSGlobalScheduling {
  private final int GB = 1024;

  private YarnConfiguration conf;

  RMNodeLabelsManager mgr;

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }

  @Test
  public void testSimpleGlobalScheduling() throws Exception {
    // inject node label manager
    conf.setBoolean(CapacitySchedulerConfiguration.SCHEDULE_GLOBALLY_ENABLE, true);

    MockRM rm1 = new MockRM(conf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8000); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 8000); // label = y
    MockNM nm3 = rm1.registerNode("h3:1234", 8000); // label = <empty>


    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    RMApp app1 = rm1.submitApp(200, "app", "user", null);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm3);

    am1.allocate(Arrays.asList(ResourceRequest
        .newInstance(Priority.create(0), "*", Resources.createResource(1024),
            3), ResourceRequest
        .newInstance(Priority.create(1), "h1", Resources.createResource(1024),
            3), ResourceRequest
        .newInstance(Priority.create(1), "h2", Resources.createResource(1024),
            1), ResourceRequest.newInstance(Priority.create(1), "/default-rack",
        Resources.createResource(1024), 4), ResourceRequest
        .newInstance(Priority.create(1), "*", Resources.createResource(1024),
            4)), null);


    // request a container.
    // am1.allocate("*", 1024, 1, new ArrayList<ContainerId>());

    Thread.sleep(1000000);


    rm1.close();
  }
}
