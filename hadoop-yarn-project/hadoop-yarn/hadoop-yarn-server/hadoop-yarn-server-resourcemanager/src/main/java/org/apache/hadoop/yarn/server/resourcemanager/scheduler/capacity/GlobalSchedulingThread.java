package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PlacementSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalSchedulingThread extends Thread {
  private static final Log LOG = LogFactory.getLog(CapacityScheduler.class);

  // Schedulable nodes could exclude nodes which don't have available resource,
  // Or reserved nodes
  private Map<NodeId, FiCaSchedulerNode> schedulableNodes;
  private Map<NodeId, FiCaSchedulerNode> reservedNodes;
  private Map<NodeId, FiCaSchedulerNode> exhaustedNodes;

  private long lastSyncNodeListVersion = -1;
  private long lastRefreshNodeTS = -1;

  boolean scheduleOnReservedNodes = false;
  int numContinuousNonReservedNodeScheduled = 0;

  private final CapacityScheduler cs;

  public GlobalSchedulingThread(CapacityScheduler cs) {
    setName("=============== Global Scheduling ====================");
    this.cs = cs;
    setDaemon(true);

    schedulableNodes = new ConcurrentHashMap<>();
    reservedNodes = new ConcurrentHashMap<>();
    exhaustedNodes = new ConcurrentHashMap<>();
  }

  @Override
  public void run() {
    // Do we need to refresh nodes in next run?

    while (true) {
      java.util.concurrent.locks.Lock readLock =
          cs.getNodeTracker().getNodeListReadLock();
      try {
        // Lock the node list prevent modifying while doing scheduling
        readLock.lock();

        // Refresh and sync from scheduler when necessary
        refreshNodesWhenNecessary();

        // Do scheduling on cached nodes
        if (!schedulableNodes.isEmpty() || !reservedNodes.isEmpty()) {
          schedule();
        }
      } finally {
        readLock.unlock();
      }

      sleep();
    }
  }

  private void sleep() {
    int sleepTime = 100;
    if (cs.getNumClusterNodes() > 0) {
      sleepTime = 1000 / cs.getNumClusterNodes();
      sleepTime = Math.max(sleepTime, 1);
    }
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
    }
  }

  private void refreshNodesWhenNecessary() {
    long now = System.currentTimeMillis();
    if (lastSyncNodeListVersion != cs.getNodeTracker()
        .getNodeListVersion() || now - lastRefreshNodeTS >= 3000) {
      // Force refresh nodes
      schedulableNodes.clear();
      exhaustedNodes.clear();
      reservedNodes.clear();

      for (FiCaSchedulerNode node : cs.getAllNodes()) {
        if (Resources.lessThanOrEqual(cs.getResourceCalculator(),
            cs.getClusterResource(), node.getUnallocatedResource(),
            Resources.none())) {
          // Exhausted nodes
          exhaustedNodes.put(node.getNodeID(), node);
        } else if (node.getReservedContainer() != null) {
          // Reserved nodes
          reservedNodes.put(node.getNodeID(), node);
        } else {
          // Schedulable nodes (has available resource and not reserved)
          schedulableNodes.put(node.getNodeID(), node);
        }
        schedulableNodes.put(node.getNodeID(), node);
      }

      LOG.info(
          "refresh node, now schedulable nodes = " + schedulableNodes.size());

      lastSyncNodeListVersion = cs.getNodeTracker().getNodeListVersion();
      lastRefreshNodeTS = System.currentTimeMillis();
    }
  }

  private void schedule() {
    if (scheduleOnReservedNodes) {
      for (FiCaSchedulerNode node : reservedNodes.values()) {
        if (node.getReservedContainer() != null) {
          cs.allocateOnReservedNode(node);
        }
      }
      scheduleOnReservedNodes = false;
    } else if (!schedulableNodes.isEmpty()) {
      CSAssignment assignment = cs.getRootQueue().assignContainers(
          cs.getClusterResource(),
          new PlacementSet<>(null, schedulableNodes,
              RMNodeLabelsManager.NO_LABEL), new ResourceLimits(
              cs.getNodeLabelsManager()
                  .getResourceByLabel(RMNodeLabelsManager.NO_LABEL,
                      cs.getClusterResource())),
          SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
      assignment.setSchedulingMode(SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
      cs.submitResourceCommitRequest(cs.getClusterResource(), assignment);

      for (String partition : cs.getRMContext().getNodeLabelManager()
          .getClusterNodeLabelNames()) {
        assignment = cs.getRootQueue().assignContainers(cs.getClusterResource(),
            new PlacementSet<>(null, schedulableNodes, partition),
            new ResourceLimits(cs.getNodeLabelsManager()
                .getResourceByLabel(partition, cs.getClusterResource())),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
        assignment.setSchedulingMode(SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
        cs.submitResourceCommitRequest(cs.getClusterResource(), assignment);
      }

      // Set if do reserved allocation in next round
      numContinuousNonReservedNodeScheduled++;

      if (numContinuousNonReservedNodeScheduled >= schedulableNodes.size()) {
        scheduleOnReservedNodes = true;
        numContinuousNonReservedNodeScheduled = 0;
      }
    }
  }
}