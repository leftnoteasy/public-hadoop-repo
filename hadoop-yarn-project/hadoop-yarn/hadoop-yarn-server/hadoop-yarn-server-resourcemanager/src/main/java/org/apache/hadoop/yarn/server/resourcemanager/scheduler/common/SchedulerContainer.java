package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;

/**
 * Contexts for a container inside scheduler
 */
public class SchedulerContainer<A extends SchedulerApplicationAttempt,
    N extends SchedulerNode> {
  private RMContainer rmContainer;
  private String nodePartition;
  private A schedulerApplicationAttempt;
  private N schedulerNode;
  private boolean allocated; // Allocated (True) or reserved (False)

  public SchedulerContainer(A app, N node, RMContainer rmContainer,
      String nodePartition, boolean allocated) {
    this.schedulerApplicationAttempt = app;
    this.schedulerNode = node;
    this.rmContainer = rmContainer;
    this.nodePartition = nodePartition;

    RMContainerState nowState = rmContainer.getState();
    if (nowState == RMContainerState.NEW) {
      this.allocated = allocated;
    } else {
      this.allocated = nowState != RMContainerState.RESERVED;
    }
  }

  public String getNodePartition() {
    return nodePartition;
  }

  public void setNodePartition(String nodePartition) {
    this.nodePartition = nodePartition;
  }

  public RMContainer getRmContainer() {
    return rmContainer;
  }

  public void setRmContainer(RMContainer rmContainer) {
    this.rmContainer = rmContainer;
  }

  public A getSchedulerApplicationAttempt() {
    return schedulerApplicationAttempt;
  }

  public void setSchedulerApplicationAttempt(A schedulerApplicationAttempt) {
    this.schedulerApplicationAttempt = schedulerApplicationAttempt;
  }

  public N getSchedulerNode() {
    return schedulerNode;
  }

  public void setSchedulerNode(N schedulerNode) {
    this.schedulerNode = schedulerNode;
  }

  public boolean isAllocated() {
    return allocated;
  }

  public void setAllocated(boolean allocated) {
    this.allocated = allocated;
  }

  public SchedulerRequestKey getSchedulerRequestKey() {
    if (rmContainer.getState() == RMContainerState.RESERVED) {
      return rmContainer.getReservedSchedulerKey();
    }
    return rmContainer.getAllocatedSchedulerKey();
  }
}
