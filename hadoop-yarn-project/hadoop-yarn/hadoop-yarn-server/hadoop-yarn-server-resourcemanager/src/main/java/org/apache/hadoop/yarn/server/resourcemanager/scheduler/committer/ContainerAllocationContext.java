package org.apache.hadoop.yarn.server.resourcemanager.scheduler.committer;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;

import java.util.Collections;
import java.util.List;

/**
 * Contexts to allocate a new container
 */
public class ContainerAllocationContext<A extends SchedulerApplicationAttempt,
    N extends SchedulerNode> {
  // Container we allocated or reserved
  private SchedulerContainer<A, N> allocatedOrReservedContainer;

  // Containers we need to release before allocating or reserving the
  // new container
  private List<SchedulerContainer<A, N>> toRelease;

  // When trying to allocate from a reserved container, set this, and this will
  // not be included by toRelease list
  private SchedulerContainer<A, N> allocateFromReservedContainer;

  private boolean isIncreasedAllocation;

  private NodeType nodeType;

  private SchedulingMode schedulingMode;

  private Resource allocatedResource; // newly allocated resource

  public ContainerAllocationContext(
      SchedulerContainer<A, N> allocatedOrReservedContainer,
      List<SchedulerContainer<A, N>> toRelease,
      SchedulerContainer<A, N> allocateFromReservedContainer,
      boolean isIncreasedAllocation, NodeType nodeType,
      SchedulingMode schedulingMode, Resource allocatedResource) {
    this.allocatedOrReservedContainer = allocatedOrReservedContainer;
    if (null != toRelease) {
      this.toRelease = Collections.emptyList();
    }
    this.allocateFromReservedContainer = allocateFromReservedContainer;
    this.isIncreasedAllocation = isIncreasedAllocation;
    this.nodeType = nodeType;
    this.schedulingMode = schedulingMode;
    this.allocatedResource = allocatedResource;
  }

  public SchedulingMode getSchedulingMode() {
    return schedulingMode;
  }

  public void setSchedulingMode(SchedulingMode schedulingMode) {
    this.schedulingMode = schedulingMode;
  }

  public Resource getAllocatedOrReservedResource() {
    return allocatedResource;
  }


  public void setAllocatedResource(Resource allocatedResource) {
    this.allocatedResource = allocatedResource;
  }


  public NodeType getNodeType() {
    return nodeType;
  }

  public void setNodeType(NodeType nodeType) {
    this.nodeType = nodeType;
  }

  public boolean isIncreasedAllocation() {
    return isIncreasedAllocation;
  }

  public void setIncreasedAllocation(boolean increasedAllocation) {
    isIncreasedAllocation = increasedAllocation;
  }

  public SchedulerContainer<A, N> getAllocateFromReservedContainer() {
    return allocateFromReservedContainer;
  }

  public void setAllocateFromReservedContainer(
      SchedulerContainer<A, N> allocateFromReservedContainer) {
    this.allocateFromReservedContainer = allocateFromReservedContainer;
  }

  public SchedulerContainer<A, N> getAllocatedOrReservedContainer() {
    return allocatedOrReservedContainer;
  }

  public void setAllocatedOrReservedContainer(
      SchedulerContainer<A, N> allocatedOrReservedContainer) {
    this.allocatedOrReservedContainer = allocatedOrReservedContainer;
  }

  public List<SchedulerContainer<A, N>> getToRelease() {
    return toRelease;
  }

  public void setToRelease(List<SchedulerContainer<A, N>> toRelease) {
    this.toRelease = toRelease;
  }
}
