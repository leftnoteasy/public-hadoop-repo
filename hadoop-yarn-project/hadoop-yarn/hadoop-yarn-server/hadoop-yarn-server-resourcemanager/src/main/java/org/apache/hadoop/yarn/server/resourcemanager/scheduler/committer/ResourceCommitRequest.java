package org.apache.hadoop.yarn.server.resourcemanager.scheduler.committer;

import com.google.common.collect.Lists;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ResourceCommitRequest<A extends SchedulerApplicationAttempt,
    N extends SchedulerNode> {
  // New containers to be allocated
  private List<ContainerAllocationContext<A, N>> containersToAllocate =
      Collections.emptyList();

  // New containers to be released
  private List<ContainerAllocationContext<A, N>> containersToReserve =
      Collections.emptyList();

  // We don't need these containers anymore
  private List<SchedulerContainer<A, N>> toReleaseContainers =
      Collections.emptyList();

  private Resource totalAllocatedResource;
  private Resource totalReservedResource;
  private Resource totalReleasedResource;

  public ResourceCommitRequest(
      List<ContainerAllocationContext<A, N>> containersToAllocate,
      List<ContainerAllocationContext<A, N>> containersToReserve,
      List<SchedulerContainer<A, N>> toReleaseContainers) {
    if (null != containersToAllocate) {
      this.containersToAllocate = containersToAllocate;
    }
    if (null != containersToReserve) {
      this.containersToReserve = containersToReserve;
    }
    if (null != toReleaseContainers) {
      this.toReleaseContainers = toReleaseContainers;
    }

    totalAllocatedResource = Resources.createResource(0);
    totalReservedResource = Resources.createResource(0);
    totalReleasedResource = Resources.createResource(0);

    for (ContainerAllocationContext<A,N> c : this.containersToAllocate) {
      Resources.addTo(totalAllocatedResource,
          c.getAllocatedOrReservedResource());
      for (SchedulerContainer<A,N> r : c.getToRelease()) {
        Resources.addTo(totalReleasedResource,
            r.getRmContainer().getAllocatedOrReservedResource());
      }
    }

    for (ContainerAllocationContext<A,N> c : this.containersToReserve) {
      Resources.addTo(totalReservedResource,
          c.getAllocatedOrReservedResource());
      for (SchedulerContainer<A,N> r : c.getToRelease()) {
        Resources.addTo(totalReleasedResource,
            r.getRmContainer().getAllocatedOrReservedResource());
      }
    }

    for (SchedulerContainer<A,N> r : this.toReleaseContainers) {
      Resources.addTo(totalReleasedResource,
          r.getRmContainer().getAllocatedOrReservedResource());
    }
  }

  public List<ContainerAllocationContext<A, N>> getContainersToAllocate() {
    return containersToAllocate;
  }

  public void setContainersToAllocate(
      List<ContainerAllocationContext<A, N>> containersToAllocate) {
    this.containersToAllocate = containersToAllocate;
  }

  public List<ContainerAllocationContext<A, N>> getContainersToReserve() {
    return containersToReserve;
  }

  public void setContainersToReserve(
      List<ContainerAllocationContext<A, N>> containersToReserve) {
    this.containersToReserve = containersToReserve;
  }

  public List<SchedulerContainer<A, N>> getContainersToRelease() {
    return toReleaseContainers;
  }

  public void setContainersToRelease(
      List<SchedulerContainer<A, N>> toReleaseContainers) {
    this.toReleaseContainers = toReleaseContainers;
  }

  public Resource getTotalAllocatedResource() {
    return totalAllocatedResource;
  }

  public void setTotalAllocatedResource(Resource totalAllocatedResource) {
    this.totalAllocatedResource = totalAllocatedResource;
  }

  public Resource getTotalReservedResource() {
    return totalReservedResource;
  }

  public void setTotalReservedResource(Resource totalReservedResource) {
    this.totalReservedResource = totalReservedResource;
  }

  public Resource getTotalReleasedResource() {
    return totalReleasedResource;
  }

  public void setTotalReleasedResource(Resource totalReleasedResource) {
    this.totalReleasedResource = totalReleasedResource;
  }

  /*
   * Util functions to make your life easier
   */
  public boolean anythingAllocatedOrReserved() {
    return (!containersToAllocate.isEmpty()) || (!containersToReserve
        .isEmpty());
  }

  public ContainerAllocationContext<A, N> getFirstAllocatedOrReservedContainer() {
    ContainerAllocationContext<A, N> c = null;
    if (!containersToAllocate.isEmpty()) {
      c = containersToAllocate.get(0);
    }
    if (c == null && !containersToReserve.isEmpty()) {
      c = containersToReserve.get(0);
    }

    return c;
  }
}
