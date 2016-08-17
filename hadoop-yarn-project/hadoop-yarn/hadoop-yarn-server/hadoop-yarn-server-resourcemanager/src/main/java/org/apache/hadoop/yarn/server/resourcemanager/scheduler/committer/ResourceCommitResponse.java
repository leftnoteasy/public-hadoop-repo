package org.apache.hadoop.yarn.server.resourcemanager.scheduler.committer;

import java.util.List;

public class ResourceCommitResponse {
  private List<ResourceCommitRequest> acceptedAllocations;
  private List<ResourceCommitRequest> rejectedAllocations;

  public ResourceCommitResponse(List<ResourceCommitRequest> acceptedAllocations,
      List<ResourceCommitRequest> rejectedAllocations) {
    this.acceptedAllocations = acceptedAllocations;
    this.rejectedAllocations = rejectedAllocations;
  }

  public List<ResourceCommitRequest> getAcceptedAllocations() {
    return acceptedAllocations;
  }

  public void setAcceptedAllocations(
      List<ResourceCommitRequest> acceptedAllocations) {
    this.acceptedAllocations = acceptedAllocations;
  }

  public List<ResourceCommitRequest> getRejectedAllocations() {
    return rejectedAllocations;
  }

  public void setRejectedAllocations(
      List<ResourceCommitRequest> rejectedAllocations) {
    this.rejectedAllocations = rejectedAllocations;
  }
}
