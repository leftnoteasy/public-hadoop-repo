package org.apache.hadoop.yarn.server.resourcemanager.scheduler.committer;

import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

import java.util.List;

public interface ResourceCommitter {
  ResourceCommitResponse commitResource(
      List<RMContainer> allocationCandidates);
}
