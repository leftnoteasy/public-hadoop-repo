package org.apache.hadoop.yarn.server.resourcemanager.scheduler.committer;

import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

import java.util.List;

public class ResourceCommitRequest {
  List<RMContainer> containersToAllocate;
  List<RMContainer> containersToReserve;
  List<RMContainer> containersToRelease;
}
