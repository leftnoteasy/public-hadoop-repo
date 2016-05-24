package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.scorer;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PlacementSet;

import java.util.Iterator;

public class DoNotCareNodesScorer<N extends SchedulerNode>
    implements SchedulerNodesScorer<N> {
  @Override
  public Iterator<N> scorePlacementSet(
      PlacementSet<N> candidates) {
    return candidates.getAllSchedulableNodes().values().iterator();
  }
}
