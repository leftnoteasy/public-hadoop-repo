package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.scorer;

import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerRequestKey;

import java.util.Map;

/**
 * Do necessary caching for scorer according to type and applications
 */
public class SchedulerNodesScorerCache {
  // At most store 10K objects
  private static LRUMap lruCache = new LRUMap(1024 * 10);

  private static SchedulerNodesScorerType getSchedulerNodesScorerType(
      SchedulerApplicationAttempt attempt, SchedulerRequestKey schedulerKey) {
    Map<String, ResourceRequest> requests = attempt.getResourceRequests(
        schedulerKey);

    // Simplest rule to determine with nodes scorer will be used:
    // When requested #resourceName > 0, use locality, otherwise use DO_NOT_CARE
    if (requests != null && requests.size() > 1) {
      return SchedulerNodesScorerType.LOCALITY;
    }

    return SchedulerNodesScorerType.ANY;
  }

  public static <N extends SchedulerNode> SchedulerNodesScorer<N> getOrCreateScorer(
      SchedulerApplicationAttempt attempt, SchedulerRequestKey schedulerKey) {
    SchedulerNodesScorerType type = getSchedulerNodesScorerType(attempt,
        schedulerKey);

    return getOrCreateScorer(attempt, schedulerKey, type);
  }

  public static <N extends SchedulerNode> SchedulerNodesScorer<N> getOrCreateScorer(
      SchedulerApplicationAttempt attempt, SchedulerRequestKey schedulerKey,
      SchedulerNodesScorerType type) {
    String key = attempt.getApplicationAttemptId().toString() + schedulerKey
        .getPriority().toString();
    SchedulerNodesScorer<N> scorer;
    // scorer = (SchedulerNodesScorer<N>) lruCache.get(key);
    // FIXME: need to correctly compare if we need to update
    scorer = null;

    if (null == scorer) {
      // FIXME, for simple, create scorer every time. We can cache scorer
      // without any issue
      switch (type) {
      case LOCALITY:
        scorer = new LocalityNodesScorer<>(attempt, schedulerKey);
        break;
      case ANY:
        scorer = new DoNotCareNodesScorer<>();
        break;
      default:
        return null;
      }

      lruCache.put(key, scorer);
    }

    return scorer;
  }
}
