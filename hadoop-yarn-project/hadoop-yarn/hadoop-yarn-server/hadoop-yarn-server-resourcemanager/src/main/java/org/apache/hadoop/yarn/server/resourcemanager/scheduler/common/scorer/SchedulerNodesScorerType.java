package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.scorer;

public enum SchedulerNodesScorerType {
  ANY, // Any nodes is fine
  LOCALITY, // Locality-based
}
