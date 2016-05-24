package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.scorer;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerRequestKey;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractSchedulerNodesScorer<N extends SchedulerNode>
    implements SchedulerNodesScorer<N> {
  SchedulerApplicationAttempt attempt;
  SchedulerRequestKey schedulerKey;
  ReentrantReadWriteLock.ReadLock readLock;
  ReentrantReadWriteLock.WriteLock writeLock;

  AbstractSchedulerNodesScorer(SchedulerApplicationAttempt attempt,
      SchedulerRequestKey schedulerKey) {
    this.attempt = attempt;
    this.schedulerKey = schedulerKey;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }
}
