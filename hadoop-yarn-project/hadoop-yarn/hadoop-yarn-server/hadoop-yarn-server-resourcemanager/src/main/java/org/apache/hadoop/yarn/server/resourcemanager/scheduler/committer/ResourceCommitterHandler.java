package org.apache.hadoop.yarn.server.resourcemanager.scheduler.committer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ResourceCommitterHandler<A extends SchedulerApplicationAttempt, N extends SchedulerNode>
    extends AbstractService {
  private CapacityScheduler cs;
  private Thread queueThread;
  private final BlockingQueue<ResourceCommitRequest<A, N>> blockingQueue =
      new LinkedBlockingQueue<>();
  private boolean asynchronizedHandler = false;

  private class HandlerRunnable implements Runnable {

    @Override
    public void run() {
      while (true) {
        ResourceCommitRequest<A, N> request = null;
        try {
          request = blockingQueue.take();
          cs.processResourceCommitRequest(request);
        } catch (InterruptedException e) {
          System.out.println("Interrupted");
        }
      }
    }
  }

  // TODO: should not hard code using CS here
  public ResourceCommitterHandler(CapacityScheduler cs) {
    super(ResourceCommitterHandler.class.getName());
    this.cs = cs;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    if (asynchronizedHandler) {
      queueThread = new Thread(new HandlerRunnable());
      queueThread.start();
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (asynchronizedHandler) {
      queueThread.interrupt();
    }
    super.serviceStop();
  }

  public void handle(ResourceCommitRequest<A, N> request) {
    if (asynchronizedHandler) {
      blockingQueue.offer(request);
    } else {
      cs.processResourceCommitRequest(request);
    }
  }
}
