package com.linkedin.pinot.minion.executor;

public class SegmentMergeRollupTaskExecutorFactory implements PinotTaskExecutorFactory {
  @Override
  public PinotTaskExecutor create() {
    return new SegmentMergeRollupTaskExecutor();
  }
}
