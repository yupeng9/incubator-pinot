package com.linkedin.thirdeye.taskexecution.dataflow;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;

public interface Port<T> {
  /**
   * Returns the identifier of the node to which this port belongs to.
   *
   * @return the identifier of the node to which this port belongs to.
   */
  NodeIdentifier getNodeIdentifier();

  // Used by executor
  void initialize();
}
