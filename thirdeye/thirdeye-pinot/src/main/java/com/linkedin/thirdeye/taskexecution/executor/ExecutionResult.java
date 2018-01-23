package com.linkedin.thirdeye.taskexecution.executor;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;

public class ExecutionResult {
  private NodeIdentifier nodeIdentifier;
  private ExecutionStatus executionStatus;
  private String message = "";

  public NodeIdentifier getNodeIdentifier() {
    return nodeIdentifier;
  }

  public void setNodeIdentifier(NodeIdentifier nodeIdentifier) {
    this.nodeIdentifier = Preconditions.checkNotNull(nodeIdentifier);
  }

  public ExecutionStatus getExecutionStatus() {
    return executionStatus;
  }

  public void setExecutionStatus(ExecutionStatus executionStatus) {
    this.executionStatus = Preconditions.checkNotNull(executionStatus);
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = Preconditions.checkNotNull(message).trim();
  }
}
