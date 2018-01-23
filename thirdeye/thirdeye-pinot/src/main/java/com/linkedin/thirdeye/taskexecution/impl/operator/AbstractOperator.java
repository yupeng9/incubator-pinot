package com.linkedin.thirdeye.taskexecution.impl.operator;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.operator.Operator;
import com.linkedin.thirdeye.taskexecution.operator.OperatorConfig;
import org.apache.commons.configuration.Configuration;


public abstract class AbstractOperator implements Operator {
  private NodeIdentifier nodeIdentifier;
  protected Configuration configuration;

  public AbstractOperator(NodeIdentifier nodeIdentifier, Configuration configuration) {
    setNodeIdentifier(nodeIdentifier);
    setConfiguration(configuration);
  }

  public final void setNodeIdentifier(NodeIdentifier nodeIdentifier) {
    this.nodeIdentifier = Preconditions.checkNotNull(nodeIdentifier);
  }

  public void setConfiguration(Configuration configuration) {
    this.configuration = Preconditions.checkNotNull(configuration);
  }

  @Override
  final public NodeIdentifier getNodeIdentifier() {
    return nodeIdentifier;
  }

  @Override
  public OperatorConfig newOperatorConfigInstance() {
    return new SimpleOperatorConfig();
  }

  @Override
  public void initializeIOPorts() {

  }
}
