package com.linkedin.thirdeye.taskexecution.impl.operator;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.impl.dataflow.GenericOutputPort;
import com.linkedin.thirdeye.taskexecution.dataflow.writer.OutputPort;
import org.apache.commons.configuration.Configuration;


public abstract class Operator0x1<OUT> extends AbstractOperator {
  private final OutputPort<OUT> outputPort = new GenericOutputPort<>(this);

  public Operator0x1(NodeIdentifier nodeIdentifier, Configuration configuration) {
    super(nodeIdentifier, configuration);
  }

  public OutputPort<OUT> getOutputPort() {
    return outputPort;
  }

  @Override
  public void initializeIOPorts() {
    outputPort.initialize();
  }
}
