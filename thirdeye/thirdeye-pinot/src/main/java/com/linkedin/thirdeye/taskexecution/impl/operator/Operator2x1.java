package com.linkedin.thirdeye.taskexecution.impl.operator;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.impl.dataflow.GenericInputPort;
import com.linkedin.thirdeye.taskexecution.dataflow.reader.InputPort;
import org.apache.commons.configuration.Configuration;


public abstract class Operator2x1<IN1, IN2, OUT> extends Operator0x1<OUT> {
  private final InputPort<IN1> inputPort1;
  private final InputPort<IN2> inputPort2;


  public Operator2x1(NodeIdentifier nodeIdentifier, Configuration configuration) {
    super(nodeIdentifier, configuration);
    inputPort1 = new GenericInputPort<>(nodeIdentifier);
    inputPort2 = new GenericInputPort<>(nodeIdentifier);
  }

  public InputPort<IN1> getInputPort1() {
    return inputPort1;
  }

  public InputPort<IN2> getInputPort2() {
    return inputPort2;
  }

  @Override
  public void initializeIOPorts() {
    super.initializeIOPorts();
    inputPort1.initialize();
    inputPort2.initialize();
  }
}
