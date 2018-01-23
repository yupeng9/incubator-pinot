package com.linkedin.thirdeye.taskexecution.impl.dataflow;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.dataflow.reader.InputPort;
import com.linkedin.thirdeye.taskexecution.dataflow.reader.Reader;


public class DelegateInputPort<T> implements InputPort<T> {
  private InputPort<T> delegatePort;

  @Override
  public NodeIdentifier getNodeIdentifier() {
    return delegatePort.getNodeIdentifier();
  }

  @Override
  public void initialize() {
    delegatePort.initialize();
  }

  @Override
  public void addContext(Reader<T> reader) {
    delegatePort.addContext(reader);
  }

  @Override
  public Reader<T> getReader() {
    return delegatePort.getReader();
  }

  @Override
  public void setDelegatePort(InputPort<T> delegatePort) {
    this.delegatePort = delegatePort;
  }
}
