package com.linkedin.thirdeye.taskexecution.impl.dataflow;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.dataflow.reader.Reader;
import com.linkedin.thirdeye.taskexecution.dataflow.writer.OutputPort;
import com.linkedin.thirdeye.taskexecution.dataflow.writer.Writer;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GenericOutputPort<T> implements OutputPort<T> {
  private NodeIdentifier nodeIdentifier;
  private Collection<T> storage;

  public GenericOutputPort(NodeIdentifier nodeIdentifier) {
    this.nodeIdentifier = Preconditions.checkNotNull(nodeIdentifier);
  }

  @Override
  public void initialize() {
    storage = new ConcurrentLinkedQueue<>();
  }

  @Override
  public NodeIdentifier getNodeIdentifier() {
    return nodeIdentifier;
  }

  @Override
  public Writer<T> getWriter() {
    Preconditions.checkNotNull(storage, "This port is not initialized; Please invoke initialize before this method.");
    return new CollectionWriter<>(storage);
  }

  @Override
  public Reader<T> getReader() {
    Preconditions.checkNotNull(storage, "This port is not initialized; Please invoke initialize before this method.");
    return new CollectionReader<>(storage);
  }

  @Override
  public void setDelegatePort(OutputPort<T> delegatePort) {
    throw new UnsupportedOperationException();
  }
}
