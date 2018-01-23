package com.linkedin.thirdeye.taskexecution.impl.dataflow;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.dataflow.reader.InputPort;
import com.linkedin.thirdeye.taskexecution.dataflow.reader.Reader;

public class GenericInputPort<T> implements InputPort<T> {
  private NodeIdentifier nodeIdentifier;
  private CollectionReader.Builder<T> builder;

  /**
   * Constructs a input port that is associated to the node or the operator with the given node identifier.
   *
   * @param nodeIdentifier the node identifier of the node or operator with which the port is associated.
   */
  public GenericInputPort(NodeIdentifier nodeIdentifier) {
    this.nodeIdentifier = Preconditions.checkNotNull(nodeIdentifier);
  }

  @Override
  public NodeIdentifier getNodeIdentifier() {
    return nodeIdentifier;
  }

  @Override
  public void initialize() {
    builder = CollectionReader.builder();
  }

  @Override
  public void addContext(Reader<T> reader) {
    Preconditions.checkNotNull(builder, "This port is not initialized; please invoke initialize before this method.");
    if (reader != null) {
      while (reader.hasNext()) {
        builder.add(reader.next());
      }
    }
  }

  @Override
  public Reader<T> getReader() {
    Preconditions.checkNotNull(builder, "This port is not initialized; please invoke initialize before this method.");
    return builder.build();
  }

  @Override
  public void setDelegatePort(InputPort<T> delegatePort) {
    throw new UnsupportedOperationException();
  }
}
