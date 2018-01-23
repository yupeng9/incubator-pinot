package com.linkedin.thirdeye.taskexecution.dataflow.writer;

import com.linkedin.thirdeye.taskexecution.dataflow.Port;
import com.linkedin.thirdeye.taskexecution.dataflow.reader.Reader;

public interface OutputPort<T> extends Port<T> {
  // Used by Operator
  Writer<T> getWriter();

  // Used by executor
  Reader<T> getReader();

  /**
   * Sets a delegate port for this port; all actions of this port will be performed by the delegate port, except
   * getOperator().
   *
   * @param delegatePort the delegate port of this port.
   */
  void setDelegatePort(OutputPort<T> delegatePort);
}
