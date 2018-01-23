package com.linkedin.thirdeye.taskexecution.dataflow.reader;

import com.linkedin.thirdeye.taskexecution.operator.Operator;

public interface InputPort<T> {
  Operator getOperator();

  // Used by executor
  void initialize();

  // Used by executor; this is InputPort's getWriter()
  void addContext(Reader<T> reader);

  // Used by Operator
  Reader<T> getReader();

  /**
   * Sets a delegate port for this port; all actions of this port will be performed by the delegate port, except
   * getOperator().
   *
   * @param delegatePort the delegate port of this port.
   */
  void setDelegatePort(InputPort<T> delegatePort);
}
