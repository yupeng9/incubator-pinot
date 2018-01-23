package com.linkedin.thirdeye.taskexecution.dag;

import com.linkedin.thirdeye.taskexecution.operator.Operator;
import java.util.Set;

public interface Node<E extends Edge> {

  /** Execution Related Methods **/
  NodeIdentifier getIdentifier();

  Operator getOperator();

  /** Topology Related Methods **/
  void addIncomingNode(NodeIdentifier node);

  void addOutgoingNode(NodeIdentifier node);

  Set<NodeIdentifier> getIncomingNodes();

  Set<NodeIdentifier> getOutgoingNodes();

  void addIncomingEdge(E edge);

  void addOutgoingEdge(E edge);

  Set<E> getIncomingEdges();

  Set<E> getOutgoingEdges();
}
