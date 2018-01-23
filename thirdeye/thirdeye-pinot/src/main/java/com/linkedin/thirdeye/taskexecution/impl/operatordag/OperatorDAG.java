package com.linkedin.thirdeye.taskexecution.impl.operatordag;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.linkedin.thirdeye.taskexecution.dag.DAG;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class OperatorDAG implements DAG {
  private Map<NodeIdentifier, OperatorNode> nodes = new HashMap<>();

  /**
   * Add the given node if it has not been inserted to this DAG and returns the node that has the same {@link
   * NodeIdentifier}.
   *
   * @param node the node to be added.
   *
   * @return the node that is just being added or the existing node that has the same {@link NodeIdentifier}.
   */
  OperatorNode addNode(OperatorNode node) {
    Preconditions.checkNotNull(node, "Unable to add an null node.");
    return getOrAdd(node);
  }

  /**
   * Add execution dependency from the source to the sink without considering data exchange between them. See {@link
   * #addChannel(Channel)} if there is data needs to be passed from the source to the sink.
   *
   * @param source the source node.
   * @param sink the sink node.
   */
  void addExecutionDependency(OperatorNode source, OperatorNode sink) {
    Preconditions.checkNotNull(source, "Unable to add an null source.");
    Preconditions.checkNotNull(sink, "Unable to add an null sink.");
    source = getOrAdd(source);
    sink = getOrAdd(sink);
    addNodeDependency(source, sink);
  }

  /**
   * Add execution dependency from the source to the sink of the channel. Moreover, connect the output port of source
   * to the input port of sink.
   *
   * @param channel the channel that specified the output port of source and input port of sink.
   */
  void addChannel(Channel channel) {
    OperatorNode source = getNode(channel.getSourceIdentifier());
    OperatorNode sink = getNode(channel.getSinkIdentifier());
    Preconditions.checkNotNull(source, "Source node '%s' doesn't exist in DAG.", channel.getSourceIdentifier());
    Preconditions.checkNotNull(sink, "Sink node '%s' doesn't exist in DAG.", channel.getSinkIdentifier());
    Preconditions.checkArgument(!Objects.equals(source, sink), "Source and sink node cannot be the same.");

    addNodeDependency(source, sink);
    source.addOutgoingEdge(channel);
    sink.addIncomingEdge(channel);
  }

  /**
   * Internal implementation for adding execution dependency from the source to the sink node.
   *
   * @param source the source node.
   * @param sink the sink node.
   */
  private void addNodeDependency(OperatorNode source, OperatorNode sink) {
    source.addOutgoingNode(sink.getIdentifier());
    sink.addIncomingNode(source.getIdentifier());
  }

  /**
   * Returns the given node if it has not been inserted to this DAG; otherwise, returns the previous inserted node,
   * which has the same {@link NodeIdentifier}.
   *
   * @param node the node to get or be added.
   *
   * @return the node with the same {@link NodeIdentifier}.
   */
  private OperatorNode getOrAdd(OperatorNode node) {
    NodeIdentifier nodeIdentifier = node.getIdentifier();
    if (!nodes.containsKey(nodeIdentifier)) {
      nodes.put(nodeIdentifier, node);
      return node;
    } else {
      return nodes.get(nodeIdentifier);
    }
  }

  @Override
  public OperatorNode getNode(NodeIdentifier nodeIdentifier) {
    Preconditions.checkNotNull(nodeIdentifier);
    return nodes.get(nodeIdentifier);
  }

  @Override
  public Set<OperatorNode> getParents(NodeIdentifier nodeIdentifier) {
    OperatorNode node = getNode(nodeIdentifier);
    if (node != null) {
      Set<NodeIdentifier> incomingNodeIdentifiers = node.getIncomingNodes();
      Set<OperatorNode> parents = new HashSet<>();
      for (NodeIdentifier incomingNodeIdentifier : incomingNodeIdentifiers) {
        parents.add(getNode(incomingNodeIdentifier));
      }
      return parents;
    } else {
      return Collections.emptySet();
    }
  }

  @Override
  public Set<OperatorNode> getChildren(NodeIdentifier nodeIdentifier) {
    OperatorNode node = getNode(nodeIdentifier);
    if (node != null) {
      Set<NodeIdentifier> outgoingNodeIdentifiers = node.getOutgoingNodes();
      Set<OperatorNode> parents = new HashSet<>();
      for (NodeIdentifier outgoingNodeIdentifier : outgoingNodeIdentifiers) {
        parents.add(getNode(outgoingNodeIdentifier));
      }
      return parents;
    } else {
      return Collections.emptySet();
    }
  }

  @Override
  public int size() {
    return nodes.size();
  }

  @Override
  public Collection<OperatorNode> getStartNodes() {
    ImmutableList.Builder<OperatorNode> listBuilder = ImmutableList.builder();
    for (OperatorNode operatorNode : nodes.values()) {
      if (operatorNode.getIncomingNodes().size() == 0) {
        listBuilder.add(operatorNode);
      }
    }
    return listBuilder.build();
  }

  @Override
  public Collection<OperatorNode> getEndNodes() {
    ImmutableList.Builder<OperatorNode> listBuilder = ImmutableList.builder();
    for (OperatorNode operatorNode : nodes.values()) {
      if (operatorNode.getOutgoingNodes().size() == 0) {
        listBuilder.add(operatorNode);
      }
    }
    return listBuilder.build();
  }

  @Override
  public Collection<OperatorNode> getAllNodes() {
    return new HashSet<>(nodes.values());
  }

  @Override
  public void changeNamespace(String namespace) {
    List<OperatorNode> values = new ArrayList<>(nodes.values());
    nodes.clear();
    for (OperatorNode node : values) {
      NodeIdentifier identifier = node.getIdentifier();
      identifier.setNamespace(namespace);
      nodes.put(identifier, node);
    }
  }

  @Override
  public void addParentNamespace(String namespace) {
    List<OperatorNode> values = new ArrayList<>(nodes.values());
    nodes.clear();
    for (OperatorNode node : values) {
      NodeIdentifier identifier = node.getIdentifier();
      identifier.addParentNamespace(namespace);
      nodes.put(identifier, node);
    }
  }
}
