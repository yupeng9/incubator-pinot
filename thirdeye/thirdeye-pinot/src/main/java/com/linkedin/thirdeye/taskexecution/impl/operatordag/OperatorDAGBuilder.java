package com.linkedin.thirdeye.taskexecution.impl.operatordag;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.dataflow.reader.InputPort;
import com.linkedin.thirdeye.taskexecution.dataflow.writer.OutputPort;
import com.linkedin.thirdeye.taskexecution.impl.operator.AbstractOperator;
import com.linkedin.thirdeye.taskexecution.operator.Operator;
import com.linkedin.thirdeye.taskexecution.impl.operator.Operator0x1;
import com.linkedin.thirdeye.taskexecution.impl.operator.Operator1x1;
import com.linkedin.thirdeye.taskexecution.impl.operator.Operator2x1;
import com.linkedin.thirdeye.taskexecution.impl.operator.OperatorUtils;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.configuration.Configuration;

public class OperatorDAGBuilder {
  private Map<NodeIdentifier, Operator> operators;
  private Map<Channel, Channel> channels;

  public OperatorDAGBuilder() {
    initBuild();
  }

  private void initBuild() {
    operators = new HashMap<>();
    channels = new HashMap<>();
  }

  public <T extends AbstractOperator> T addOperator(String nodeName, Class<T> operatorClaz) {
    Preconditions.checkNotNull(nodeName);
    T operator = OperatorUtils.initiateOperatorInstance(new NodeIdentifier(nodeName), operatorClaz);
    return addOperator(operator);
  }

  public <T extends AbstractOperator> T addOperator(NodeIdentifier nodeIdentifier, Class<T> operatorClaz) {
    Preconditions.checkNotNull(nodeIdentifier);
    T operator = OperatorUtils.initiateOperatorInstance(nodeIdentifier, operatorClaz);
    return addOperator(operator);
  }

  public <T extends AbstractOperator> T addOperator(NodeIdentifier nodeIdentifier, Configuration configuration,
      Class<T> operatorClaz) {
    Preconditions.checkNotNull(nodeIdentifier);
    Preconditions.checkNotNull(configuration);
    T operator = OperatorUtils.initiateOperatorInstance(nodeIdentifier, configuration, operatorClaz);
    return addOperator(operator);
  }

  /**
   * Adds the given operator to the DAG. If the DAG has had an operator with the same node identifier as the given
   * operator, then the existing operator will be returned.
   *
   * @param operator the operator to be added to the DAG.
   *
   * @return the operator in the DAG.
   */
  public <T extends AbstractOperator> T addOperator(T operator) {
    Preconditions.checkNotNull(operator);
    NodeIdentifier nodeIdentifier = Preconditions.checkNotNull(operator.getNodeIdentifier());

    if (operators.containsKey(nodeIdentifier)) {
      return (T) operators.get(nodeIdentifier);
    } else {
      operators.put(nodeIdentifier, operator);
      return operator;
    }
  }

  public <T1, T2> LinkedHashSet<Channel> addChannels(Operator0x1<? extends T1> source1, Operator0x1<? extends T2> source2,
      Operator2x1<? super T1, ? super T2, ?> sink) {
    LinkedHashSet<Channel> channels = new LinkedHashSet<>();
    channels.add(addChannel(source1.getOutputPort(), sink.getInputPort1()));
    channels.add(addChannel(source2.getOutputPort(), sink.getInputPort2()));
    return channels;
  }

  public <T> Channel<T> addChannel(Operator0x1<? extends T> source, Operator1x1<? super T, ?> sink) {
    return addChannel(source.getOutputPort(), sink.getInputPort());
  }

  public <T> Channel<T> addChannel(OutputPort<? extends T> sourcePort, InputPort<? super T> sinkPort) {
    final NodeIdentifier sourceIdentity = sourcePort.getOperator().getNodeIdentifier();
    final NodeIdentifier sinkIdentify = sinkPort.getOperator().getNodeIdentifier();
    Preconditions.checkArgument(!Objects.equals(sourceIdentity, sinkIdentify), "Source and sink operators "
        + "cannot be the same.");

    Channel.ChannelBuilder<T> builder = new Channel.ChannelBuilder<>();
    builder.setSourcePort(sourcePort).setSourceIdentify(sourceIdentity)
        .setSinkPort(sinkPort).setSinkIdentity(sinkIdentify);
    Channel<T> channel = builder.createChannel();
    if (channels.containsKey(channel)) {
      return channels.get(channel);
    } else {
      channels.put(channel, channel);
      return channel;
    }
  }

  public OperatorDAG build() {
    OperatorDAG dag = new OperatorDAG();
    for (Map.Entry<NodeIdentifier, Operator> identifierOperatorPair : operators.entrySet()) {
      Operator operator = identifierOperatorPair.getValue();
      dag.addNode(new OperatorNode(operator));
    }
    for (Channel channel : channels.values()) {
      dag.addChannel(channel);
    }

    initBuild();

    return dag;
  }


}
