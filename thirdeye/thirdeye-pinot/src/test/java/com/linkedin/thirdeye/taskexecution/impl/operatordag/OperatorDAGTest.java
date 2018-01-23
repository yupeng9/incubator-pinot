package com.linkedin.thirdeye.taskexecution.impl.operatordag;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.impl.operator.AbstractOperator;
import java.util.Collection;
import java.util.Collections;
import org.apache.commons.configuration.MapConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OperatorDAGTest {
  private OperatorDAG dag;
  private OperatorNode start;

  @Test
  public void testCreation() {
    try {
      dag = new OperatorDAG();
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test(dependsOnMethods = {"testCreation"})
  public void testAddRoot() {
    dag = new OperatorDAG();
    start = new OperatorNode(new DummyOperator("1"));
    dag.addNode(start);

    Assert.assertEquals(dag.getStartNodes().size(), 1);
    Assert.assertEquals(dag.getAllNodes().size(), 1);
    Assert.assertEquals(dag.size(), 1);
    Assert.assertEquals(dag.getEndNodes().size(), 1);
  }

  @Test(dependsOnMethods = {"testCreation", "testAddRoot"})
  public void testAddDuplicatedNode() {
    OperatorNode node2 = new OperatorNode(new DummyOperator("1"));
    OperatorNode dagNode1 = dag.addNode(node2);

    Assert.assertEquals(dagNode1, start);
    Assert.assertEquals(dag.getStartNodes().size(), 1);
    Assert.assertEquals(dag.getAllNodes().size(), 1);
    Assert.assertEquals(dag.getEndNodes().size(), 1);
  }

  @Test(dependsOnMethods = {"testCreation", "testAddRoot", "testAddDuplicatedNode"})
  public void testAddNodes() {
    OperatorNode node2 = new OperatorNode(new DummyOperator("2"));
    dag.addNode(node2);
    dag.addExecutionDependency(start, node2);
    dag.addExecutionDependency(start, node2);
    Assert.assertEquals(dag.getStartNodes().size(), 1);
    Assert.assertEquals(dag.getAllNodes().size(), 2);
    Assert.assertEquals(dag.getEndNodes().size(), 1);

    OperatorNode node3 = new OperatorNode(new DummyOperator("3"));
    dag.addNode(node3);
    dag.addExecutionDependency(start, node3);
    Assert.assertEquals(dag.getStartNodes().size(), 1);
    Assert.assertEquals(dag.getAllNodes().size(), 3);
    Assert.assertEquals(dag.getEndNodes().size(), 2);
  }

  @Test
  public void testAddNodeWithNullNodeIdentifier() {
    OperatorDAG dag = new OperatorDAG();
    try {
      OperatorNode node = new OperatorNode(new DummyOperator(""));
      node.setIdentifier(null);
      dag.addNode(node);
    } catch (NullPointerException e) {
      return;
    }
    Assert.fail();
  }

  @Test
  public void testChangeNamespace() {
    String operatorNamespace = "namespace";
    String parentsNamespace = "namespace1" + NodeIdentifier.NAMESPACE_SEPARATOR + "namespace2";
    OperatorDAG dag = new OperatorDAG();
    NodeIdentifier identifierInOperator = new NodeIdentifier(operatorNamespace, "name");
    OperatorNode operatorNode = new OperatorNode(new DummyOperator(identifierInOperator));
    dag.addNode(operatorNode);
    dag.changeNamespace(parentsNamespace);
    Collection<OperatorNode> nodes = dag.getAllNodes();
    for (OperatorNode node : nodes) {
      NodeIdentifier identifier = node.getIdentifier();
      Assert.assertEquals(identifier.getNamespace(), parentsNamespace);
      Assert.assertEquals(identifier, identifierInOperator);
    }
  }

  @Test
  public void testAddParentNamespace() {
    String operatorNamespace = "namespace";
    String parentsNamespace = "namespace1" + NodeIdentifier.NAMESPACE_SEPARATOR + "namespace2";
    OperatorDAG dag = new OperatorDAG();
    NodeIdentifier identifierInOperator1 = new NodeIdentifier(operatorNamespace, "name1");
    NodeIdentifier identifierInOperator2 = new NodeIdentifier(operatorNamespace, "name2");
    OperatorNode operatorNode1 = new OperatorNode(new DummyOperator(identifierInOperator1));
    OperatorNode operatorNode2 = new OperatorNode(new DummyOperator(identifierInOperator2));
    dag.addNode(operatorNode1);
    dag.addNode(operatorNode2);
    dag.addExecutionDependency(operatorNode1, operatorNode2);

    dag.addParentNamespace(parentsNamespace);
    // Check node 1
    {
      Assert.assertEquals(operatorNode1.getIdentifier().getNamespace(),
          parentsNamespace + NodeIdentifier.NAMESPACE_SEPARATOR + operatorNamespace);
      OperatorNode next = operatorNode1.getOutgoingNodes().iterator().next();
      Assert.assertEquals(next.getIdentifier().getFullName(),
          parentsNamespace + NodeIdentifier.NAMESPACE_SEPARATOR + operatorNamespace + NodeIdentifier.NAMESPACE_SEPARATOR
              + "name2");
    }
    // Check node 2
    {
      Assert.assertEquals(operatorNode2.getIdentifier().getNamespace(),
          parentsNamespace + NodeIdentifier.NAMESPACE_SEPARATOR + operatorNamespace);
      OperatorNode previous = operatorNode2.getIncomingNodes().iterator().next();
      Assert.assertEquals(previous.getIdentifier().getFullName(),
          parentsNamespace + NodeIdentifier.NAMESPACE_SEPARATOR + operatorNamespace + NodeIdentifier.NAMESPACE_SEPARATOR
              + "name1");
    }
  }

  public static class DummyOperator extends AbstractOperator {
    public DummyOperator(String name) {
      super(new NodeIdentifier(name), new MapConfiguration(Collections.emptyMap()));
    }

    public DummyOperator(NodeIdentifier identifier) {
      super(identifier, new MapConfiguration(Collections.emptyMap()));
    }

    @Override
    public void run() {
    }
  }
}
