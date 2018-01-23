package com.linkedin.thirdeye.taskexecution.impl.operatordag;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.executor.ExecutionStatus;
import com.linkedin.thirdeye.taskexecution.impl.operator.AbstractOperator;
import java.util.Collections;
import org.apache.commons.configuration.MapConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OperatorNodeTest {
  private OperatorNode node;
  @Test
  public void testCreation() {
    node = new OperatorNode(new DummyOperator("Test"));
  }

  @Test (enabled = false, dependsOnMethods = "testCreation")
  public void testEmptyNode() {
    Assert.assertEquals(node.getExecutionStatus(), ExecutionStatus.SKIPPED);
  }

  @Test
  public void testEquals() {

  }

  public static class DummyOperator extends AbstractOperator {
    public DummyOperator(String name) {
      super(new NodeIdentifier(name), new MapConfiguration(Collections.emptyMap()));
    }

    @Override
    public void run() {
    }
  }
}
