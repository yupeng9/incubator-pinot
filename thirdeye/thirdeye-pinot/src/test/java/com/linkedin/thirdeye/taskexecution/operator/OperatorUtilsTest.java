package com.linkedin.thirdeye.taskexecution.operator;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.impl.operator.AbstractOperator;
import com.linkedin.thirdeye.taskexecution.impl.operator.OperatorUtils;
import java.util.Collections;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.testng.annotations.Test;

public class OperatorUtilsTest {

  @Test
  public void testCompleteInitializeOperatorSuccess() {
    OperatorUtils.initiateOperatorInstance(new NodeIdentifier("Dummy"), new MapConfiguration(Collections.emptyMap()),
        CompleteDummyOperator.class);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testPartialInitializeOperatorFail() {
    OperatorUtils.initiateOperatorInstance(new NodeIdentifier("Dummy"), new MapConfiguration(Collections.emptyMap()),
        PartialDummyOperator1.class);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testInitializeOperatorFail() {
    OperatorUtils.initiateOperatorInstance(new NodeIdentifier("Dummy"), IncompleteDummyOperator.class);
  }

  @Test
  public void testPartialInitializeOperatorSuccess() {
    OperatorUtils.initiateOperatorInstance(new NodeIdentifier("Dummy"), PartialDummyOperator1.class);

    OperatorUtils.initiateOperatorInstance(new NodeIdentifier("Dummy"), PartialDummyOperator2.class);
    OperatorUtils.initiateOperatorInstance(new NodeIdentifier("Dummy"), new MapConfiguration(Collections.emptyMap()),
        PartialDummyOperator2.class);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testFailureInitializeOperator() {
    OperatorUtils.initiateOperatorInstance(new NodeIdentifier("Must FAIL"), FailedInitializedOperator.class);
  }

  public static class CompleteDummyOperator extends AbstractOperator {
    public CompleteDummyOperator(NodeIdentifier identifier, Configuration configuration) {
      super(identifier, configuration);
    }
    @Override
    public void run() {
    }
  }

  public static class PartialDummyOperator1 extends AbstractOperator {
    public PartialDummyOperator1(NodeIdentifier identifier) {
      super(identifier, new MapConfiguration(Collections.emptyMap()));
    }
    @Override
    public void run() {
    }
  }

  public static class PartialDummyOperator2 extends AbstractOperator {
    public PartialDummyOperator2(NodeIdentifier identifier, Configuration configuration) {
      super(identifier, configuration);
    }
    @Override
    public void run() {
    }
  }

  public static class IncompleteDummyOperator extends AbstractOperator {
    public IncompleteDummyOperator() {
      super(new NodeIdentifier(), new MapConfiguration(Collections.emptyMap()));
    }
    @Override
    public void run() {
    }
  }

  public static class FailedInitializedOperator extends AbstractOperator {
    private FailedInitializedOperator(NodeIdentifier nodeIdentifier) {
      super(nodeIdentifier, new MapConfiguration(Collections.emptyMap()));
    }
    @Override
    public void run() {
    }
  }

}
