package com.linkedin.thirdeye.taskexecution.operator;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.impl.operator.AbstractOperator;
import com.linkedin.thirdeye.taskexecution.impl.operator.OperatorUtils;
import java.util.Collections;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OperatorUtilsTest {

  @Test
  public void testSuccessInitializeOperator() {
    OperatorUtils.initiateOperatorInstance(new NodeIdentifier("Dummy"), new MapConfiguration(Collections.emptyMap()),
        CompleteDummyOperator.class);
    OperatorUtils.initiateOperatorInstance(new NodeIdentifier("Dummy"), CompleteDummyOperator.class);
  }

  @Test
  public void testPartialSuccessInitializeOperator() {
    OperatorUtils.initiateOperatorInstance(new NodeIdentifier("Dummy"), new MapConfiguration(Collections.emptyMap()),
        DummyOperator.class);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
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

  public static class DummyOperator extends AbstractOperator {
    public DummyOperator(NodeIdentifier identifier) {
      super(identifier, new MapConfiguration(Collections.emptyMap()));
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
