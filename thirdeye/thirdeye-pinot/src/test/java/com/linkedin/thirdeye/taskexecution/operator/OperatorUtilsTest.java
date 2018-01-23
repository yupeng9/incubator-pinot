package com.linkedin.thirdeye.taskexecution.operator;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.impl.operator.AbstractOperator;
import com.linkedin.thirdeye.taskexecution.impl.operator.OperatorUtils;
import java.util.Collections;
import org.apache.commons.configuration.MapConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OperatorUtilsTest {

    @Test
    public void testSuccessInitializeOperator() {
      OperatorUtils.initiateOperatorInstance(DummyOperator.class);
      OperatorUtils.initiateOperatorInstance(new NodeIdentifier("Dummy"), DummyOperator.class);
    }

    @Test
    public void testFailureInitializeOperator() {
      try {
        OperatorUtils.initiateOperatorInstance(FailedInitializedOperator.class);
      } catch (Exception e) {
        return;
      }
      Assert.fail();
    }

  public static class DummyOperator extends AbstractOperator {
    public DummyOperator() {
      super(new NodeIdentifier(), new MapConfiguration(Collections.emptyMap()));
    }

    @Override
    public void run() {
    }
  }

  public static class FailedInitializedOperator extends AbstractOperator {
    private FailedInitializedOperator() {
      super(new NodeIdentifier(), new MapConfiguration(Collections.emptyMap()));
    }

    @Override
    public void run() {
    }
  }

}
