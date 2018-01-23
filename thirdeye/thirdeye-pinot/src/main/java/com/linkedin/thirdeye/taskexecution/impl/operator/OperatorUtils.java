package com.linkedin.thirdeye.taskexecution.impl.operator;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperatorUtils {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorUtils.class);

  private OperatorUtils() { }

  /**
   * Instantiates an operator of the specified class with a node identifier.
   *
   * @param nodeIdentifier the node identifier to be associated with the instantiated operator.
   * @param operatorClass the class of the operator to be instantiated.
   *
   * @return an operator of the specified class that is associated with the given node identifier.
   */
  public static <T extends AbstractOperator> T initiateOperatorInstance(NodeIdentifier nodeIdentifier,
      Class<T> operatorClass) {
    return initiateOperatorInstance(nodeIdentifier, new MapConfiguration(Collections.emptyMap()), operatorClass);
  }

  /**
   * Instantiates an operator of the specified class with a node identifier and a configuration.
   *
   * @param nodeIdentifier the node identifier to be associated with the instantiated operator.
   * @param configuration the configuration for the instantiated operator.
   * @param operatorClass the class of the operator to be instantiated.
   *
   * @return an operator of the specified class that is associated with the given node identifier.
   */
  public static <T extends AbstractOperator> T initiateOperatorInstance(NodeIdentifier nodeIdentifier,
      Configuration configuration, Class<T> operatorClass) {
    Preconditions.checkNotNull(nodeIdentifier);
    T operator;
    try {
      Constructor<T> constructor = operatorClass.getConstructor(NodeIdentifier.class, Configuration.class);
      // TODO: Pass configuration to the operator during construction
      operator = constructor.newInstance(nodeIdentifier, configuration);
    } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
      LOG.debug("Failed to initiate operator '{}' with {}({}.class, {}.class); trying out {}({}.class)",
          operatorClass.getSimpleName(), operatorClass.getSimpleName(), NodeIdentifier.class.getSimpleName(),
          Configuration.class.getSimpleName(), operatorClass.getSimpleName(), NodeIdentifier.class.getSimpleName());
      try {
        Constructor<T> constructor = operatorClass.getConstructor(NodeIdentifier.class);
        operator = constructor.newInstance(nodeIdentifier);
      } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e1) {
        LOG.error("Failed to initiate operator '{}' with {}({}.class, {}.class) or {}({}.class).",
            operatorClass.getSimpleName(), operatorClass.getSimpleName(), NodeIdentifier.class.getSimpleName(),
            Configuration.class.getSimpleName(), operatorClass.getSimpleName(), NodeIdentifier.class.getSimpleName());
        throw new IllegalArgumentException(e1);
      }
    }
    return operator;
  }
}
