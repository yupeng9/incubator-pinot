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
    Preconditions.checkNotNull(nodeIdentifier);

    try {
      Constructor<T> constructor = operatorClass.getConstructor(NodeIdentifier.class);
      return constructor.newInstance(nodeIdentifier);
    } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
      LOG.debug("Failed to initiate operator '{}' with {}({}.class); trying out {}({}.class, {}.class)",
          operatorClass.getSimpleName(), operatorClass.getSimpleName(), NodeIdentifier.class.getSimpleName(),
          operatorClass.getSimpleName(), NodeIdentifier.class.getSimpleName(), Configuration.class.getSimpleName());
      try {
        Constructor<T> constructor = operatorClass.getConstructor(NodeIdentifier.class, Configuration.class);
        return constructor.newInstance(nodeIdentifier, new MapConfiguration(Collections.emptyMap()));
      } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e1) {
        String errorMessage = String
            .format("Failed to initiate operator '%s' with %s(%s.class, %s.class) or %s(%s.class).",
                operatorClass.getSimpleName(), operatorClass.getSimpleName(), NodeIdentifier.class.getSimpleName(),
                Configuration.class.getSimpleName(), operatorClass.getSimpleName(),
                NodeIdentifier.class.getSimpleName());
        throw new RuntimeException(errorMessage);
      }
    }
  }

  /**
   * Instantiates an operator of the specified class with a node identifier and a configuration. The specified class
   * must have this constructor method: Constructor(NodeIdentifier.class, Configuration.class).
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
    try {
      Constructor<T> constructor = operatorClass.getConstructor(NodeIdentifier.class, Configuration.class);
      // TODO: Pass configuration to the operator during construction
      return constructor.newInstance(nodeIdentifier, configuration);
    } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
      String errorMessage = String
          .format("Failed to initiate operator '%s' with %s(%s.class, %s.class).", operatorClass.getSimpleName(),
              operatorClass.getSimpleName(), NodeIdentifier.class.getSimpleName(), Configuration.class.getSimpleName(),
              operatorClass.getSimpleName(), NodeIdentifier.class.getSimpleName());
      throw new RuntimeException(errorMessage);
    }
  }
}
