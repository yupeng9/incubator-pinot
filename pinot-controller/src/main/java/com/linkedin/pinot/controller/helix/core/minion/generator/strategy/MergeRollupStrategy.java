package com.linkedin.pinot.controller.helix.core.minion.generator.strategy;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;


public interface MergeRollupStrategy {
  List<Pair<String, List<String>>> generateMergeRollupTasks();
}
