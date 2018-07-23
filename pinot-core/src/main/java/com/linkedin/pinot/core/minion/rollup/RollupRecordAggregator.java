/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.minion.rollup;

import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.minion.rollup.aggregate.ValueAggregator;
import com.linkedin.pinot.core.minion.rollup.aggregate.ValueAggregatorFactory;
import com.linkedin.pinot.core.minion.segment.RecordAggregator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RollupRecordAggregator implements RecordAggregator {
  private final static ValueAggregator DEFAULT_AGGREGATOR_FUNCTION = ValueAggregatorFactory.getValueAggregator("SUM");

  private Map<String, ValueAggregator> _aggregatorMap;
  private Schema _schema;

  public RollupRecordAggregator(Schema schema, Map<String, String> aggregateTypes) {
    _schema = schema;
    // Initialize aggregatorMap
    _aggregatorMap = new HashMap<>();
    for (Map.Entry<String, String> entry: aggregateTypes.entrySet()) {
      _aggregatorMap.put(entry.getKey(), ValueAggregatorFactory.getValueAggregator(entry.getValue()));
    }
  }

  @Override
  public GenericRow aggregateRecords(List<GenericRow> rows) {
    GenericRow resultRow = rows.get(0);
    for (int i = 1; i < rows.size(); i++) {
      GenericRow currentRow = rows.get(i);
      for (MetricFieldSpec metric : _schema.getMetricFieldSpecs()) {
        String metricName = metric.getName();
        ValueAggregator aggregator = (_aggregatorMap == null) ? DEFAULT_AGGREGATOR_FUNCTION
            : _aggregatorMap.getOrDefault(metricName, DEFAULT_AGGREGATOR_FUNCTION);
        Object aggregatedResult =
            aggregator.aggregate(resultRow.getValue(metricName), currentRow.getValue(metricName), metric);
        resultRow.putField(metricName, aggregatedResult);
      }
    }
    return resultRow;
  }
}
