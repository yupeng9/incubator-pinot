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

import com.linkedin.pinot.common.config.MultiLevelRollupSetting;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.minion.segment.RecordTransformer;
import com.linkedin.pinot.core.operator.transform.transformer.datetime.BaseDateTimeTransformer;
import com.linkedin.pinot.core.operator.transform.transformer.datetime.DateTimeTransformerFactory;


public class RollupRecordTransformer implements RecordTransformer {

  private BaseDateTimeTransformer _dateTimeTransformer;
  private TimeFieldSpec _timeColumnFieldSpec;

  public RollupRecordTransformer(TimeFieldSpec timeColumnFieldSpec, MultiLevelRollupSetting rollupSetting) {
    _timeColumnFieldSpec = timeColumnFieldSpec;

    // TODO: add validation (only long, long or string, string should be allowed)
    _dateTimeTransformer = DateTimeTransformerFactory.getDateTimeTransformer(rollupSetting.getTimeInputFormat(),
        rollupSetting.getTimeOutputFormat(), rollupSetting.getTimeOutputGranularity());
  }

  @Override
  public GenericRow transformRecord(GenericRow row) {
    String timeColumn = _timeColumnFieldSpec.getName();
    switch (_timeColumnFieldSpec.getDataType()) {
      case INT:
        long[] intInput = new long[1];
        long[] intOutput = new long[1];
        intInput[0] = (Long) row.getValue(timeColumn);
        _dateTimeTransformer.transform(intInput, intOutput, 1);
        row.putField(timeColumn, (int) intOutput[0]);
        break;
      case LONG:
        long[] longInput = new long[1];
        long[] longOutput = new long[1];
        longInput[0] = (Long) row.getValue(timeColumn);
        _dateTimeTransformer.transform(longInput, longOutput, 1);
        row.putField(timeColumn, longOutput[0]);
        break;
      case STRING:
        String[] stringInput = new String[1];
        String[] stringOutput = new String[1];
        stringInput[0] = (String) row.getValue(timeColumn);
        _dateTimeTransformer.transform(stringInput, stringOutput, 1);
        row.putField(timeColumn, stringOutput[0]);
      default:
        throw new RuntimeException(
            "Invalid data type for time column. Currently, int, long, string types are supported for time column rollup");
    }
    return row;
  }
}
