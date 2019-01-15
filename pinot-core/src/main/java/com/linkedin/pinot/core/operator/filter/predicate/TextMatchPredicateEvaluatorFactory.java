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
package com.linkedin.pinot.core.operator.filter.predicate;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.TextMatchPredicate;


/**
 * Factory for REGEXP_LIKE predicate evaluators.
 */
public class TextMatchPredicateEvaluatorFactory {
  private TextMatchPredicateEvaluatorFactory() {
  }

  /**
   * Create a new instance of raw value based REGEXP_LIKE predicate evaluator.
   *
   * @param textMatchPredicate REGEXP_LIKE predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based REGEXP_LIKE predicate evaluator
   */
  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(TextMatchPredicate textMatchPredicate,
      FieldSpec.DataType dataType) {
    return new RawValueBasedTextMatchPredicateEvaluator(textMatchPredicate);
  }

  public static final class RawValueBasedTextMatchPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    String _query;
    String _options;
    public RawValueBasedTextMatchPredicateEvaluator(TextMatchPredicate textMatchPredicate) {
      _query = textMatchPredicate.getQuery();
      _options = textMatchPredicate.getQueryOptions();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.TEXT_MATCH;
    }

    @Override
    public boolean applySV(String value) {
      throw new UnsupportedOperationException("Text Match is not supported via scanning, its supported only via inverted index");
    }

    public String getQueryString() {
      return _query;
    }

    public String getQueryOptions() {
      return _options;
    }
  }
}
