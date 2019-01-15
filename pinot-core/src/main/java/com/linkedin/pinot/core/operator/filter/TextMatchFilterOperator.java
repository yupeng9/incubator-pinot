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
package com.linkedin.pinot.core.operator.filter;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.operator.blocks.FilterBlock;
import com.linkedin.pinot.core.operator.docidsets.ArrayBasedDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.BitmapDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.LuceneDocIdSet;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.TextMatchPredicateEvaluatorFactory.*;
import com.linkedin.pinot.core.segment.index.readers.SearchIndexReader;
import org.apache.lucene.search.TopDocs;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class TextMatchFilterOperator extends BaseFilterOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(TextMatchFilterOperator.class);
  private static final String OPERATOR_NAME = "TextMatchFilterOperator";

  private final String _query;
  private final String _options;
  private final DataSource _dataSource;
  private final int _startDocId;
  // TODO: change it to exclusive
  // Inclusive
  private final int _endDocId;
  private TopDocs _docs;

  TextMatchFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource, int startDocId,
                          int endDocId) {
    // NOTE:
    // Predicate that is always evaluated as true or false should not be passed into the TextMatchFilterOperator for
    // performance concern.
    // If predicate is always evaluated as true, use MatchAllFilterOperator; if predicate is always evaluated as false,
    // use EmptyFilterOperator.
    Preconditions.checkArgument(!predicateEvaluator.isAlwaysTrue() && !predicateEvaluator.isAlwaysFalse());
    Preconditions.checkArgument(predicateEvaluator instanceof RawValueBasedTextMatchPredicateEvaluator);

    RawValueBasedTextMatchPredicateEvaluator evaluator = (RawValueBasedTextMatchPredicateEvaluator) predicateEvaluator;
    _query = evaluator.getQueryString();
    _options = evaluator.getQueryOptions();
    _dataSource = dataSource;
    _startDocId = startDocId;
    _endDocId = endDocId;
  }

  @Override
  protected FilterBlock getNextBlock() {

    if (_docs == null) {
      SearchIndexReader<TopDocs> searchIndex = _dataSource.getSearchIndex();
      _docs = searchIndex.getDocIds(_query, _options);
    }
    return new FilterBlock(new LuceneDocIdSet(_docs.scoreDocs, _startDocId, _endDocId));
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
