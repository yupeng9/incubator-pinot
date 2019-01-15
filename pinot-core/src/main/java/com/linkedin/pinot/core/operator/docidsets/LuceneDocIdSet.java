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
package com.linkedin.pinot.core.operator.docidsets;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.operator.dociditerators.BitmapDocIdIterator;
import com.linkedin.pinot.core.operator.dociditerators.LuceneDocIdIterator;
import com.linkedin.pinot.core.segment.creator.impl.textsearch.LuceneIndexCreator;
import org.apache.lucene.search.ScoreDoc;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class LuceneDocIdSet implements FilterBlockDocIdSet {
  private final ScoreDoc[] _docs;
  private int _startDocId;
  // Inclusive
  private int _endDocId;

  public LuceneDocIdSet(ScoreDoc[] docs, int startDocId, int endDocId) {
    _docs = docs;
    _startDocId = startDocId;
    _endDocId = endDocId;
  }

  @Override
  public int getMinDocId() {
    return _startDocId;
  }

  @Override
  public int getMaxDocId() {
    return _endDocId;
  }

  @Override
  public void setStartDocId(int startDocId) {
    _startDocId = startDocId;
  }

  @Override
  public void setEndDocId(int endDocId) {
    _endDocId = endDocId;
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return 0L;
  }

  @Override
  public BlockDocIdIterator iterator() {
    LuceneDocIdIterator docIdIterator = new LuceneDocIdIterator(_docs);
    docIdIterator.setStartDocId(_startDocId);
    docIdIterator.setEndDocId(_endDocId);
    return docIdIterator;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getRaw() {
    return null;
  }
}
