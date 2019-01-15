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
package com.linkedin.pinot.core.operator.dociditerators;

import com.linkedin.pinot.core.common.Constants;
import org.apache.lucene.search.ScoreDoc;
import org.roaringbitmap.IntIterator;
import sun.util.resources.cldr.so.CurrencyNames_so;


public final class LuceneDocIdIterator implements IndexBasedDocIdIterator {
  private int _endDocId = Integer.MAX_VALUE;
  private int _startDocId;
  private int _currentDocId = -1;
  private int _currentIndex = -1;
  private final ScoreDoc[] _scoreDocs;

  public LuceneDocIdIterator(ScoreDoc[] docs) {
    _scoreDocs = docs;
  }

  @Override
  public int currentDocId() {
    return _currentDocId;
  }

  public void setStartDocId(int startDocId) {
    this._startDocId = startDocId;
  }

  public void setEndDocId(int endDocId) {
    this._endDocId = endDocId;
  }

  @Override
  public int next() {
    // Empty?
    if (_currentDocId == Constants.EOF || ++_currentIndex == _scoreDocs.length) {
      _currentDocId = Constants.EOF;
      return Constants.EOF;
    }

    _currentDocId = _scoreDocs[_currentIndex].doc;

    // Advance to startDocId if necessary
    while(_currentDocId < _startDocId && ++_currentIndex < _scoreDocs.length) {
      _currentDocId = _scoreDocs[_currentIndex].doc;
    }

    // Current docId outside of the valid range?
    if (_currentDocId < _startDocId || _endDocId < _currentDocId) {
      _currentDocId = Constants.EOF;
    }

    return _currentDocId;
  }

  @Override
  public int advance(int targetDocId) {
    if (targetDocId < _currentDocId) {
      throw new IllegalArgumentException("Trying to move backwards to docId " + targetDocId +
          ", current position " + _currentDocId);
    }

    if (_currentDocId == targetDocId) {
      return _currentDocId;
    } else {
      int curr = next();
      while(curr < targetDocId && curr != Constants.EOF) {
        curr = next();
      }
      return curr;
    }
  }

  public String toString() {
    return LuceneDocIdIterator.class.getSimpleName();
  }

}
