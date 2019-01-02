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
package com.linkedin.pinot.core.segment.creator.impl.textsearch;

import java.io.File;


public class TextSearchIndexConfig {

  private final TextSearchIndexType _type;
  private final boolean _store;
  private final File _indexDir;
  private final String _columnName;

  public TextSearchIndexConfig(TextSearchIndexType type, boolean store, File indexDir, String columnName) {
    _type = type;
    _store = store;
    _indexDir = indexDir;
    _columnName = columnName;
  }

  public TextSearchIndexType getType() {
    return _type;
  }

  public boolean shouldStore() {
    return _store;
  }

  public File getIndexDir() {
    return _indexDir;
  }

  public String getColumnName() {
    return _columnName;
  }

  public static TextSearchIndexConfig getDefaultConfig(String columnName, File indexDir) {
    return new TextSearchIndexConfig(TextSearchIndexType.LUCENE, true, indexDir, columnName);
  }
}
