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

import com.linkedin.pinot.core.segment.creator.NoDictionaryBasedInvertedIndexCreator;
import java.io.File;
import java.io.IOException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class LuceneIndexCreator implements NoDictionaryBasedInvertedIndexCreator {

  private static final Logger LOGGER = LoggerFactory.getLogger(LuceneIndexCreator.class);

  public static final String VERSION = "7.6.0";
  // Index file will be flushed after reaching this threshold
  private static final int MAX_BUFFER_SIZE_MB = 500;
  private static final String DEFAULT_FIELD = "TEXT";
  private static final Field.Store DEFAULT_STORE = Field.Store.NO;

  private final TextSearchIndexConfig _config;
  private final StandardAnalyzer _analyzer;
  private final IndexWriter _writer;
  private final IndexWriterConfig _indexWriterConfig;
  private final Directory _indexDirectory;

  public LuceneIndexCreator(TextSearchIndexConfig config) {
    _config = config;
    _analyzer = new StandardAnalyzer();
    _indexWriterConfig = new IndexWriterConfig(_analyzer);
    _indexWriterConfig.setRAMBufferSizeMB(MAX_BUFFER_SIZE_MB);
    File dir = new File(config.getIndexDir().getPath() + "/" + _config.getColumnName());
    try {
      _indexDirectory = FSDirectory.open(dir.toPath());
      _writer = new IndexWriter(_indexDirectory, _indexWriterConfig);
    } catch (IOException e) {
      LOGGER.error("Encountered error creating LuceneIndexCreator ", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void seal() throws IOException {

  }

  @Override
  public void add(Object doc) {
    Document document = new Document();
    document.add(new TextField(DEFAULT_FIELD, doc.toString(), DEFAULT_STORE));
    try {
      _writer.addDocument(document);
    } catch (IOException e) {
      LOGGER.error("Encountered exception while adding doc:{}", doc.toString(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    _writer.close();
  }
}
