package com.linkedin.pinot.core.segment.index.readers;

import com.linkedin.pinot.core.segment.creator.impl.textsearch.LuceneIndexCreator;
import java.io.File;
import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LuceneSearchIndexReader implements SearchIndexReader<TopDocs> {

  private static final Logger LOGGER = LoggerFactory.getLogger(LuceneSearchIndexReader.class);
  private final IndexSearcher _searcher;
  private final Analyzer _analyzer = new StandardAnalyzer();

  public LuceneSearchIndexReader(String columnName, File segmentIndexDir) {

    try {
      File searchIndexDir = new File(segmentIndexDir.getPath() + "/" + columnName);
      Directory index = FSDirectory.open(searchIndexDir.toPath());
      IndexReader reader = DirectoryReader.open(index);
      _searcher = new IndexSearcher(reader);
    } catch (IOException e) {
      LOGGER.error("Encountered error creating LuceneSearchIndexReader ", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public TopDocs getDocIds(String queryStr, String options) {
    QueryParser queryParser = new QueryParser(LuceneIndexCreator.DEFAULT_FIELD, _analyzer);
    Query query = null;
    try {
      query = queryParser.parse(queryStr);
    } catch (ParseException e) {
      LOGGER.error("Encountered exception while parsing query {}", queryStr, e);
      throw new RuntimeException(e);
    }
    TopDocs docs = null;
    try {
      docs = _searcher.search(query, Integer.MAX_VALUE);
    } catch (IOException e) {
      LOGGER.error("Encountered exception while executing search query {}", queryStr, e);
      throw new RuntimeException(e);
    }
    return docs;
  }

  @Override
  public void close() throws IOException {
  }
}
