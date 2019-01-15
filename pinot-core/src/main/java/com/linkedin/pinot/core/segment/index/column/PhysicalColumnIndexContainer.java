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
package com.linkedin.pinot.core.segment.index.column;

import com.linkedin.pinot.common.config.TextSearchIndexConfig;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedByteChunkSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.SortedIndexReader;
import com.linkedin.pinot.core.io.reader.impl.v1.SortedIndexReaderImpl;
import com.linkedin.pinot.core.io.reader.impl.v1.VarByteChunkSingleValueReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.segment.index.readers.BitmapInvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.BloomFilterReader;
import com.linkedin.pinot.core.segment.index.readers.BytesDictionary;
import com.linkedin.pinot.core.segment.index.readers.DoubleDictionary;
import com.linkedin.pinot.core.segment.index.readers.FloatDictionary;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.IntDictionary;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.LongDictionary;
import com.linkedin.pinot.core.segment.index.readers.LuceneSearchIndexReader;
import com.linkedin.pinot.core.segment.index.readers.OnHeapDoubleDictionary;
import com.linkedin.pinot.core.segment.index.readers.OnHeapFloatDictionary;
import com.linkedin.pinot.core.segment.index.readers.OnHeapIntDictionary;
import com.linkedin.pinot.core.segment.index.readers.OnHeapLongDictionary;
import com.linkedin.pinot.core.segment.index.readers.OnHeapStringDictionary;
import com.linkedin.pinot.core.segment.index.readers.SearchIndexReader;
import com.linkedin.pinot.core.segment.index.readers.StringDictionary;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class PhysicalColumnIndexContainer implements ColumnIndexContainer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalColumnIndexContainer.class);

  private final DataFileReader _forwardIndex;
  private final InvertedIndexReader _invertedIndex;
  private final SearchIndexReader _searchIndex;
  private final ImmutableDictionaryReader _dictionary;
  private final BloomFilterReader _bloomFilterReader;

  public PhysicalColumnIndexContainer(SegmentDirectory.Reader segmentReader, File indexDir,
      ColumnMetadata metadata, IndexLoadingConfig indexLoadingConfig) throws IOException {
    String columnName = metadata.getColumnName();
    boolean loadInvertedIndex = false;
    boolean loadOnHeapDictionary = false;
    boolean loadBloomFilter = false;
    if (indexLoadingConfig != null) {
      loadInvertedIndex = indexLoadingConfig.getInvertedIndexColumns().contains(columnName);
      loadOnHeapDictionary = indexLoadingConfig.getOnHeapDictionaryColumns().contains(columnName);
      loadBloomFilter = indexLoadingConfig.getBloomFilterColumns().contains(columnName);
    }
    PinotDataBuffer fwdIndexBuffer = segmentReader.getIndexFor(columnName, ColumnIndexType.FORWARD_INDEX);

    FieldSpec.DataType type = metadata.getDataType();
    if (metadata.hasDictionary()) {
      _searchIndex = null;
      //bloom filter
      if (loadBloomFilter) {
        PinotDataBuffer bloomFilterBuffer = segmentReader.getIndexFor(columnName, ColumnIndexType.BLOOM_FILTER);
        _bloomFilterReader = new BloomFilterReader(bloomFilterBuffer);
      } else {
        _bloomFilterReader = null;
      }
      // Dictionary-based index
      _dictionary = loadDictionary(segmentReader.getIndexFor(columnName, ColumnIndexType.DICTIONARY), metadata,
          loadOnHeapDictionary);
      if (metadata.isSingleValue()) {
        // Single-value
        if (metadata.isSorted()) {
          // Sorted
          SortedIndexReader sortedIndexReader = new SortedIndexReaderImpl(fwdIndexBuffer, metadata.getCardinality());
          _forwardIndex = sortedIndexReader;
          _invertedIndex = sortedIndexReader;
          return;
        } else {
          // Unsorted
          _forwardIndex =
              new FixedBitSingleValueReader(fwdIndexBuffer, metadata.getTotalDocs(), metadata.getBitsPerElement());
        }
      } else {
        // Multi-value
        _forwardIndex =
            new FixedBitMultiValueReader(fwdIndexBuffer, metadata.getTotalDocs(), metadata.getTotalNumberOfEntries(),
                metadata.getBitsPerElement());
      }
      if (loadInvertedIndex) {
        _invertedIndex =
            new BitmapInvertedIndexReader(segmentReader.getIndexFor(columnName, ColumnIndexType.INVERTED_INDEX),
                metadata.getCardinality());
      } else {
        _invertedIndex = null;
      }
    } else {
      // Raw index
      _forwardIndex = loadRawForwardIndex(fwdIndexBuffer, type);
      _dictionary = null;
      _bloomFilterReader = null;
      _invertedIndex = null;
      if (loadInvertedIndex) {
        if (type == FieldSpec.DataType.TEXT) {
          TextSearchIndexConfig searchConfig = indexLoadingConfig.getTextSearchIndexConfig();
          if (searchConfig == null) {
            searchConfig = TextSearchIndexConfig.getDefaultConfig();
          }
          switch (searchConfig.getType()) {
            case "LUCENE":
              _searchIndex = new LuceneSearchIndexReader(columnName, indexDir);
              break;
            default:
              throw new RuntimeException("Unsupported Text search index type " + searchConfig.getType());
          }
        } else {
          _searchIndex = null;
        }
      } else {
        _searchIndex = null;
      }
    }
  }

  @Override
  public DataFileReader getForwardIndex() {
    return _forwardIndex;
  }

  @Override
  public InvertedIndexReader getInvertedIndex() {
    return _invertedIndex;
  }

  @Override
  public SearchIndexReader getSearchIndex() {
    return _searchIndex;
  }

  @Override
  public ImmutableDictionaryReader getDictionary() {
    return _dictionary;
  }

  @Override
  public BloomFilterReader getBloomFilter() {
    return _bloomFilterReader;
  }


  private static ImmutableDictionaryReader loadDictionary(PinotDataBuffer dictionaryBuffer, ColumnMetadata metadata,
      boolean loadOnHeap) {
    FieldSpec.DataType dataType = metadata.getDataType();
    if (loadOnHeap) {
      String columnName = metadata.getColumnName();
      LOGGER.info("Loading on-heap dictionary for column: {}", columnName);
    }

    int length = metadata.getCardinality();
    switch (dataType) {
      case INT:
        return (loadOnHeap) ? new OnHeapIntDictionary(dictionaryBuffer, length)
            : new IntDictionary(dictionaryBuffer, length);

      case LONG:
        return (loadOnHeap) ? new OnHeapLongDictionary(dictionaryBuffer, length)
            : new LongDictionary(dictionaryBuffer, length);

      case FLOAT:
        return (loadOnHeap) ? new OnHeapFloatDictionary(dictionaryBuffer, length)
            : new FloatDictionary(dictionaryBuffer, length);

      case DOUBLE:
        return (loadOnHeap) ? new OnHeapDoubleDictionary(dictionaryBuffer, length)
            : new DoubleDictionary(dictionaryBuffer, length);

      case STRING:
        int numBytesPerValue = metadata.getColumnMaxLength();
        byte paddingByte = (byte) metadata.getPaddingCharacter();
        return loadOnHeap ? new OnHeapStringDictionary(dictionaryBuffer, length, numBytesPerValue, paddingByte)
            : new StringDictionary(dictionaryBuffer, length, numBytesPerValue, paddingByte);

      case BYTES:
        numBytesPerValue = metadata.getColumnMaxLength();
        return new BytesDictionary(dictionaryBuffer, length, numBytesPerValue);

      default:
        throw new IllegalStateException("Illegal data type for dictionary: " + dataType);
    }
  }

  private static SingleColumnSingleValueReader loadRawForwardIndex(PinotDataBuffer forwardIndexBuffer,
      FieldSpec.DataType dataType) {

    switch (dataType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new FixedByteChunkSingleValueReader(forwardIndexBuffer);
      case STRING:
      case BYTES:
      case TEXT:
        return new VarByteChunkSingleValueReader(forwardIndexBuffer);
      default:
        throw new IllegalStateException("Illegal data type for raw forward index: " + dataType);
    }
  }

}
