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
package com.linkedin.pinot.tools;

import com.google.common.base.Splitter;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.creator.impl.SegmentDictionaryCreator;
import com.linkedin.pinot.core.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class TextInvertedIndex {

    public static void main(String[] args) throws Exception {
        String rootDir = "/tmp/pinot-index/access-log/";
//        createIndices(rootDir);
        loadLuceneIndex(rootDir);
        return;

    }

    private static void loadLuceneIndex(String rootDir) throws Exception {
        MMapDirectory directory;
        Path path = Paths.get(rootDir, "lucene");
        directory = new MMapDirectory(path);
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query query = new TermQuery(new Term("content","mozilla"));
        TopDocs topDocs = searcher.search(query, 10);

        System.out.println("topDocs.totalHits = " + topDocs.totalHits);

    }

    private static void createIndices(String rootDir) throws IOException {
        Splitter splitter = Splitter.on(" ").omitEmptyStrings();
        List<String> lines = IOUtils.readLines(new FileInputStream(new File("/Users/kishoreg/Downloads/access.log")));

        HashMap<String, Integer> dictionary = new HashMap<>();
        Map<Integer, Set<Integer>> map = new HashMap<>();
        int numValues = 0;
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            Iterable<String> tokens = splitter.split(line);
            TreeSet<Integer> dictIdSet = new TreeSet<>();
            for (String token : tokens) {
                token = token.trim();
                if (!dictionary.containsKey(token)) {
                    dictionary.put(token, dictionary.size());
                }
                dictIdSet.add(dictionary.get(token));
            }
            map.put(i, dictIdSet);
            numValues += dictIdSet.size();
        }
        File indexDir;
        indexDir = new File(rootDir, "roaringBitmap");
        indexDir.delete();
        indexDir.mkdirs();
        FieldSpec fieldSpec = new DimensionFieldSpec();
        fieldSpec.setDataType(FieldSpec.DataType.STRING);
        fieldSpec.setSingleValueField(false);
        fieldSpec.setName("textField");
        int cardinality = dictionary.size();
        int numDocs = map.size();

        createRoaringBitmap(dictionary, map, numValues, indexDir, fieldSpec, cardinality, numDocs);
        createLuceneIndex(lines, rootDir);
    }

    private static void createLuceneIndex(List<String> lines, String rootDir) throws IOException {
        Directory index = FSDirectory.open(Paths.get(rootDir, "lucene"));
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig conf = new IndexWriterConfig(analyzer);
        conf.setRAMBufferSizeMB(500);
        IndexWriter writer = new IndexWriter(index, conf);
        for (int i = 0; i < lines.size(); i++) {
            Document doc = new Document();
            doc.add(new TextField("content", lines.get(i), Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.close();
    }

    private static void createRoaringBitmap(HashMap<String, Integer> dictionary, Map<Integer, Set<Integer>> map, int numValues, File indexDir, FieldSpec fieldSpec, int cardinality, int numDocs) throws IOException {
        try (OffHeapBitmapInvertedIndexCreator creator = new OffHeapBitmapInvertedIndexCreator(indexDir, fieldSpec, cardinality, numDocs, numValues)) {
            for (int i = 0; i < map.size(); i++) {
                Set<Integer> dictIdSet = map.get(new Integer(i));
                int[] dictIds = new int[dictIdSet.size()];
                int k = 0;
                for (int dictId : dictIdSet) {
                    dictIds[k++] = dictId;
                }
//                System.out.println(dictIds);
                creator.add(dictIds, dictIds.length);
            }
            creator.seal();
        }
        String sortedValues[] = new String[dictionary.size()];
        dictionary.keySet().toArray(sortedValues);
        Arrays.sort(sortedValues);
        SegmentDictionaryCreator creator = new SegmentDictionaryCreator(sortedValues, fieldSpec, indexDir);
        creator.close();
    }


}
