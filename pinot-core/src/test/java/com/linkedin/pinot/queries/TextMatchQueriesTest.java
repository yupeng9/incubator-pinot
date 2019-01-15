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
package com.linkedin.pinot.queries;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.response.broker.AggregationResult;
import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import com.linkedin.pinot.common.response.broker.GroupByResult;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.manager.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.query.AggregationGroupByOperator;
import com.linkedin.pinot.core.operator.query.AggregationOperator;
import com.linkedin.pinot.core.query.aggregation.function.PercentileTDigestAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.creator.impl.textsearch.LuceneIndexCreator;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.tdunning.math.stats.TDigest;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for TEXT_MATCH queries.
 *
 * <ul>
 *   <li>Generates a segment with a TEXT column, and 2 group by columns.</li>
 *   <li>Runs simple text-match queries.</li>
 * </ul>
 */
public class TextMatchQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "TextMatchQueriesTest");
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_ROWS = 2000;
  private static final double VALUE_RANGE = Integer.MAX_VALUE;
  private static final String TEXT_COLUMN = "textColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String GROUP_BY_COLUMN = "groupByColumn";
  private static final String[] GROUPS = new String[]{"G1", "G2", "G3"};
  private static final long RANDOM_SEED = System.nanoTime();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final String ERROR_MESSAGE = "Random seed: " + RANDOM_SEED;

  private static final String QUERY_FILTER = "WHERE TEXT_MATCH(textColumn, \"Fire*\", \"\")";

  private ImmutableSegment _indexSegment;
  private List<SegmentDataManager> _segmentDataManagers;

  @Override
  protected String getFilter() {
    return QUERY_FILTER;
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<SegmentDataManager> getSegmentDataManagers() {
    return _segmentDataManagers;
  }

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    try {
      buildSegment();
    } catch (Exception e) {
      e.printStackTrace();
    }
    IndexLoadingConfig config = new IndexLoadingConfig();
    config.setInvertedIndexColumns(Collections.singleton(TEXT_COLUMN));
    try {
      _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), config);
    } catch(Exception e) {
      e.printStackTrace();
    }
    _segmentDataManagers =
        Arrays.asList(new ImmutableSegmentDataManager(_indexSegment), new ImmutableSegmentDataManager(_indexSegment));
  }

  protected void buildSegment() throws Exception {

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    URL resourceUrl = getClass().getClassLoader().getResource("data/logfile");
    File file = new File(resourceUrl.getFile());
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {

      for (int i = 0; i < NUM_ROWS; i++) {
        HashMap<String, Object> valueMap = new HashMap<>();

        double value = RANDOM.nextDouble() * VALUE_RANGE;
        valueMap.put(DOUBLE_COLUMN, value);

        // add the text column
        valueMap.put(TEXT_COLUMN, br.readLine());

        String group = GROUPS[RANDOM.nextInt(GROUPS.length)];
        valueMap.put(GROUP_BY_COLUMN, group);

        GenericRow genericRow = new GenericRow();
        genericRow.init(valueMap);
        rows.add(genericRow);
      }
    }

    Schema schema = new Schema();
    schema.addField(new MetricFieldSpec(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE));
    schema.addField(new DimensionFieldSpec(TEXT_COLUMN, FieldSpec.DataType.TEXT, true));
    schema.addField(new DimensionFieldSpec(GROUP_BY_COLUMN, FieldSpec.DataType.STRING, true));

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setInvertedIndexCreationColumns(Collections.singletonList(TEXT_COLUMN));
    config.setRawIndexCreationColumns(Collections.singletonList(TEXT_COLUMN));

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows, schema)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  @Test
  public void testTextSearchFilter() {
    String query = "SELECT COUNT(*) FROM testTable " + getFilter();

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    // todo: verify counts here
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 3364, 0L, 0L, 8000,
        new String[]{"3364"});
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
