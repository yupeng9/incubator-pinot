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
package com.linkedin.pinot.core.segment.index.creator;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.query.aggregation.function.PercentileTDigestAggregationFunction;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.core.util.AvroUtils;
import com.tdunning.math.stats.TDigest;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Class for testing segment generation with text data type.
 */
public class SegmentGenerationWithTextTypeTest {
  private static final int NUM_ROWS = 10001;
  private static final int MAX_STRING_LENGTH = 1000;

  private static final String SEGMENT_DIR_NAME =
      System.getProperty("java.io.tmpdir") + File.separator + "textTypeTest";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String AVRO_DIR_NAME = System.getProperty("java.io.tmpdir") + File.separator + "textTest";

  private static final String AVRO_NAME = "text.avro";

  private static final String TEXT_SORTED_COLUMN = "sortedColumn";
  private static final String TEXT_COLUMN = "textColumn";

  private Random _random;
  private RecordReader _recordReader;
  private Schema _schema;
  private ImmutableSegment _segment;

  /**
   * Setup to build a segment with raw indexes (no-dictionary) of various data types.
   *
   * @throws Exception
   */
  @BeforeClass
  public void setup() throws Exception {

    _schema = new Schema();
    _schema.addField(new DimensionFieldSpec(TEXT_SORTED_COLUMN, FieldSpec.DataType.TEXT, true));
    _schema.addField(new DimensionFieldSpec(TEXT_COLUMN, FieldSpec.DataType.TEXT, true));

    _random = new Random(System.nanoTime());
    try {
      _recordReader = buildIndex(_schema);
      _segment = ImmutableSegmentLoader.load(new File(SEGMENT_DIR_NAME, SEGMENT_NAME), ReadMode.heap);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Clean up after test
   */
  @AfterClass
  public void cleanup() throws IOException {
    _recordReader.close();
    _segment.destroy();
    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
    FileUtils.deleteQuietly(new File(AVRO_DIR_NAME));
  }

  @Test
  public void test() throws Exception {
    PinotSegmentRecordReader pinotReader = new PinotSegmentRecordReader(new File(SEGMENT_DIR_NAME, SEGMENT_NAME));

    _recordReader.rewind();
    while (pinotReader.hasNext()) {
      GenericRow expectedRow = _recordReader.next();
      GenericRow actualRow = pinotReader.next();

      for (String column : _schema.getColumnNames()) {
        String actual = (String) actualRow.getValue(column);
        String expected = (String) expectedRow.getValue(column);

        Assert.assertEquals(actual, expected);
      }
    }

    // Ensure both record readers are exhausted, ie same number of rows.
    Assert.assertTrue(!_recordReader.hasNext());
    pinotReader.close();
  }

  @Test
  public void testMetadata() {
    Assert.assertTrue(_segment.getDataSource(TEXT_SORTED_COLUMN).getDataSourceMetadata().isSorted());
    Assert.assertFalse(_segment.getSegmentMetadata().hasDictionary(TEXT_COLUMN));
    Assert.assertFalse(_segment.getSegmentMetadata().hasDictionary(TEXT_SORTED_COLUMN));
  }

  /**
   * This test generates TEXT data, and tests segment generation.
   */
  @Test
  public void testTextAvro() throws Exception {
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(TEXT_COLUMN, FieldSpec.DataType.TEXT, true));
    schema.addField(new DimensionFieldSpec(TEXT_SORTED_COLUMN, FieldSpec.DataType.TEXT, true));

    String[] sortedExpected = new String[NUM_ROWS];
    String[] unsortedExpected = new String[NUM_ROWS];

    buildAvro(schema, sortedExpected, unsortedExpected);

    IndexSegment segment = buildSegmentFromAvro(schema, AVRO_DIR_NAME, AVRO_NAME, SEGMENT_NAME);
    SegmentMetadata metadata = segment.getSegmentMetadata();

    Assert.assertFalse(metadata.hasDictionary(TEXT_COLUMN));
    Assert.assertFalse(metadata.hasDictionary(TEXT_SORTED_COLUMN));

    PinotSegmentRecordReader reader = new PinotSegmentRecordReader(new File(AVRO_DIR_NAME, SEGMENT_NAME));
    GenericRow row = new GenericRow();

    int i = 0;
    while (reader.hasNext()) {
      row = reader.next(row);
      Assert.assertEquals(((String)row.getValue(TEXT_COLUMN)), unsortedExpected[i], "Comparison failed at index " + i);
      Assert.assertTrue(((String)row.getValue(TEXT_SORTED_COLUMN)).equals(sortedExpected[i]), "Comparison failed at index " + i);
      i++;
    }
    segment.destroy();
  }

  /**
   * Helper method to build a segment containing a single valued string column with RAW (no-dictionary) index.
   *
   * @return Array of string values for the rows in the generated index.
   * @throws Exception
   */

  private RecordReader buildIndex(Schema schema) throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);

    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);
    List<String> columns = Arrays.asList(TEXT_COLUMN, TEXT_SORTED_COLUMN);
    config.setRawIndexCreationColumns(columns);
    config.setInvertedIndexCreationColumns(columns);

    String[] sorted = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      String str = RandomStringUtils.randomAlphabetic(1 + _random.nextInt(MAX_STRING_LENGTH));
      sorted[i] = str;
    }
    Arrays.sort(sorted);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      HashMap<String, Object> map = new HashMap<>();

      map.put(TEXT_SORTED_COLUMN, sorted[i]);
      map.put(TEXT_COLUMN, RandomStringUtils.randomAlphabetic(1 + _random.nextInt(MAX_STRING_LENGTH)));

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    RecordReader recordReader = new GenericRowRecordReader(rows, schema);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, recordReader);
    driver.build();

    SegmentDirectory.createFromLocalFS(driver.getOutputDirectory(), ReadMode.mmap);
    recordReader.rewind();
    return recordReader;
  }

  /**
   * Build Avro file containing Text.
   *
   * @param schema Schema of data (one fixed and one variable column)
   * @param sortedExpected Sorted column are populated here
   * @param unsortedExpected unsorted column are populated here
   * @throws IOException
   */
  private void buildAvro(Schema schema, String[] sortedExpected, String[] unsortedExpected) throws IOException {
    org.apache.avro.Schema avroSchema = AvroUtils.getAvroSchemaFromPinotSchema(schema);

    try (DataFileWriter<GenericData.Record> recordWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {

      if (!new File(AVRO_DIR_NAME).mkdir()) {
        throw new RuntimeException("Unable to create test directory: " + AVRO_DIR_NAME);
      }

      for (int i = 0; i < NUM_ROWS; i++) {
        String str = RandomStringUtils.randomAlphanumeric(1 + _random.nextInt(MAX_STRING_LENGTH));
        sortedExpected[i] = str;
      }
      Arrays.sort(sortedExpected);

      recordWriter.create(avroSchema, new File(AVRO_DIR_NAME, AVRO_NAME));
      for (int i = 0; i < NUM_ROWS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);

        String str = RandomStringUtils.randomAlphanumeric(1 + _random.nextInt(MAX_STRING_LENGTH));
        unsortedExpected[i] = str;
        record.put(TEXT_COLUMN, str);
        record.put(TEXT_SORTED_COLUMN, sortedExpected[i]);
        recordWriter.append(record);
      }
    }
  }

  /**
   * Helper method that builds a segment from the given avro file.
   *
   * @param schema Schema of data
   * @return Pinot Segment
   */
  private IndexSegment buildSegmentFromAvro(Schema schema, String dirName, String avroName, String segmentName)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig();
    config.setInputFilePath(dirName + File.separator + avroName);
    config.setOutDir(dirName);
    config.setSegmentName(segmentName);
    config.setSchema(schema);
    List<String> columns = Arrays.asList(TEXT_COLUMN, TEXT_SORTED_COLUMN);
    config.setRawIndexCreationColumns(columns);
    config.setInvertedIndexCreationColumns(columns);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    return ImmutableSegmentLoader.load(new File(AVRO_DIR_NAME, SEGMENT_NAME), ReadMode.mmap);
  }
}
