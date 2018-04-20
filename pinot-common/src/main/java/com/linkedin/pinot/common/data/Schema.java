/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.data;

import com.google.common.base.Preconditions;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.utils.EqualityUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>Schema</code> class is defined for each table to describe the details of the table's fields (columns).
 * <p>Four field types are supported: DIMENSION, METRIC, TIME, DATE_TIME.
 * ({@link com.linkedin.pinot.common.data.DimensionFieldSpec}, {@link com.linkedin.pinot.common.data.MetricFieldSpec},
 * {@link com.linkedin.pinot.common.data.TimeFieldSpec}, {@link com.linkedin.pinot.common.data.DateTimeFieldSpec})
 * <p>For each field, a {@link com.linkedin.pinot.common.data.FieldSpec} is defined to provide the details of the field.
 * <p>There could be multiple DIMENSION or METRIC or DATE_TIME fields, but at most 1 TIME field.
 * <p>In pinot, we store data using 5 <code>DataType</code>s: INT, LONG, FLOAT, DOUBLE, STRING. All other
 * <code>DataType</code>s will be converted to one of them.
 */
@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public final class Schema {
  private static final Logger LOGGER = LoggerFactory.getLogger(Schema.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String _schemaName;
  private final List<DimensionFieldSpec> _dimensionFieldSpecs = new ArrayList<>();
  private final List<MetricFieldSpec> _metricFieldSpecs = new ArrayList<>();
  private TimeFieldSpec _timeFieldSpec;
  private final List<DateTimeFieldSpec> _dateTimeFieldSpecs = new ArrayList<>();

  // Json ignored fields
  private transient final Map<String, FieldSpec> _fieldSpecMap = new HashMap<>();
  private transient final List<String> _dimensionNames = new ArrayList<>();
  private transient final List<String> _metricNames = new ArrayList<>();
  private transient final List<String> _dateTimeNames = new ArrayList<>();

  @Nonnull
  public static Schema fromFile(@Nonnull File schemaFile) throws IOException {
    return MAPPER.readValue(schemaFile, Schema.class);
  }

  @Nonnull
  public static Schema fromString(@Nonnull String schemaString) throws IOException {
    return MAPPER.readValue(schemaString, Schema.class);
  }

  @Nonnull
  public static Schema fromInputSteam(@Nonnull InputStream schemaInputStream) throws IOException {
    return MAPPER.readValue(schemaInputStream, Schema.class);
  }

  /**
   * NOTE: schema name could be null in tests
   */
  public String getSchemaName() {
    return _schemaName;
  }

  public void setSchemaName(@Nonnull String schemaName) {
    _schemaName = schemaName;
  }

  @Nonnull
  public List<DimensionFieldSpec> getDimensionFieldSpecs() {
    return _dimensionFieldSpecs;
  }

  /**
   * Required by JSON deserializer. DO NOT USE. DO NOT REMOVE.
   * Adding @Deprecated to prevent usage
   * @param dimensionFieldSpecs
   */
  @Deprecated
  public void setDimensionFieldSpecs(@Nonnull List<DimensionFieldSpec> dimensionFieldSpecs) {
    Preconditions.checkState(_dimensionFieldSpecs.isEmpty());

    for (DimensionFieldSpec dimensionFieldSpec : dimensionFieldSpecs) {
      addField(dimensionFieldSpec);
    }
  }

  @Nonnull
  public List<MetricFieldSpec> getMetricFieldSpecs() {
    return _metricFieldSpecs;
  }

  /**
   * Required by JSON deserializer. DO NOT USE. DO NOT REMOVE.
   * Adding @Deprecated to prevent usage
   * @param metricFieldSpecs
   */
  @Deprecated
  public void setMetricFieldSpecs(@Nonnull List<MetricFieldSpec> metricFieldSpecs) {
    Preconditions.checkState(_metricFieldSpecs.isEmpty());

    for (MetricFieldSpec metricFieldSpec : metricFieldSpecs) {
      addField(metricFieldSpec);
    }
  }

  @Nonnull
  public List<DateTimeFieldSpec> getDateTimeFieldSpecs() {
    return _dateTimeFieldSpecs;
  }

  /**
   * Required by JSON deserializer. DO NOT USE. DO NOT REMOVE.
   * Adding @Deprecated to prevent usage
   * @param dateTimeFieldSpecs
   */
  @Deprecated
  public void setDateTimeFieldSpecs(@Nonnull List<DateTimeFieldSpec> dateTimeFieldSpecs) {
    // if we encounter timeFieldSpec, we are setting it as dateTimeFieldSpec. As a result, the condition _dateTimeFieldSpec.isEmpty() will not be met.
    // TODO: Remove the second part of the check, once we remove all traces of timeFieldSpec
    Preconditions.checkState(_dateTimeFieldSpecs.isEmpty() || (_dateTimeFieldSpecs.size() == 1 && _timeFieldSpec != null
        && _dateTimeFieldSpecs.get(0).getName().equals(_timeFieldSpec.getName())));

    for (DateTimeFieldSpec dateTimeFieldSpec : dateTimeFieldSpecs) {
      addField(dateTimeFieldSpec);
    }
  }

  /**
   * TimeFieldSpec has been deprecated.
   * Required by JSON deserializer, for backward compatibility. DO NOT USE. DO NOT REMOVE.
   * Adding @Deprecated to prevent usage
   * @param timeFieldSpec
   */
  @Deprecated
  public void setTimeFieldSpec(TimeFieldSpec timeFieldSpec) {
    if (timeFieldSpec != null) {
      addField(timeFieldSpec);
    }
  }

  public void addField(@Nonnull FieldSpec fieldSpec) {
    Preconditions.checkNotNull(fieldSpec);
    String columnName = fieldSpec.getName();
    Preconditions.checkNotNull(columnName);
    Preconditions.checkState(!_fieldSpecMap.containsKey(columnName),
        "Field spec already exists for column: " + columnName);

    FieldType fieldType = fieldSpec.getFieldType();
    switch (fieldType) {
      case DIMENSION:
        _dimensionNames.add(columnName);
        _dimensionFieldSpecs.add((DimensionFieldSpec) fieldSpec);
        break;
      case METRIC:
        _metricNames.add(columnName);
        _metricFieldSpecs.add((MetricFieldSpec) fieldSpec);
        break;
      case TIME: // TIME has been deprecated, this case is required for backward compatibility with older schemas
        Preconditions.checkState(_timeFieldSpec == null, "Already defined the time column: " + _timeFieldSpec);
        _timeFieldSpec = (TimeFieldSpec) fieldSpec;
        DateTimeFieldSpec dateTimeFieldSpec = getDateTimeFieldSpecFromTimeFieldSpec(_timeFieldSpec);
        fieldSpec = dateTimeFieldSpec;
      case DATE_TIME:
        _dateTimeNames.add(columnName);
        _dateTimeFieldSpecs.add((DateTimeFieldSpec) fieldSpec);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
    }

    _fieldSpecMap.put(columnName, fieldSpec);
  }

  @JsonIgnore
  private DateTimeFieldSpec getDateTimeFieldSpecFromTimeFieldSpec(TimeFieldSpec timeFieldSpec) {
    String name = timeFieldSpec.getName();
    TimeGranularitySpec outgoingTimeSpec = timeFieldSpec.getOutgoingGranularitySpec();
    DataType dataType = outgoingTimeSpec.getDataType();
    int columnSize = outgoingTimeSpec.getTimeUnitSize();
    TimeUnit columnUnit = outgoingTimeSpec.getTimeType();
    String[] timeFormatTokens = outgoingTimeSpec.getTimeFormat().split(":");
    String columnTimeFormat = timeFormatTokens[0];
    DateTimeFormatSpec dateTimeFormatSpec;
    if (timeFormatTokens.length > 1) {
      String sdfPattern = timeFormatTokens[1];
      dateTimeFormatSpec = new DateTimeFormatSpec(columnSize, String.valueOf(columnUnit), columnTimeFormat, sdfPattern);
    } else {
      dateTimeFormatSpec = new DateTimeFormatSpec(columnSize, String.valueOf(columnUnit), columnTimeFormat);
    }
    DateTimeGranularitySpec dateTimeGranularitySpec = new DateTimeGranularitySpec(columnSize, columnUnit);
    DateTimeFieldSpec dateTimeFieldSpec =
        new DateTimeFieldSpec(name, dataType, dateTimeFormatSpec, dateTimeGranularitySpec);
    return dateTimeFieldSpec;
  }

  public boolean removeField(String columnName) {
    FieldSpec existingFieldSpec = _fieldSpecMap.remove(columnName);
    if (existingFieldSpec != null) {
      FieldType fieldType = existingFieldSpec.getFieldType();
      switch (fieldType) {
        case DIMENSION:
          int index = _dimensionNames.indexOf(columnName);
          _dimensionNames.remove(index);
          _dimensionFieldSpecs.remove(index);
          break;
        case METRIC:
          index = _metricNames.indexOf(columnName);
          _metricNames.remove(index);
          _metricFieldSpecs.remove(index);
          break;
        case TIME:
          _timeFieldSpec = null;
          break;
        case DATE_TIME:
          index = _dateTimeNames.indexOf(columnName);
          _dateTimeNames.remove(index);
          _dateTimeFieldSpecs.remove(index);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
      }
      return true;
    } else {
      return false;
    }
  }

  public boolean hasColumn(@Nonnull String columnName) {
    return _fieldSpecMap.containsKey(columnName);
  }

  @JsonIgnore
  @Nonnull
  public Map<String, FieldSpec> getFieldSpecMap() {
    return _fieldSpecMap;
  }

  @JsonIgnore
  @Nonnull
  public Set<String> getColumnNames() {
    return _fieldSpecMap.keySet();
  }

  @JsonIgnore
  @Nonnull
  public Collection<FieldSpec> getAllFieldSpecs() {
    return _fieldSpecMap.values();
  }

  public int size() {
    return _fieldSpecMap.size();
  }

  @JsonIgnore
  public FieldSpec getFieldSpecFor(@Nonnull String columnName) {
    return _fieldSpecMap.get(columnName);
  }

  @JsonIgnore
  public MetricFieldSpec getMetricSpec(@Nonnull String metricName) {
    FieldSpec fieldSpec = _fieldSpecMap.get(metricName);
    if (fieldSpec != null && fieldSpec.getFieldType() == FieldType.METRIC) {
      return (MetricFieldSpec) fieldSpec;
    }
    return null;
  }

  @JsonIgnore
  public DimensionFieldSpec getDimensionSpec(@Nonnull String dimensionName) {
    FieldSpec fieldSpec = _fieldSpecMap.get(dimensionName);
    if (fieldSpec != null && fieldSpec.getFieldType() == FieldType.DIMENSION) {
      return (DimensionFieldSpec) fieldSpec;
    }
    return null;
  }

  @JsonIgnore
  public DateTimeFieldSpec getDateTimeSpec(@Nonnull String dateTimeName) {
    FieldSpec fieldSpec = _fieldSpecMap.get(dateTimeName);
    if (fieldSpec != null && fieldSpec.getFieldType() == FieldType.DATE_TIME) {
      return (DateTimeFieldSpec) fieldSpec;
    }
    return null;
  }

  @JsonIgnore
  @Nonnull
  public List<String> getDimensionNames() {
    return _dimensionNames;
  }

  @JsonIgnore
  @Nonnull
  public List<String> getMetricNames() {
    return _metricNames;
  }

  @JsonIgnore
  @Nonnull
  public List<String> getDateTimeNames() {
    return _dateTimeNames;
  }

  @JsonIgnore
  @Nonnull
  public String getJSONSchema() {
    JsonObject jsonSchema = new JsonObject();
    jsonSchema.addProperty("schemaName", _schemaName);
    if (!_dimensionFieldSpecs.isEmpty()) {
      JsonArray jsonArray = new JsonArray();
      for (DimensionFieldSpec dimensionFieldSpec : _dimensionFieldSpecs) {
        jsonArray.add(dimensionFieldSpec.toJsonObject());
      }
      jsonSchema.add("dimensionFieldSpecs", jsonArray);
    }
    if (!_metricFieldSpecs.isEmpty()) {
      JsonArray jsonArray = new JsonArray();
      for (MetricFieldSpec metricFieldSpec : _metricFieldSpecs) {
        jsonArray.add(metricFieldSpec.toJsonObject());
      }
      jsonSchema.add("metricFieldSpecs", jsonArray);
    }
    if (!_dateTimeFieldSpecs.isEmpty()) {
      JsonArray jsonArray = new JsonArray();
      for (DateTimeFieldSpec dateTimeFieldSpec : _dateTimeFieldSpecs) {
        jsonArray.add(dateTimeFieldSpec.toJsonObject());
      }
      jsonSchema.add("dateTimeFieldSpecs", jsonArray);
    }
    return new GsonBuilder().setPrettyPrinting().create().toJson(jsonSchema);
  }

  /**
   * Validates a pinot schema.
   * <p>The following validations are performed:
   * <ul>
   *   <li>For dimension, time, date time fields, support {@link DataType}: INT, LONG, FLOAT, DOUBLE, STRING</li>
   *   <li>For non-derived metric fields, support {@link DataType}: INT, LONG, FLOAT, DOUBLE</li>
   * </ul>
   *
   * @param ctxLogger Logger used to log the message (if null, the current class logger is used)
   * @return Whether schema is valid
   */
  public boolean validate(Logger ctxLogger) {
    if (ctxLogger == null) {
      ctxLogger = LOGGER;
    }

    // Log ALL the schema errors that may be present.
    for (FieldSpec fieldSpec : _fieldSpecMap.values()) {
      FieldType fieldType = fieldSpec.getFieldType();
      DataType dataType = fieldSpec.getDataType();
      String fieldName = fieldSpec.getName();
      switch (fieldType) {
        case DIMENSION:
        case DATE_TIME:
          switch (dataType) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
              break;
            default:
              ctxLogger.info("Unsupported data type: {} in DIMENSION/DATETIME field: {}", dataType, fieldName);
              return false;
          }
          break;
        case METRIC:
          switch (dataType) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
              break;
            case STRING:
              MetricFieldSpec metricFieldSpec = (MetricFieldSpec) fieldSpec;
              if (!metricFieldSpec.isDerivedMetric()) {
                ctxLogger.info("Unsupported data type: STRING in non-derived METRIC field: {}", fieldName);
                return false;
              }
              break;
            default:
              ctxLogger.info("Unsupported data type: {} in METRIC field: {}", dataType, fieldName);
              return false;
          }
          break;
        default:
          ctxLogger.info("Unsupported field type: {} for field: {}", dataType, fieldName);
          return false;
      }
    }

    return true;
  }

  public static class SchemaBuilder {
    private Schema _schema;

    public SchemaBuilder() {
      _schema = new Schema();
    }

    public SchemaBuilder setSchemaName(@Nonnull String schemaName) {
      _schema.setSchemaName(schemaName);
      return this;
    }

    public SchemaBuilder addSingleValueDimension(@Nonnull String dimensionName, @Nonnull DataType dataType) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, true));
      return this;
    }

    public SchemaBuilder addSingleValueDimension(@Nonnull String dimensionName, @Nonnull DataType dataType,
        @Nonnull Object defaultNullValue) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, true, defaultNullValue));
      return this;
    }

    public SchemaBuilder addMultiValueDimension(@Nonnull String dimensionName, @Nonnull DataType dataType) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, false));
      return this;
    }

    public SchemaBuilder addMultiValueDimension(@Nonnull String dimensionName, @Nonnull DataType dataType,
        @Nonnull Object defaultNullValue) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, false, defaultNullValue));
      return this;
    }

    public SchemaBuilder addMetric(@Nonnull String metricName, @Nonnull DataType dataType) {
      _schema.addField(new MetricFieldSpec(metricName, dataType));
      return this;
    }

    public SchemaBuilder addMetric(@Nonnull String metricName, @Nonnull DataType dataType,
        @Nonnull Object defaultNullValue) {
      _schema.addField(new MetricFieldSpec(metricName, dataType, defaultNullValue));
      return this;
    }

    public SchemaBuilder addMetric(@Nonnull String name, @Nonnull DataType dataType, int fieldSize,
        @Nonnull MetricFieldSpec.DerivedMetricType derivedMetricType) {
      _schema.addField(new MetricFieldSpec(name, dataType, fieldSize, derivedMetricType));
      return this;
    }

    public SchemaBuilder addMetric(@Nonnull String name, @Nonnull DataType dataType, int fieldSize,
        @Nonnull MetricFieldSpec.DerivedMetricType derivedMetricType, @Nonnull Object defaultNullValue) {
      _schema.addField(new MetricFieldSpec(name, dataType, fieldSize, derivedMetricType, defaultNullValue));
      return this;
    }

    public SchemaBuilder addDateTime(@Nonnull String name, @Nonnull DataType dataType, @Nonnull String format,
        @Nonnull String granularity) {
      _schema.addField(new DateTimeFieldSpec(name, dataType, format, granularity));
      return this;
    }

    public SchemaBuilder addDateTime(@Nonnull String name, @Nonnull DataType dataType,
        @Nonnull DateTimeFormatSpec dateTimeFormatSpec, @Nonnull DateTimeGranularitySpec dateTimeGranularitySpec) {
      _schema.addField(new DateTimeFieldSpec(name, dataType, dateTimeFormatSpec, dateTimeGranularitySpec));
      return this;
    }

    public Schema build() {
      if (!_schema.validate(LOGGER)) {
        throw new RuntimeException("Invalid schema");
      }
      return _schema;
    }
  }

  @Override
  public String toString() {
    return getJSONSchema();
  }

  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    Schema that = (Schema) o;
    return EqualityUtils.isEqual(_schemaName, that._schemaName) && EqualityUtils.isEqual(_dimensionFieldSpecs,
        that._dimensionFieldSpecs) && EqualityUtils.isEqual(_metricFieldSpecs, that._metricFieldSpecs)
        && EqualityUtils.isEqual(_timeFieldSpec, that._timeFieldSpec) && EqualityUtils.isEqual(_dateTimeFieldSpecs,
        that._dateTimeFieldSpecs);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_schemaName);
    result = EqualityUtils.hashCodeOf(result, _dimensionFieldSpecs);
    result = EqualityUtils.hashCodeOf(result, _metricFieldSpecs);
    result = EqualityUtils.hashCodeOf(result, _timeFieldSpec);
    result = EqualityUtils.hashCodeOf(result, _dateTimeFieldSpecs);
    return result;
  }


    public static void main(String[] args) throws IOException {
//       Schema schema = Schema.fromFile(new File("/Users/npawar/pinotOnAzureProject/schemaDateTime.json"));
//       Schema schema = Schema.fromFile(new File("/Users/npawar/pinotOnAzureProject/schemaOnlyDateTime.json"));
//       Schema schema = Schema.fromFile(new File("/Users/npawar/pinotOnAzureProject/schema.json"));
       Schema schema = Schema.fromFile(new File("/Users/npawar/pinotOnAzureProject/thirdeyeKbmiSchema.json"));
        System.out.println(schema.getJSONSchema());
        System.out.println(schema.getFieldSpecFor("Date"));
        System.out.println(schema.getFieldSpecFor("hoursSinceEpoch"));
        System.out.println(schema.getFieldSpecFor("timestampInEpoch"));
        System.out.println(schema.getDateTimeNames());
        System.out.println(schema.getDateTimeFieldSpecs());
        System.out.println(schema.getDateTimeSpec("Date"));
        System.out.println(schema.getDateTimeSpec("hoursSinceEpoch"));
      }

}
