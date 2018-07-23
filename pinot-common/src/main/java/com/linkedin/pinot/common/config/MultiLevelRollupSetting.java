package com.linkedin.pinot.common.config;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class MultiLevelRollupSetting {

  @ConfigKey("timeInputFormat")
  public String _timeInputFormat;

  @ConfigKey("timeOutputFormat")
  public String _timeOutputFormat;

  @ConfigKey("timeOutputGranularity")
  public String _timeOutputGranularity;

  @ConfigKey("minNumSegments")
  public int _minNumSegments;

  @ConfigKey("minNumTotalDocs")
  public int _minNumTotalDocs;

  public String getTimeInputFormat() {
    return _timeInputFormat;
  }

  public void setTimeInputFormat(String timeInputFormat) {
    _timeInputFormat = timeInputFormat;
  }

  public String getTimeOutputFormat() {
    return _timeOutputFormat;
  }

  public void setTimeOutputFormat(String timeOutputFormat) {
    _timeOutputFormat = timeOutputFormat;
  }

  public String getTimeOutputGranularity() {
    return _timeOutputGranularity;
  }

  public void setTimeOutputGranularity(String timeOutputGranularity) {
    _timeOutputGranularity = timeOutputGranularity;
  }

  public int getMinNumSegments() {
    return _minNumSegments;
  }

  public void setMinNumSegments(int minNumSegments) {
    _minNumSegments = minNumSegments;
  }

  public int getMinNumTotalDocs() {
    return _minNumTotalDocs;
  }

  public void setMinNumTotalDocs(int minNumTotalDocs) {
    _minNumTotalDocs = minNumTotalDocs;
  }
}