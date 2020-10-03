package org.apache.pinot.core.upsert;

import com.google.common.base.Objects;


/**
 * Indicate a record's location on the local host.
 */
public class RecordLocation {
  private String _segmentName;
  private int _docId;
  private long _timestamp;

  public RecordLocation(String segmentName, int docId, long timestamp) {
    _segmentName = segmentName;
    _docId = docId;
    _timestamp = timestamp;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public int getDocId() {
    return _docId;
  }

  public long getTimestamp() {
    return _timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RecordLocation that = (RecordLocation) o;
    return Objects.equal(_segmentName, that._segmentName) && Objects.equal(_docId, that._docId) && Objects
        .equal(_timestamp, that._timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(_segmentName, _docId, _timestamp);
  }
}
