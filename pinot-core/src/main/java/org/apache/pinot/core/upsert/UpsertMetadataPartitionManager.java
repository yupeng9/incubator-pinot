package org.apache.pinot.core.upsert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.core.realtime.impl.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.data.readers.PrimaryKey;


/**
 * Manages the upsert metadata per partition. This shall be accessed from UpsertMetadataTableManager.
 */
@ThreadSafe
class UpsertMetadataPartitionManager {

  private final int _partitionId;

  private final Map<PrimaryKey, RecordLocation> _primaryKeyIndex = new ConcurrentHashMap();
  // the mapping between the (sealed) segment and its validDocuments
  // TODO(upsert) concurrency protection
  private final Map<String, ThreadSafeMutableRoaringBitmap> _segmentToValidDocIndexMap = new ConcurrentHashMap();

  UpsertMetadataPartitionManager(int partitionId) {
    _partitionId = partitionId;
  }

  synchronized void removeRecordLocation(PrimaryKey primaryKey) {
    _primaryKeyIndex.remove(primaryKey);
  }

  synchronized boolean containsKey(PrimaryKey primaryKey) {
    return _primaryKeyIndex.containsKey(primaryKey);
  }

  RecordLocation getRecordLocation(PrimaryKey primaryKey) {
    return _primaryKeyIndex.get(primaryKey);
  }

  synchronized void updateRecordLocation(PrimaryKey primaryKey, RecordLocation recordLocation) {
    _primaryKeyIndex.put(primaryKey, recordLocation);
  }

  ThreadSafeMutableRoaringBitmap getValidDocIndex(String segmentName) {
    return _segmentToValidDocIndexMap.get(segmentName);
  }

  synchronized void putUpsertMetadata(String segmentName, Map<PrimaryKey, RecordLocation> primaryKeyIndex,
      ThreadSafeMutableRoaringBitmap validDocIndex) {
    _primaryKeyIndex.putAll(primaryKeyIndex);
    _segmentToValidDocIndexMap.put(segmentName, validDocIndex);
  }

  int getPartitionId() {
    return _partitionId;
  }
}
