package com.linkedin.pinot.minion.executor;

import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import com.linkedin.pinot.core.common.MinionConstants;
import com.linkedin.pinot.core.minion.rollup.MergeRollupSegmentConverter;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


public class SegmentMergeRollupTaskExecutor extends BaseMultipleSegmentsConversionExecutor {
  @Override
  protected List<SegmentConversionResult> convert(@Nonnull PinotTaskConfig pinotTaskConfig,
      @Nonnull List<File> originalIndexDirs, @Nonnull File workingDir) throws Exception {
    Map<String, String> configs = pinotTaskConfig.getConfigs();

    String mergeType = configs.get(MinionConstants.SegmentMergeRollupTask.MERGE_TYPE);
    String mergedSegmentName = configs.get(MinionConstants.SegmentMergeRollupTask.MERGED_SEGEMNT_NAME_KEY);
    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);

    MergeRollupSegmentConverter rollupSegmentConverter = new MergeRollupSegmentConverter.Builder()
        .setMergeType(mergeType)
        .setTableName(tableNameWithType)
        .setSegmentName(mergedSegmentName)
        .setInputIndexDirs(originalIndexDirs)
        .setWorkingDir(workingDir)
        .build();

    List<File> resultFiles = rollupSegmentConverter.convert();

    List<SegmentConversionResult> results = new ArrayList<>();

    for (File file: resultFiles) {
      String outputSegmentName = file.getName();
      results.add(new SegmentConversionResult.Builder()
          .setFile(file)
          .setSegmentName(outputSegmentName)
          .setTableNameWithType(tableNameWithType)
          .build());
    }
    return results;
  }
}
