package com.linkedin.pinot.minion.executor;

import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import com.linkedin.pinot.core.common.MinionConstants;
import com.linkedin.pinot.core.minion.RollupSegmentConverter;
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

    RollupSegmentConverter rollupSegmentConverter = new RollupSegmentConverter.Builder()
        .setMergeType(mergeType)
        .setInputIndexDirs(originalIndexDirs)
        .setWorkingDir(workingDir)
        .build();

    List<File> resultFiles = rollupSegmentConverter.convert();

    List<SegmentConversionResult> results = new ArrayList<>();
    for (File file: resultFiles) {
      results.add(new SegmentConversionResult.Builder().setFile(file).setSegmentName("merged").build());
    }

    return results;
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier() {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
        Collections.singletonMap(MinionConstants.ConvertToRawIndexTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            String.valueOf(System.currentTimeMillis())));
  }
}
